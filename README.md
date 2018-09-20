Convert SAS files to bucketed columnized files
==============================================

Suppose we are given a collection of large SAS files having a common
set of variables.  Our goal here is to produce a directory layout
containing all the data obtained from the SAS files, organized like
this:

```
Project
|------Buckets/
|      |-------0001/
|      |       |-------Var1.bin.sz
|      |       |-------Var2.bin.sz
|      |       |-------Var3.bin.sz
|      |       |-------dtypes.json
|      |-------0002/
|      |       |-------Var1.bin.sz
|      |       |-------Var2.bin.sz
|      |       |-------Var3.bin.sz
|      |       |-------dtypes.json
```

Each data file (e.g. Var1.bin.sz ) ontains a raw stream of binary data
values, with no header or metadata (some metadata is placed in the
dtypes.json file).  The exception to this is that string values are
delimited by newlines.

The filename prefixes Var1, Var2, etc. are the variable names from the
SAS files.  The number of "buckets" (e.g. two in the example above) is
configurable by the user.

Rows from the SAS files are mapped into the buckets using an id
variable.  The id variable is hashed to determine the bucket for a
given row of data.  All rows with the same id are sent to the same
bucket.  The data within each bucket are sorted by id, and within
levels of the id variable, the data are sorted by a sequence variable
(e.g. time).

The variables can be converted from their SAS type to any Go type when
forming the buckets.  Go types are native integer, floating point, and
string values, so it is easy to process these data using most
programming languages (numeric types are always written in little
endian form).

String variables whose values are "factors" can be converted to Go
[uvarint](https://golang.org/pkg/encoding/binary/#Uvarint) values for
better compression.

The `dtypes.json` file contains a json-formatted map from string
variable names to string data types (using Go type names), for
example:

```
{"Var1": "uint32", "Var2": "float64", "Var3": "string", "Var4": "uvarint"}
```

The data construction pipeline involves three steps, controlled by a
configuration file described below.

Design goals, use-cases and related work
----------------------------------------

The basic idea implemented here is inspired by the
[bcolz](https://github.com/Blosc/bcolz) Python project and the
[feather](https://blog.rstudio.org/2016/03/29/feather) columnar data
container, but there are a few key differences.  A major goal here is
to support "long format" data such as administrative records in which
each subject has multiple data records.  It is strongly desirable that
those records be stored adjacently on disk (the records for one
subject are usually not adjacent in the original SAS files).  We
accomplish this by defining the buckets based on a hash function
applied to the id variable.  The use of a sequence variable further
facilitates longitudinal analyses of these datasets.

The main use-case for this data layout is to support analyses that
require one or more "full table scans", where the processing of
different subjects can be done concurrently.  It is easy to write code
that "walks through" each bucket, processing the (adjacent) records
for each subject in sequence.  These "walking" processes can be run
concurrently over the buckets.

The construction of the data described here is intended to be
efficient for large SAS files, making extensive use of concurrent
processing.  For example, around 3TB of SAS files can be reduced to
around 200GB of data in around 15 hours on a single workstation.

The SAS files are read using a [native Go SAS
reader](https://github.com/kshedden/datareader), therefore SAS
software is not required to run this pipeline.  The native Go SAS
reader has been tested on many examples and found to give accurate
results.  However the SAS file specification is not public so it is
not possible to know that the results will be accurate in every case.

Configuration
-------------

The pipeline is configured using a toml-formatted configuration file.
The configuration file contains the following parameters:

* __Sourcedir__: The directory prefix for all SAS files to be
  processed

* __SASFiles__: The base names of all SAS files to be processed

* __TargetDir__: The directory prefix where the files being
  constructed are placed

* __CodesDir__: The directory where factor code information is stored

* __SASChunkSize__: The number of SAS rows that are read in each
  iteration

* __NumBuckets__: The number of buckets to use

* __BufMaxRecs__: The number of records held in memory by sastocols,
per bucket, before flushing the bucket to disk

* __MaxChunk__: Stop processing each SAS file after this number of
chunks are read (used for debugging and testing).  If zero, all chunks
are read.

sastocols
---------

sastocols copies the data from the SAS files into the bucket layout
described above.  It is the first step in a three-step pipeline.  It
does not do any sorting or varint construction, its only job is to
copy the data from the SAS files into the appropriate buckets,
converting types as needed.  For performance reasons, a Go script is
generated with static type information about the source and target
files.  This script is generated based on a toml-format variable
definition file, containing one line per variable.  The format of
these lines is:

```
[[Variable]]
  Name = "Income"
  GoType = "uint64"
  SASType = "float64"
  Must = true
  KeyVar = false
```

The elements of a variable description are as follows:

* __Name__: the name of the variable in the SAS file.  SAS is not case
   sensitive for variable names, so the case does not need to match
   the case used in the SAS file

* __GoType__: The type of the data as stored on disk in the buckets,
  using Go type names

* __SASType__: The type of the data in the SAS file, using SAS type
  names (float64 or string)

* __Must__: A boolean defining variables that do not need to be
  present in each file.  If true, the conversion will stop if the
  variable is missing in any of the SAS files

To build a go program to perform the conversions, run the `gen.go`
script in the `sastocols` directory, passing in a variable definition
file (e.g. `defs.toml` below) formatted as described above:

```
go run gen.go defs.toml > sastocols.go
```

Now you will have a go script (called here `sastocols.go`) that you
can run as follows:

```
go run sastocols.go config.toml
```

factorize
---------

The factorize command is used to convert strings to integer factor
codes (represented on disk as uvarint values).  Doing this is mainly
useful for a variable that has a small to moderate number of distinct
values, or when there is a large number of distinct values but a small
subset of these values are much more common than the others.

The factor codes are represented on-disk as `uvarint` values that can
be easily converted to standard fixed-width integers when reading into
memory.  A `map[string]int` mapping the string values to their integer
codes is written (in json-format) to the `CodesDir` directory
specified in the configuration file.

A group of variables can be factorized together, meaning that they
will share the same code dictionary.

To perform the factorization, use the following shell command

```
factorize run prefix config1.toml config2.toml...
```

Here, `prefix` defines which variables will be factorized as a group
(i.e. using the same set of integer codes).  For example, if prefix is
"ABX", then all variables beginning with "ABX" (e.g. ABX1, ABXZ, etc.)
will be jointly factorized.  The `config` files are `gosascols`
configuration file as described above, describing the databases that
are to be jointly factorized.

Since factorize modifies the `dtypes.json` file, do not run multiple
factorize scripts on a database simultaneously.

The `factorize` command supports a limited "undo" operation.  The
factorization can be reverted (i.e. the uvarint values are converted
back to their string values) using the command

```
factorize revert prefix config1.toml config2.toml...
```

Note that reversion is not possible after `sortbuckets` (below) has
been run, or if `cleanbuckets` has been run.

sortbuckets
-----------

`sortbuckets` is the final step of the pipeline.  It sorts the data
within each bucket first by the values of the id variable, and
optionally within id levels by a sequence variable (e.g. time).

To perform the sorting, use the following shell command:

```
sortbuckets run idvar timevar config.toml
```

Here, `idvar` and `timevar` are the names of the id variable and
sequence variable, respectively.

If `cleanbuckets` has not been run, the sorting can be reverted as
follows:

```
sortbuckets revert idvar timevar config.toml
```

Other tools
-----------

__cleanbuckets__: After the pipeline is complete, this script can be
run to remove any temporary files from the bucket storage area.  After
running `cleanbuckets` no reversion of the factorize or sorting steps
is possible.

__qperson__: Query function, returns all data for a given value of the
bucketing id variable.

TODO
----

* We have done a fair amount of incidental testing, but we do not have
  a robust set of unit tests.

* The id variable must have SAS type float and Go type uint64.  It may
  or may not be desirable to allow this to be more configurable.

* The sequence variable is currently mandatory and must have type
  `uint16`, it could be made optional or allowed to have other types.

* We only support unsigned Go integer types, it would be easy to add
  support for signed integer types.

* The files are currently [snappy](https://google.github.io/snappy)
  compressed, but optional gzip compression would be easy to add.
