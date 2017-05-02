Convert SAS files to bucketed columnized files
==============================================

Suppose we are given a collection of large SAS files having a common
set of variables (optionally, some variables may be absent from some
of the files).  The goal is to produce a directory layout containing
all of the SAS file data like this:

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

The files name prefixes Var1, Var2, etc. are the variable names from
the SAS files.  The number of "buckets" (e.g. two in the example
above) is configurable by the user.

Rows from the SAS files are mapped into the buckets using an id
variable.  The id variable is hashed to determine the bucket for a
given row of data.  All rows with the same id are sent to the same
bucket.  The data within each bucket are sorted by id, and within id
levels the data can optionally be sorted by a specified time or index
variable.

The variables can be converted from their SAS type to any Go type when
forming the buckets.  Go types are native integer, floating point, and
string values, so it is relatively easy to process these data from any
programming language.

String variables whose values are "factors" can be converted to Go
[uvarint](https://golang.org/pkg/encoding/binary/#Uvarint) values for
better compression.

The `dtypes.json` file contains a json-formatted map from string
variable names to string data types (using Go type names), for
example:

```
{"Var1": "uint32", "Var2": "float64", "Var3": "string"}
```

The data construction pipeline involves three steps, controlled by a
configuration file described in the next section.

Configuration
-------------

The pipeline is configured using a json formatted configuration file.
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
chunks are read (used for debugging and testing)

sastocols
---------

sastocols copies the data from the SAS files into the bucket layout
described above.  It is the first step in a three-step pipeline.  It
does not do any sorting or varint construction, its only job is to
copy the data from the SAS files into the appropriate buckets,
converting types as needed.  For performance reasons, a Go script is
generated with static type information about the source and target
files.  This script is generated based on a json-format variable
definition file, containing one line per variable.  The format of
these lines is:

```
{"Name": "Admtyp", "GoType": "uint8", "SASType": "string", "Must": true}
{"Name": "Admdate", "GoType": "uint16", "SASType": "float64", "Must": true}
{"Name": "Dx1", "GoType": "string", "SASType": "string", "Must": true}
```

The fields of each row of the variable description file are as follows:

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
file (e.g. `defs.json` below) formatted as described above:

```
go run gen.go defs.json > sastocols.go
```

Now you will have a go script (called here `sastocols.go`) that you
can run as follows:

```
go run sastocols.go config.json
```

factorize
---------

`factorize` is used to convert strings to integer factor codes.  It is
mainly useful when the variable has a small to moderate number of
distinct values, or when there is a large number of distinct values
but a small subset of these values are much more common than the
others.  The factor codes are represented as `uvarint` values that can
be easily converted to standard fixed-width integers.  A
`map[string]int` mapping the string values to their integer values is
written (in json-format) to the `CodesDir` directory specified in the
configuration file.

A group of variables can be factorized together, meaning that they
will share the same code dictionary.

To perform the factorization, use the following shell command

```
factorize run prefix config1.json config2.json...
```

Here, `prefix` defines which variables will be factorized as a group
(i.e. using the same set of integer codes).  For example, if prefix is
"ABX", then all variables beginning with "ABX" (e.g. ABX1, ABXZ, etc.)
will be jointly factorized.  The `config` files are `gosascols`
configuration file as described above, describing the databases that
are to be jointly factorized.

Since factorize modifies the `dtypes.json` file, do not run multiple
factorize scripts on a database simultaneously.

sortbuckets
-----------

`sortbuckets` is the final step of the pipeline.  It sorts the data
within each bucket first by the values of the id variable, and
optionally within id levels by a sequence variable (e.g. time).

To perform the sorting, use the following shell command:

```
sortbuckets config.json idvar timevar run
```

Here, `idvar` and `timevar` are the names of the id variable and
sequence variable, respectively.