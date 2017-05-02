Convert SAS files to bucketed columnized files
----------------------------------------------

Suppose we are given a collection of large SAS files with a common set
of variables (optionally, some variables may be missing in some of the
files).  The goal is to produce a directory layout like this
containing all of the SAS file data:

```
Project
|------Buckets/
|      |-------0001/
|      |       |--------Var1.bin.sz
|      |       |--------Var2.bin.sz
|      |       |--------Var3.bin.sz
|      |       |--------dtypes.json
|      |-------0002/
|      |       |--------Var1.bin.sz
|      |       |--------Var2.bin.sz
|      |       |--------Var3.bin.sz
|      |       |--------dtypes.json
```

The files name prefixes Var1, Var2, etc. are the variable names from
the SAS files (SAS variable names are not case-sensitive, we use title
case).  The number of "buckets" (two in the example above) is
configurable by the user.

Rows from the SAS files are mapped into the buckets using an id
variable.  The id variable is hashed to determine the bucket for a
given row of data.  All rows with the same id are sent to the same
bucket.  The data within each bucket are sorted by id, and optionally
by a specified "time" variable.

SAS variables must have type SAS type float or string.  The variables
can be converted to any Go type when forming the buckets.  String
variables whose values are "factors" (i.e. are drawn from a
dictionary) can be converted to Go uvarint values for better
compression.

Configuration
=============

The pipeline is configured using a json formatted configuration file.
The configuration files contains the following parameters:

* __Sourcedir__: The directory prefix for all SAS files to be
  processed.

* __SASFiles__: The base names of all SAS files to be processed.

* __TargetDir__: The directory prefix where the files being
  constructed are placed.

* __CodesDir__: The directory where factor code information is stored.

* __SASChunkSize__: The number of SAS rows that are read in each
  iteration.

* __NumBuckets__: The number of buckets to use.

* __BufMaxRecs__: The number of records held in memory by sastocols,
per bucket, before flushing the bucket to disk.

* __MaxChunk__: Stop processing each SAS file after this number of
chunks are read (used for debugging and testing).

sastocols
=========

sastocols copies the data from the SAS files into the bucket layout
described above.  It is the first step in a threee-step pipeline.  It
does not do any sorting or varint construction, its only job is to
construct the raw buckets.  For performance, a Go script is generated
with static type information about the source and target files.  The
Go script is generated based on a json-format variable definition
file, containing one line per variable.  The format of these lines is:

```
{"Name": "Admtyp", "GoType": "uint8", "SASType": "string", "Must": true}
{"Name": "Admdate", "GoType": "uint16", "SASType": "float64", "Must": true}
{"Name": "Dx1", "GoType": "string", "SASType": "string", "Must": true}
```

The fields of each row of the variable description file are as follows:

* __Name__: the name of the variable in the SAS file.  SAS is not case
   sensitive for variable names, so the case does not need to match
   the case used in the SAS file.

* __GoType__: The type of the data as stored on disk in the buckets,
  using Go type names.

* __SASType__: The type of the data in the SAS file, using SAS type
  names (float64 or string).

* __Must__: A boolean defining variables that do not need to be
  present in each file.  If true, the conversion will stop if the
  variable is missing in any of the SAS files.

factorize
=========




sortbuckets
===========