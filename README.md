FSAL
====

FSAL is a stupid "File System Abstraction Layer" for Erlang. It is
intended to be used mainly for benchmarking purposes. Since I needed
to compare the performance of a few different storage solutions, I
figured it would be a good approach to separate my benchmark logic
from the file system details, and thus FSAL was born.

There are currently three FSAL backends available:

* Amazon S3
* File backend (regular *nix file system)
* NilFS (dummy backend)
