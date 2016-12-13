# cff-bench
Benchmarking Columnar File Formats

## Background
Columnar file formats (cff) have become very popular in the big data realm. These technologies offer efficient serialization, indexing, and data retreival. Each of thse projects were born out of the necessity to process extremely large volumes of data, very quickly. The two most prominent examples of these in open source are Apachhe Parquet [1] and Apache ORC [2]. Each has influences from other works, but most importantly from Google's Dremel paper [3].

* [1] - https://parquet.apache.org/
* [2] - https://orc.apache.org/
* [3] - http://research.google.com/pubs/pub36632.html

## Purpose
This project is meant to offer a means to benchmark Parquet and ORC with an arbitrary dataset, schema, and indexing mechanisms without requiring Hive, Impala, Drill, etc. It comes with an example using NOAA weather collection from 1998-2000. See nooa_example.md
