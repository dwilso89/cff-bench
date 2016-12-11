# cff-bench
Benchmarking Columnar File Formats

## Background
Columnar file formats (cff) have become very popular in the big data realm. These technologies offer efficient serialization, indexing, and data retreival. Each of thse projects were born out of the necessity to process extremely large volumes of data, very quickly. The two most prominent examples of these in open source are Apachhe Parquet [1] and Apache ORC [2]. Each has influences from other works, but most importantly from Google's Dremel paper [3].

* [1] - https://parquet.apache.org/
* [2] - https://orc.apache.org/
* [3] - http://research.google.com/pubs/pub36632.html

## Purpose
This project is meant to offer a means to quickly benchmark Parquet and ORC with an arbitrary dataset, schema, and indexing mechanisms. It comes with an example using NOAA weather collection from 1998-2000.

## Example

### NOAA - 2008-2010

Detailed information about the dataset can be found in ftp://ftp.ncdc.noaa.gov/pub/data/gsod/readme.txt

1. Download the data from their ftp site.
```
ftp://ftp.ncdc.noaa.gov/pub/data/gsod/1998/gsod_1998.tar
ftp://ftp.ncdc.noaa.gov/pub/data/gsod/1999/gsod_1999.tar
ftp://ftp.ncdc.noaa.gov/pub/data/gsod/2000/gsod_2000.tar
```

2. Turn each archive into one uncompressed CSV per year

```
tar -xf gsod_2000.tar -C 2000/
gzip -d 2000/*
cat 2000/*.op > 2000/gsod_2000.txt
rm 2000/*.op
```

Note: The archives could be directly transformed into a cff it will be easier to deal with a one to one conversion

3. Convert to Parquet


4. Convert to ORC
