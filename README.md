# cff-bench
Creating and Benchmarking Columnar File Formats

## Background
Columnar file formats (cff) have become very popular in the big data realm. These technologies offer efficient serialization, indexing, and data retreival. Each of thse projects were born out of the necessity to process extremely large volumes of data, very quickly. The two most prominent examples of these in open source are Apache Parquet [1] and Apache ORC [2]. Each has influences from other works, but most importantly from Google's Dremel paper [3].

* [1] - https://parquet.apache.org/
* [2] - https://orc.apache.org/
* [3] - http://research.google.com/pubs/pub36632.html

## Purpose
This project is meant to offer a means to benchmark Parquet and ORC with an arbitrary dataset, schema, and indexing mechanisms without requiring Hive, Impala, Drill, etc. It comes with an example using NOAA weather collection from 1998-2000. See [noaa_example.md](noaa_example.md)

## Features

### Print CFF Serialized Data

```
#Parquet
java -cp cff-bench-0.0.1.jar cff.bench.convert.ParquetPrinter -parquetFile ${YOUR_FILE}

#ORC
java -cp cff-bench-0.0.1.jar cff.bench.convert.ORCPrinter -orcFile ${YOUR_FILE}
```

### Convert Data to a CFF

```
#Parquet
java -cp cff-bench-0.0.1.jar cff.bench.convert.ConvertCSVToParquet -csvFile ${YOUR_FILE} -schemaFile ${YOUR_PARQUET_SCHEMA} -delimiter ${DELIMITER) -parquetFile ${OUTPUT_FILE}

#ORC
java -cp cff-bench-0.0.1.jar cff.bench.convert.ConvertORCToParquet -csvFile ${YOUR_FILE} -schemaFile ${YOUR_ORC_SCHEMA} -delimiter ${DELIMITER) -parquetFile ${OUTPUT_FILE}
```

### Read Specific Columns from CFF (MapReduce)

```
#Parquet
hadoop jar cff-bench-0.0.1.jar cff.bench.mr.ReadParquet -in ${INPUT_FILE_OR_DIR} -out ${OUTPUT_DIR} -schemaFile ${PROJECTED_PARQUET_SCHEMA}

#ORC
hadoop jar cff-bench-0.0.1.jar cff.bench.mr.ReadORC -in ${INPUT_FILE_OR_DIR} -out ${OUTPUT_DIR} -schemaFile ${INPUT_SCHEMA} -columns ${COMMA_SEPARATED_LIST_OF_COLUMN_INDEXES}
```
