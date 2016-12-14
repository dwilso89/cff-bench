# Example

## NOAA - 2008-2010

Detailed information about the dataset can be found in ftp://ftp.ncdc.noaa.gov/pub/data/gsod/readme.txt

### 1 - Download the data from their ftp site.

<dl>
  <dd>ftp://ftp.ncdc.noaa.gov/pub/data/gsod/1998/gsod_1998.tar</dd>
  <dd>ftp://ftp.ncdc.noaa.gov/pub/data/gsod/1999/gsod_1999.tar</dd>
  <dd>ftp://ftp.ncdc.noaa.gov/pub/data/gsod/2000/gsod_2000.tar</dd>
</dl>

### 2 - Turn each archive into one uncompressed CSV per year. The dataset is fixed width, so there is some manual cleanup and removal of headers required.

```
tar -xf gsod_2000.tar -C 2000/
gzip -d 2000/*
cat 2000/*.op > 2000/tmp.txt
sed 's/.\{108\}/&,/' 2000/tmp.txt | sed -e 's/.\{117\}/&,/' | sed -e 's/.\{125\}/&,/' | sed -e 's/[[:space:]]\{1,\}/,/g' | sed '/S.*T/d' > 2000/gsod_2000.txt
rm 2000/tmp.txt 2000/*.op
```

Note: The archives could be directly transformed into a columnar file but it will be easier to deal with a one to one conversion

### 3 - Convert to Parquet

Convert

```
java -cp cff-bench-0.0.1.jar cff.bench.convert.ConvertCSVToParquet -csvFile 2000/gsod_2000.txt -schemaFile noaa_parquet.schema -delimiter ',' -parquetFile 2000/gsod_2000.parquet -dict
```

Print File

```
java -cp cff-bench-0.0.1.jar cff.bench.convert.ParquetPrinter -parquetFile gsod_2000.parquet
```

### 4 - Convert to ORC

```
java -cp cff-bench-0.0.1.jar cff.bench.convert.ConvertCSVToORC -csvFile 2000/gsod_2000.txt -schemaFile noaa_orc.schema -delimiter ',' -orcFile 2000/gsod_2000.orc -dict
```

Print File

```
java -cp cff-bench-0.0.1.jar cff.bench.convert.ORCPrinter -orcFile gsod_2000.orc
```

### 5 - Run MapReduce over Parquet

Run multiple jobs with various schema projections by specifying a projected schema

Example run:
```
hadoop jar cff-bench-0.0.1.jar cff.bench.mr.ReadParquet -in /in/parquet/2000/gsod_2000.parquet -out parquetFourColumns -schemaFile noaa_4cols_parquet.schema
```

### 6 - Run MapReduce over ORC

Example run:
```
hadoop jar cff-bench-0.0.1.jar cff.bench.mr.ReadORC -in /in/parquet/2000/gsod_2000.orc -out orcFourColumns -schemaFile noaa_4cols_orc.schema
```

#### Parquet Results:
![Alt text](/img/parquetResults.png?raw=true "Parquet Results Chart")

#### ORC Results:
![Alt text](/img/orcResults.png?raw=true "ORC Results Chart")
