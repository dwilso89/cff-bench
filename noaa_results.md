## NOAA in CFFs Benchmarks

### Benchmarking Column Access
<p>
After following the instructions in [here](noaa_example.md), you can gather the results of accessing different column projections in Parquet and ORC formats.
</p>

#### Test Setup

##### "Hardware"
<ul>
<li>Amazon EC2 - m3.large</li>
</ul>

##### Software
<ul>
<li>Hadoop 2.7.2 in pseudo-distributed mode</li> 
<li>Centos 6.5</li>
</ul>

#### Test Description

Time measurements were taken for reading 1 to 25 columns from each set of parquet and ORC files. There were 5 runs for each test. The mean was then calculated from these 5 results.

The columns were accessed cumulatively; first column [1], then columns [1, 2], then [1, 2, 3], etc.

### Results:

#### CFF Size
![Alt text](/img/noaaSize.PNG?raw=true "Size Results Chart")

#### Column Access Time
![Alt text](/img/noaaTime.PNG?raw=true "Column Access Results Chart")

#### Conclusion
At a very high level, it may be noteworthy that parquet compressed this dataset slightly better than ORC. Additionally, ORC's data access performance outpaced parquet. However, both of these statements would be misleading.

No conclusion about which technology performs better can really be made at this point. These tests were run using a single dataset, unoptimized schemas, and default indexing/compression. As well, these results were taken on virtualized hardware that has minimal guarantees on exact performance

This was meant as an example. The tools developed/included in cff-bench should be useful in making decisions about one particular schema vs another. They should also be useful for quickly prototyping one particular cff implementation without the need to deploy Spark/Drill/Hive/Impala/etc
