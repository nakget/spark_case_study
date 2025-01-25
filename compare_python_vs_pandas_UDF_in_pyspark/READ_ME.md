# Benchmark: Comparing Python UDF vs Pandas UDF in PySpark

This benchmark compares the performance of two types of UDFs in PySpark: a standard Python UDF and a Pandas UDF. Typically, Pandas UDFs are expected to perform better than standard Python UDFs due to their optimized vectorized operations, which are particularly effective when dealing with larger datasets. However, in this benchmark using a sample email campaign dataset, the Python UDF outperformed the Pandas UDF, which was an unexpected result.

```
Result 1: 
Rows in DataFrame: 1,000,000 
Time taken with Python UDF: 0.02101 minutes 
Time taken with Pandas UDF: 0.07204 minutes

Result 2: 
Rows in DataFrame: 1,000,000 
Time taken with Python UDF: 0.02149 minutes 
Time taken with Pandas UDF: 0.06983 minutes

Result 3: 
Rows in DataFrame: 10,000 
Time taken with Python UDF: 0.0054 minutes 
Time taken with Pandas UDF: 0.05357 minutes
```
### Possible Reason for Unexpected Results

The performance anomaly observed in this benchmark may be attributed to the local Spark setup on my machine. Since Pandas UDFs are designed to exploit parallelism in a distributed environment, the lack of a multi-node cluster and the limited resources on my local setup could have affected the performance outcome.

### Benchmark Results

Below are the results from the benchmark, comparing the execution times for both UDF types on different dataset sizes:

