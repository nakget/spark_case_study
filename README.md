# spark_case_study

Here, I am publishing some of my notes created while working on Spark. Recently, I was writing data from a MySQL table to an AWS S3 bucket. The table size was approximately 3.5 GB. To get the table storage size in MySQL, the following query can be used:

```
SELECT 
 table_name AS `Table`, 
 round(((data_length + index_length) / 1024 / 1024), 2) `Size in MB` ,
 round((( index_length) / 1024 / 1024), 2) `index Size in MB` ,
 round(((data_length ) / 1024 / 1024), 2) `data Size in MB` 
FROM information_schema.TABLES 
WHERE table_schema = "db"
 AND table_name = "table_name";
```
 
Initially, I thought of using Pandas to read the table into a DataFrame and write it to S3. However, it didn’t work (I can’t recall the exact issue), but generally, I avoid using Pandas for datasets larger than 1 GB.

I then wrote a simple PySpark script to read the MySQL table and write the data to S3 in Parquet format. As expected, Parquet took significantly less space—only 24% of the original size. The MySQL table was 3.5 GB, but the Parquet file (with Snappy compression) in S3 was just 850 MB. This illustrates the efficiency of Parquet and Snappy compression.

Summary of Table Sizes

| mysql   table size       | 3.5gb |
|--------------------------|-------|
| S3 parquet   snappy size | 850mb |

Observations
The reduction in file size depends on several factors, such as the repetition of column values and the data types of the columns (e.g., int/decimal vs. string).

Cluster Configuration
I created a Spark cluster in AWS EMR with the following configuration:
Spark Version: 3.5
Primary Node: r6g.xlarge (4 vCore, 30.5 GiB memory)
Core Node: r6g.2xlarge (8 vCore, 61.0 GiB memory)
The PySpark script was submitted using:
```
/usr/bin/spark-submit --packages software.amazon.awssdk:ssm:2.17.157
--jars s3://rovi-cdw/airflow-tvdm/ppe/jars/dependencies/mysql-connector-j-8.2.0.jar
--deploy-mode client
--master yarn
--conf spark.driver.memory=20g
--conf spark.executor.memory=20g
--conf spark.executor.cores=4 <pyspark script file path> 
```

Execution Details
Cluster Time: 12 minutes (billing time)
Job Completion Time: 5 minutes 30 seconds
The Spark UI revealed that:

Data Read: The MySQL table was read as a single partition, taking 3.2 minutes.
Repartitioning: I used repartition(10) in my code to ensure smaller part files (100–150 MB) instead of a single large file, which introduced a shuffle stage.
Spark Execution Plan
Spark created two jobs:
Reading data from MySQL.
Writing data to S3.
The shuffle (Exchange) in the query plan occurred due to the repartition(10) operation.

<img width="1709" alt="image" src="https://github.com/user-attachments/assets/a5ae1ae6-fe68-4f1c-bbee-a50828b5d83b" />

<img width="439" alt="image" src="https://github.com/user-attachments/assets/2c7705d4-2ba7-497f-bcc4-6631dcc87e88" />

Optimization Attempts
Later, I revisited the job and realized I had allocated more resources than necessary for a 3.5 GB input with no transformations. Here’s what I tried:

Attempt 1: Reduced Resources
Primary Node: r6g.xlarge (4 vCore, 30.5 GiB memory)
Core Node: r6g.2xlarge (8 vCore, 61.0 GiB memory)

```
/usr/bin/spark-submit --packages software.amazon.awssdk:ssm:2.17.157
--jars s3://rovi-cdw/airflow-tvdm/ppe/jars/dependencies/mysql-connector-j-8.2.0.jar
--deploy-mode client
--master yarn
--conf spark.driver.memory=20g
--conf spark.executor.memory=13g
--conf num-executors=4 
--conf spark.executor.cores=3
<pyspark script file path> 
```
Result: The job failed after running for over an hour. The error logs and Spark UI didn’t provide clear insights. Tasks were getting killed unexpectedly.

Attempt 2: Further Reduced Resources
Primary Node: r6g.xlarge (4 vCore, 30.5 GiB memory)
Core Node: r6g.xlarge (4 vCore, 30.5 GiB memory)
Observation:
The job didn’t complete after 35 minutes.
Shuffle tasks took longer to finish.


Final Solution
I applied the following changes:

Configuration:

Primary Node: r6g.xlarge (4 vCore, 30.5 GiB memory)
No core node (left it to Spark's dynamic allocation).
```
/usr/bin/spark-submit --packages software.amazon.awssdk:ssm:2.17.157
--jars s3://rovi-cdw/airflow-tvdm/ppe/jars/dependencies/mysql-connector-j-8.2.0.jar
--deploy-mode client
--master yarn
--conf spark.driver.memory=5g
--conf spark.executor.memory=11g
--conf spark.sql.shuffle.partitions=10
<pyspark script file path> 
```
Reading Optimization: Added numPartitions=10 when reading the MySQL table. This ensured that the data was split into 10 tasks for parallel processing instead of a single large task, which previously caused garbage collection (GC) issues.

Key Insights
Avoid over-allocating resources for smaller datasets, as it can increase job complexity and cost.
Use numPartitions to optimize data read from MySQL.
Minimize shuffle by tuning spark.sql.shuffle.partitions or avoiding unnecessary repartitioning.

<img width="481" alt="image" src="https://github.com/user-attachments/assets/b0ceaaad-542b-47b1-aac7-ea6fd3824fd0" />

