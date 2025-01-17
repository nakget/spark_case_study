# spark_case_study

Here I am writing publishing some my notes created while working.
Recently I was writing data from MySQL table to AWS S3 bucket. table size is 3.5 gb. We can get the table storage in MySQL by running the query

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
 
Initially I thought I can read to pandas dataframe and write it to S3. some it didn't work, I don't remember what was the issue, but usually I don't prefer pandas if the size is more than 1gb.
I wrote simple pyspark script to read the MySQL table and wrote the data to S3 in parquet format, It is not suprise that parquet takes less space , but didn't expected less than 50%, i.e., it took only 24% that is 3.5gb in MySQL table and it took 850mb in S3, it is all parquet and snappy compression magic.
mysql table size. : 3.5gb 
S3 parquet snappy size : 850mb

We have to understand this is not always the case, parquet file size depends on how many column values are repeated and how many columns are int/decimal or string.

| mysql   table size       | 3.5gb |
|--------------------------|-------|
| S3 parquet   snappy size | 850mb |

Created spark cluster in AWS EMR service with following configuration:
Spark 3.5
Primary node : r6g.xlarge (4 vCore, 30.5 GiB memory)
core node    : r6g.2xlarge (8 vCore, 61.0 GiB memory)
```
/usr/bin/spark-submit --packages software.amazon.awssdk:ssm:2.17.157
--jars s3://rovi-cdw/airflow-tvdm/ppe/jars/dependencies/mysql-connector-j-8.2.0.jar
--deploy-mode client
--master yarn
--conf spark.driver.memory=20g
--conf spark.executor.memory=20g
--conf spark.executor.cores=4 <pyspark script file path> 
```

with this configuration job was taking 12 minutes of cluster time (billing time) and 5minutes 30 seconds to complete the job. when I checked the Spark UI, noticed that data was read as single partition from MySQL table and reading task was taking 3.2 minutes. Spark created 2 jobs, one for reading the data from MySQL and one for writing to S3 bucket. In query execution plan we can see there is Exchange (shuffle) happening, this shuffle is due the repartition(10) in my code. I repartitioned the dataframe to 10, just to make sure we will have good sized part file (100-150 mb) instaed of single big file. 

<img width="1709" alt="image" src="https://github.com/user-attachments/assets/a5ae1ae6-fe68-4f1c-bbee-a50828b5d83b" />

<img width="439" alt="image" src="https://github.com/user-attachments/assets/2c7705d4-2ba7-497f-bcc4-6631dcc87e88" />

Later after some days I revisited the job , I felt alloted more resource than the job required. input data is just 3.5gb, and no transformation  before writing to S3 bucket. and I tried with below config on next run

Primary node : r6g.xlarge (4 vCore, 30.5 GiB memory)
core node    : r6g.2xlarge (8 vCore, 61.0 GiB memory)

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
I thought, I will fine tune by explicitly mentioning the number of executor 4 and cores 3, to my surprise job didn't complete even after 1 hour and failed, I couldn't get exact error in stderr and Spark UI.

Primary node : r6g.xlarge (4 vCore, 30.5 GiB memory)
core node    : r6g.xlarge (4 vCore, 30.5 GiB memory)
My thought was, job may take more time when both compute and memory reduced, to my surprise, job didn't complete even after one hour, I didn't get any clue in Spark UI to understand what happening, All I could notice is job was having one executor with 
