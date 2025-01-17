
'''
make sure to have bootstrap to install boto3 to connect to parameter store to get user name and passwork


/usr/bin/spark-submit --packages software.amazon.awssdk:ssm:2.17.157 
--jars s3://rovi-cdw/airflow-tvdm/ppe/jars/dependencies/mysql-connector-j-8.2.0.jar 
--deploy-mode client --master yarn 
--conf spark.driver.memory=5g 
--conf spark.executor.memory=11g 
--conf spark.sql.shuffle.partitions=10 
s3://s3-bucket/transfer_s3.py 
'''
import sys
from pyspark.sql import SparkSession
import boto3

def establishSqlConnection( AWS_REGION):
    session = boto3.Session()
    client = session.client('ssm', region_name = AWS_REGION)
    usr = client.get_parameter(Name="user")['Parameter']['Value']
    pwd = client.get_parameter(Name="password")['Parameter']['Value']
    return usr,pwd

ENV = sys.argv[1] 
MYSQL_HOST = sys.argv[2]  
MYSQL_DB=sys.argv[3]
MYSQL_TABLE=sys.argv[4]
S3_OUTPUT_PATH=sys.argv[5]
AWS_REGION=sys.argv[6]

print(f"""  ENV: {ENV}\n
            MYSQL_HOST: {MYSQL_HOST}\n
            MYSQL_DB: {MYSQL_DB}\n
            MYSQL_TABLE: {MYSQL_TABLE}\n
            S3_OUTPUT_PATH: {S3_OUTPUT_PATH}\n
            AWS_REGION: {AWS_REGION}
        """)

spark = SparkSession.builder \
    .appName("MySQL to S3") \
    .getOrCreate()

mysql_user,mysql_password= establishSqlConnection(AWS_REGION)

mysql_url = f"jdbc:mysql://{MYSQL_HOST}:3306/{MYSQL_DB}"
mysql_properties = {
    "user": mysql_user,
    "password": mysql_password,
    "driver": "com.mysql.cj.jdbc.Driver"
}

# df = spark.read.jdbc(url=mysql_url, table=MYSQL_TABLE, properties=mysql_properties)

df = spark.read \
    .format("jdbc") \
    .option("url", mysql_url) \
    .option("dbtable", MYSQL_TABLE) \
    .option("user", mysql_user) \
    .option("password", mysql_password) \
    .option("numPartitions", 10) \
    .option("partitionColumn", "table") \
    .option("lowerBound", 1) \
    .option("upperBound", 1000) \
    .load()

timeout = 1000
approx_count = df.rdd.countApprox(timeout)
print(f"approx_count: {approx_count}")
df\
    .write \
    .format("parquet") \
    .mode("overwrite") \
    .save(S3_OUTPUT_PATH)

print("transferred to S3")
