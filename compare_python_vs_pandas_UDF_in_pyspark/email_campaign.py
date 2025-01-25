# '''
# export PATH=/Users/mywork/spk1/.venv/bin:/opt/homebrew/opt/openjdk@11/bin:/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home:/opt/homebrew/bin:/opt/homebrew/sbin:/Library/Frameworks/Python.framework/Versions/3.11/bin:/usr/local/bin:/System/Cryptexes/App/usr/bin:/usr/bin:/bin:/usr/sbin:/sbin:/var/run/com.apple.security.cryptexd/codex.system/bootstrap/usr/local/bin:/var/run/com.apple.security.cryptexd/codex.system/bootstrap/usr/bin:/var/run/com.apple.security.cryptexd/codex.system/bootstrap/usr/appleinternal/bin
# export JAVA_HOME=/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home

# pip install pandas==2.2.3 pyspark==3.5.4 pyarrow
# pip install setuptools
# '''

from faker import Faker
import random
fake=Faker()

from pyspark.sql.functions import pandas_udf,udf,col
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,StringType,StructType,IntegerType,TimestampType,BooleanType
import pandas as pd
import time

schema=StructType([
StructField("email",StringType(),False),
StructField("user_id",StringType(),False),
StructField("email_content",StringType(),False),
StructField("sent_time",StringType(),False)
])

spark=SparkSession.builder.appName("djd").getOrCreate()

fake_data=[
    (
        fake.email(),
        str(random.randrange(1,10)),
        fake.sentence()+random.choice(['',str(random.randrange(1,10))]),
        fake.date_time_between(start_date="-1y", end_date="now").strftime("%Y-%m-%d %H:%M:%S")
    )
    for _ in range(1000000)
]

df=spark.createDataFrame(fake_data,schema=schema)

@pandas_udf(BooleanType())
def find_user_pandas(user:pd.Series,content:pd.Series)-> pd.Series:
    # return user.combine(content,lambda u,c:u in c)
    return content.str.contains(user, regex=False)

def find_user(user,content)->bool:
    return True if (user in content) else False

find_user_udf=udf(find_user,BooleanType())

start=time.time()
df1=df.withColumn("is_user_mentioned",find_user_pandas(df["user_id"],df["email_content"]))
df1.write.mode("overwrite").csv("with_pandas_udf")
end=time.time()
ptime=(end-start)/60

ustart=time.time()
df2=df.withColumn("is_user_mentioned",find_user_udf(col("user_id"),col("email_content")))
df2.write.mode("overwrite").csv("with_udf")
uend=time.time()
utime=(uend-ustart)/60
# df2.show(100,False)
print(f"time taken with udf:{round(utime,5)}\ntime taken with pandas udf:{round(ptime,5)}")

