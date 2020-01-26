## IMPORT ##

import sys
import json
import matplotlib.pyplot as plt
import pandas as pd
import gzip
import locale
import pyspark.sql.functions as F
from pyspark import SparkConf, SparkContext
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import StructField, StructType, StringType


## CONFIG ##

sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)
locale.getdefaultlocale()
locale.getpreferredencoding()

conf = SparkConf().set('spark.driver.host', '127.0.0.1')
sc = SparkContext(master='local', appName='Rank mean over years', conf=conf)
spark = SparkSession.builder.appName("Rank mean over years").getOrCreate()


## DATA ##

s = sc.textFile('hdfs://namenode:9000/subsets/Video_Games_5.json.gz')
df = spark.read.json(s)


## PROCESS ##

df2 = df.withColumn("date",F.to_timestamp(df["unixReviewTime"]))
df2 = df2.select(F.date_format('date','yyyy-MM').alias('month'),"overall").groupby('month').agg({"overall":"avg"}).orderBy("month")

dataGraphP = df2.toPandas()

# Plot
dataGraphP.plot(kind='line',x='month',y='avg(overall)', color='red')
plt.savefig('ratingTime.png')

print("Process finished!")

