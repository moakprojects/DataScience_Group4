## IMPORT ##

import sys
import json
import matplotlib.pyplot as plt
import pandas as pd
import gzip
import locale
from pyspark import SparkConf, SparkContext
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import StructField, StructType, StringType


## CONFIG ##

sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)
locale.getdefaultlocale()
locale.getpreferredencoding()

conf = SparkConf().set('spark.driver.host', '127.0.0.1')
sc = SparkContext(master='local', appName='Video Games rank - Bar graph', conf=conf)
spark = SparkSession.builder.appName("Video Games rank - Bar graph").getOrCreate()


## DATA ##

s = sc.textFile('hdfs://namenode:9000/Video_Games_5.json.gz')
df = spark.read.json(s)


## PROCESS ##

# Printing important informations about the data
df.printSchema()
df.count()
df.describe('overall').show()

# Transforming the data
dataGraph = df.groupby('overall').count()

# RDD to Pandas for ploting
dataGraphP = dataGraph.toPandas()
dataGraphP = dataGraphP.sort_values(by=['overall'])


# Ploting
plot = dataGraphP.plot(kind='bar',x='overall',y='count')
fig = plot.get_figure()
fig.savefig('barGraph.png')

print("Process finished!")

