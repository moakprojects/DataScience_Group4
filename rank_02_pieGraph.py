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
sc = SparkContext(master='local', appName='Video Games rank - Pie graph', conf=conf)
spark = SparkSession.builder.appName("Video Games rank - Pie graph").getOrCreate()


## DATA ##

s = sc.textFile('hdfs://namenode:9000/subsets/Video_Games_5.json.gz')
df = spark.read.json(s)


## PROCESS ##

total = df.count()
dataGraph = df.groupby('overall').count()
dataGraphP = dataGraph.toPandas()
dataGraphP = dataGraphP.sort_values(by=['overall'])

r1 = (dataGraphP.iloc[0].loc["count"])
r2 = (dataGraphP.iloc[1].loc["count"])
r3 = (dataGraphP.iloc[2].loc["count"])
r4 = (dataGraphP.iloc[3].loc["count"])
r5 = (dataGraphP.iloc[4].loc["count"])

# Data to plot
labels = '1/5', '2/5', '3/5', '4/5', '5/5'
sizes = [r1,r2,r3,r4,r5]
colors = ['#A20101', '#F44E54', '#FF9904', '#FDDB5E','#BAF1A1']
patches, texts, test = plt.pie(sizes, colors=colors, autopct='%1.1f%%')
plt.legend(patches, labels, loc="best")

# Plot
plt.axis('equal')
plt.title('Video Games Grading')
plt.show()
plt.savefig('pieGraph.png')

print("Process finished!")