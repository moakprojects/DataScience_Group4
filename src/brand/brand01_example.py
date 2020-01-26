from pyspark.sql.types import StructField, StructType, StringType
from pyspark.sql import Row
import json
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Python Spark SQL basic example").getOrCreate()

s = sc.textFile('hdfs://namenode:9000/reviews_Video_Games_5.json.gz')
df = spark.read.json(s)
df.printSchema()
df.count()
df.describe('overall').show()
dataGraph = df.groupby('overall').count()
print(dataGraph)
print(type(dataGraph))

dataGraphP = dataGraph.toPandas()
dataGraphP = dataGraphP.sort_values(by=['overall'])
print(dataGraphP)
print("TEST")
def plotGraph(data):
        plot = data.plot(kind='bar',x='overall',y='count')
        fig = plot.get_figure()
        fig.savefig('graph1.png')

plotGraph(dataGraphP)
print("FIN!")
