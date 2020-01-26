from pyspark import SparkConf, SparkContext
import json
from pyspark.sql.types import StructField, StructType, StringType
from pyspark.sql import Row
import json
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession

conf = SparkConf().set('spark.driver.host', '127.0.0.1')
sc = SparkContext(master='local', appName='myAppName', conf=conf)
spark = SparkSession.builder.appName("Python Spark SQL basic example").getOrCreate()

files = "hdfs://172.200.0.2:9000/smallData"
txtFiles = sc.textFile(files, 20)
json = spark.read.json(txtFiles)

json.createOrReplaceTempView("data")
data = spark.sql("SELECT * FROM data")
data.show(20)
