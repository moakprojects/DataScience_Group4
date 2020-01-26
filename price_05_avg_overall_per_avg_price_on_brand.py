from pyspark import SparkConf, SparkContext
import locale
from pyspark.sql import SparkSession

locale.getdefaultlocale()
locale.getpreferredencoding()

conf = SparkConf().set('spark.driver.host', '127.0.0.1')
spark = SparkSession \
            .builder \
            .appName("Aggregated Price Values") \
            .config("spark.some.config.option", "some-value") \
            .getOrCreate()
			
json = spark.read.json("hdfs://namenode:9000/inputProduct")

json.createOrReplaceTempView("data")

data = spark.sql("SELECT avg(overall), avg(price), brand From data INNER JOIN data2 ON data.asin = data2.asin GROUP BY brand")
data.write.mode('append').json("/output/avg_overall_per_avg_price")