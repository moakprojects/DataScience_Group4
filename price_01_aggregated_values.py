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

data = spark.sql("SELECT Min(NULLIF(price, 0)) as min_price, Max(price) as max_price, Avg(price) as avarage_price FROM data")
data.write.mode('append').json("/output/aggregated")

dataMode = spark.sql("SELECT count(price) as quantity, price FROM data GROUP BY price HAVING quantity = (SELECT max(quantity) FROM (SELECT count(price) as quantity FROM data GROUP BY price))")
dataMode.write.mode('append').json("/output/mode")