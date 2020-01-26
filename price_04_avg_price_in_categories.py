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

data = spark.sql("SELECT min(price) min_price, max(price) max_price, avg(price) avg_price, COUNT(*) as quantity, categories FROM data GROUP BY categories HAVING quantity > 30 ORDER BY avg_price DESC LIMIT 20")
data.write.mode('append').json("/output/category_avg_price")