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

data = spark.sql("SELECT tmp.range as range, COUNT(*) as quantity FROM (SELECT CASE WHEN price BETWEEN 0.00 AND 19.99 THEN '0-19' WHEN price BETWEEN 20.00 AND 39.99 THEN '20-39' WHEN price BETWEEN 40.00 AND 59.99 THEN '40-59' WHEN price BETWEEN 60.00 AND 79.99 THEN '60-79' WHEN price BETWEEN 80.00 AND 99.99 THEN '80-99' WHEN price BETWEEN 100.00 AND 119.99 THEN '100-119' WHEN price BETWEEN 120.00 AND 139.99 THEN '120-139' WHEN price BETWEEN 140.00 AND 159.99 THEN '140-159' WHEN price BETWEEN 160.00 AND 179.99 THEN '160-179' WHEN price BETWEEN 180.00 AND 199.99 THEN '180-199' END as range FROM data) as tmp GROUP BY tmp.range")
data.write.mode('append').json("/output/distribution_small")
