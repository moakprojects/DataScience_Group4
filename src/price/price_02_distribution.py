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

data = spark.sql("SELECT tmp.range as range, COUNT(*) as quantity FROM (SELECT CASE WHEN price BETWEEN 0.00 AND 199.99 THEN ’0-199’ WHEN price BETWEEN 200.00 AND 399.99 THEN ’200-399’ WHEN price BETWEEN 400.00 AND 599.99 THEN ’400-599’ WHEN price BETWEEN 600.00 AND 799.99 THEN ’600-799’ WHEN price BETWEEN 800.00 AND 999.99 THEN ’800-999’ END as range FROM data) as tmp GROUP BY tmp.range")
data.write.mode('append').json("/output/distribution")
