from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import round

spark = SparkSession.builder \
    .master('local[*]') \
    .appName('Spark_exam') \
    .getOrCreate()

df = spark.read.csv('exam.csv', sep=',', header=True)

var = (df.filter((F.col('year') >= F.lit('1951')) & (F.col('artists') >= F.lit('Sergei Rachmaninoff')))
       .groupBy('year'))


data = var.orderBy(F.asc('year'))

print(data.sh)