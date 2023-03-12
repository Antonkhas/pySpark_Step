from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder \
    .master('local[*]') \
    .appName('Spark_example') \
    .getOrCreate()

data = spark.read.csv('bank.csv',
                      sep=';',
                      header=True
                      )


print(data.groupBy('age')
      .agg(count('age').alias('count'))
      .show(5)
      )