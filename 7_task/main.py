from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pyspark.sql.functions as F


# Task 7: Разделение PySpark DataFrame на подмножества на основе условий


spark = SparkSession.builder \
    .appName('Task_7')\
    .master('local[*]') \
    .getOrCreate()

bank = spark.read.csv('bank.csv', sep=';', header=True)

# Фильтруем записи marital == 'single'
df1 = bank.filter(col('marital') == 'single')

# Фильтруем записи по колонке balance > 1001 - по возрастанию
df2 = bank.filter(col('balance') > 1001)
df2 = df2.orderBy(F.asc('balance'))


df1.show(5)
df2.show(5)

