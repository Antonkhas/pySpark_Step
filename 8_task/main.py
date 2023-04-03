from pyspark.sql import SparkSession
import pyspark.sql.functions as F


spark = SparkSession.builder \
    .appName('Task_8') \
    .master('local[*]') \
    .getOrCreate()

bank = spark.read.csv('bank.csv', sep=';', header=True)

result_bank = bank.filter(bank["balance"] > 1000)
result_bank = result_bank.orderBy(F.asc('balance'))

print(result_bank.show())
