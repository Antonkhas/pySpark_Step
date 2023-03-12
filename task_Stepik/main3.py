from pyspark.sql import SparkSession
import pyspark.sql.functions as F


spark = SparkSession.builder \
    .master('local[*]') \
    .appName('Spark_3_task') \
    .getOrCreate()

data = spark.read.csv('bank.csv', sep=';', header=True)

# Группировка данных по возрасту
data = data.groupBy('age').count()
# Нахождение 5 наиболее часто встречающихся возрастов
data = data.orderBy(F.asc('age'))
print(data.show(5))