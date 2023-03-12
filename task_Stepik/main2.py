import pyspark.sql.functions as F
from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .master('local[*]') \
    .appName('Spark_2_example') \
    .getOrCreate()


data = spark.read.csv('bank.csv', sep=';', header=True)


# Группировка данных по возрасту
data = data.groupBy('age').count()
# Нахождение 5 наиболее часто встречающихся возрастов
data = data.orderBy(F.desc('count'))
print(data.show(5))
