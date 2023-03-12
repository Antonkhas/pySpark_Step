from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import *


spark = SparkSession.builder \
    .master('local[*]') \
    .appName('Stepik_4_task') \
    .getOrCreate()

data = spark.read.csv('bank.csv', sep=';', header=True)


# Фильтрую колонку от 30, группирую возраст и считаю количество
data = data.filter(col('age') > lit('30')).groupBy('age').count()

# Вывожу по возрастанию
data = data.orderBy(F.asc('age'))

print(data.show(5))


