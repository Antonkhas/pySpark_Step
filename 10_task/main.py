from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
import pyspark.sql.functions as F


spark = SparkSession.builder \
    .appName('Task 10') \
    .master('local[*]') \
    .getOrCreate()


bank = spark.read.csv('bank.csv', sep=';', header=True)

# Устанавливаем новое количество партиций
df_bank = bank.repartition(10)

# Выводим информацию о количестве партиций
print("Number of partitions:", df_bank.rdd.getNumPartitions())

# Рассчитываем среднее значение по каждой группе, о возрастанию
avg_bank = df_bank.groupBy('age').agg(avg('balance')).orderBy(F.asc('age'))

avg_bank.show()
