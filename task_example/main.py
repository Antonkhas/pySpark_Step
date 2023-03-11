from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions as F


data_schema = [
               StructField('_c0', IntegerType(), True),
               StructField('symbol', StringType(), True),
               StructField('date', DateType(), True),
               StructField('open', DoubleType(), True),
               StructField('high', DoubleType(), True),
               StructField('low', DoubleType(), True),
               StructField('close', DoubleType(), True),
               StructField('volume', IntegerType(), True),
               StructField('adjusted', DoubleType(), True),
               StructField('market.cap', StringType(), True),
               StructField('sector', StringType(), True),
               StructField('industry', StringType(), True),
               StructField('exchange', StringType(), True),
            ]

final_struc = StructType(fields=data_schema)

spark = SparkSession.builder \
    .appName('Example Stepik') \
    .master('local[*]') \
    .getOrCreate()


data = spark.read.csv('stocks_price_final.csv',
                      sep=',', header=True,
                      schema=final_struc)

# data = data.withColumn('new_volume', data.volume)
# data = data.drop('symbol', 'close', 'volume', 'adjusted', 'market.cap', 'exchange')
# data = data.withColumnRenamed('high', 'lov')
# data = data.withColumnRenamed('low', 'high')
# data = data.withColumnRenamed('lov', 'low')
# data = data.withColumn('new_date', data.date)
# data = data.drop('date')

# Удаляет пустые значения NaN
# data = data.na.drop()



# Фильтрует данные
# print(data.filter((col('date') >= lit('2020-01-01')) & (col('date') <= lit('2020-01-31'))).show(5))


# Возвращает Тру, если соответствует указанному отрезку
# print(data.filter(data.adjusted.between(100.0, 500.0)).show())

# When - Он возвращает 0 или 1 в зависимости от заданного условия
# data.select('open', 'close',
#             when(data.adjusted <= 200.0, 1).otherwise(0)).show(5)



# Приведенный ниже код демонстрирует использование rlike() для извлечения имен секторов, которые начинаются с букв B или C
# data.select(
#     'sector',
#     data.sector.rlike('^[B,C]').alias('Колонка sector начинается с B или C')).distinct().show()


# В приведенном ниже примере объясняется, как получить среднюю цену открытия, закрытия и скорректированную цену акций по отраслям
# (data.select(['industry', 'open', 'close', 'adjusted'])
#     .groupBy('industry')
#     .mean()
#     .show()
#  )

# Агрегирование
# (data.filter((F.col('date') >= F.lit('2019-01-02')) & (F.col('date') <= F.lit('2020-01-31')))
#  .groupBy("sector")
#  .agg(F.min("date").alias("С"),
#       F.max("date").alias("По"),
#
#       F.min("open").alias("Минимум при открытии"),
#       F.max("open").alias("Максимум при открытии"),
#       F.avg("open").alias("Среднее в open"),
#
#       F.min("close").alias("Минимум при закрытии"),
#       F.max("close").alias("Максимум при закрытии"),
#       F.avg("close").alias("Среднее в close"),
#
#       F.min("adjusted").alias("Скорректированный минимум"),
#       F.max("adjusted").alias("Скорректированный максимум"),
#       F.avg("adjusted").alias("Среднее в adjusted"),
#
#       ).show(truncate=False)
#  )

