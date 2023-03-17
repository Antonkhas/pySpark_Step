# Задача: Отобразить изменение средних значений аудио характеристик от года к году.
#
# Такими характеристиками являются acousticness, danceability, energy, speechiness, liveness и valence .
# Произвести сортировку полученной таблицы по столбцу year по возрастанию. Средние значения округлить до 2ух знаков после запятой.

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import round

spark = SparkSession.builder \
    .master('local[*]') \
    .appName('Spark_exam') \
    .getOrCreate()

data = spark.read.csv('exam.csv', sep=',', header=True)

# Определяем необходимые столбцы
data = (data.select(['year',
                     'acousticness',
                     'danceability',
                     'energy',
                     'speechiness',
                     'liveness',
                     'valence']))

# Фильтруем по году от 1921
# Группирую по году
# Ищем среднее значение столбца и округляем до 2 знаков после запятой
# Распределяем год по возрастанию

var = (data.filter((F.col('year') >= F.lit('1921')))
       .groupBy('year')
       .agg(round(F.avg("acousticness"), 2).alias('acousticness'),
            round(F.avg("danceability"), 2).alias('danceability'),
            round(F.avg("energy"), 2).alias('energy'),
            round(F.avg("speechiness"), 2).alias('speechiness'),
            round(F.avg("liveness"), 2).alias('liveness'),
            round(F.avg("valence"), 2).alias('valence'))
       .orderBy(F.asc('year')))


print(var.show(5))
