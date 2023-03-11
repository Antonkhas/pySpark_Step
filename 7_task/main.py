from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pandas as pd
import matplotlib.pyplot as plt


# Создание экземпляра SparkSession
# spark = SparkSession.builder\
#     .master('local[*]')\
#     .appName("task-7")\
#     .getOrCreate()
#
# # Создание DataFrame из CSV файла
# df = spark.read.options(header='True', sep=';') \
#                   .csv('import_empl_csv.csv')
#
# # Группировка данных по отделам и наименованию продукта
# agg = df.groupBy(F.month("dept_id")) \
#                     .agg(F.sum("prod"))
#
# # Преобразование DataFrame в Pandas DataFrame для построения графиков
# agg_pd = agg.toPandas()
#
# print(df.show())
#



# Создание экземпляра SparkSession
spark = SparkSession.builder.appName("Sales Data").getOrCreate()

# Создание DataFrame из CSV файла
sales_df = spark.read.options(header='True', inferSchema='True') \
                  .csv('/file/path/to/sales_data.csv')

print(sales_df.show())