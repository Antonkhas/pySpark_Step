from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = SparkSession.builder \
    .appName('Task 9') \
    .master('local[*]') \
    .getOrCreate()

bank = spark.read.csv('bank.csv', sep=';', header=True)

# Переименование столбцов
ex1 = bank.withColumnRenamed('job', 'new_job')
ex1.show(5)

# Изменение типов данных столбцов
ex2 = bank.withColumn("age", col("age").cast("float"))
ex2.printSchema()

# Удаление дубликатов
ex3 = bank.dropDuplicates(['marital'])
ex3.show()
