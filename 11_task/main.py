from pyspark.sql import SparkSession

spark = SparkSession.builder \
  .appName('Task 11') \
  .master('local[*]') \
  .getOrCreate()

bank = spark.read.csv('bank.csv', sep=';', header=True)

print('Количество строк в исходном датасете: ', bank.count())

bank_del = bank.dropDuplicates(["age"])
print("Количество строк в датасете после удаления дубликатов:", bank_del.count())
print(bank_del.show(67))