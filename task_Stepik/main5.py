from matplotlib import pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

spark = SparkSession.builder \
    .master('local[*]') \
    .appName('Stepik_4_task') \
    .getOrCreate()

data = spark.read.csv('stocks_price_final.csv', sep=',', header=True)


sec_df = data.select(['sector',
                      'open',
                      'close',
                      'adjusted'])\
                     .groupBy('sector')\
                     .agg(avg('open'),
                          avg('close'),
                          avg('adjusted'))\
                     .toPandas()
ind = list(range(12))
ind.pop(6)
sec_df.iloc[ind, :].plot(kind='bar', x='sector', y=sec_df.columns.tolist()[1:],
                         figsize=(12, 6), ylabel='Stock Price', xlabel='Sector')
plt.show()
