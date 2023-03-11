from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType

spark = SparkSession.builder\
    .appName('task-6')\
    .master('local[*]')\
    .getOrCreate()

df = spark.read.csv('import_empl_csv.csv', sep=';', header=True)

# Группировка данных по имени и вычисление общей суммы зп и средней dept_id
name_piople = df.groupBy("name").agg({"salary": "sum", "dept_id": "sum"})
product_info = df.groupBy("name").agg({"salary": "mean"})


# Переименование столбцов
name_piople = name_piople.withColumnRenamed("sum(salary)", "new_salary")
name_piople = name_piople.withColumnRenamed("sum(dept_id)", "new_dept_id")
product_info = product_info.withColumnRenamed("avg(salary)", "new_salary")

# Объединение данных в один DataFrame
product_data = name_piople.join(product_info, "name", "inner")

print(product_data.show())



# Затем можно использовать PySpark для распределенной обработки данных и оптимизации вычислений. Например,
# для расчета общей суммы продаж и средней цены можно использовать функцию map() для распределения данных по узлам кластера
# и параллельного вычисления значений.


# Создание пользовательской функции для вычисления общей суммы продаж
def calculate_total_sales(price, quantity):
    return float(price * quantity)


calculate_total_sales_udf = udf(calculate_total_sales, FloatType())

# Параллельное вычисление общей суммы продаж
product_data = product_data.withColumn("total_sales", calculate_total_sales_udf("average_price", "total_quantity_sold"))




# Для расчета средней цены можно использовать функцию reduce() для редукции данных, полученных на узлах кластера, и вычисления итогового значения.

# Пользовательская функция для вычисления средней цены
def calculate_average_price(total_sales, total_quantity_sold):
    return float(total_sales / total_quantity_sold)

calculate_average_price_udf = udf(calculate_average_price, FloatType())

# Вычисление итогового значения средней цены
average_price = product_data.rdd \
    .map(lambda row: (row.product_name, row.total_sales, row.total_quantity_sold)) \
    .map(lambda x: (x[0], [x[1], x[2], x[2]])) \
    .reduceByKey(lambda x, y: [x[0]+y[0], x[1]+y[1], x[2]+y[2]]) \
    .map(lambda x: (x[0], calculate_average_price_udf(x[1][0], x[1][2]))) \
    .toDF(["product_name", "average_price"])

# Объединение данных в итоговый DataFrame
result = product_data.join(average_price, "product_name", "inner")


# Результат выполнения задачи будет сохранён в переменной result. В данном примере мы использовали функции map() и reduce() для пар