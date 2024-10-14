# Databricks notebook source
data = [(1, 'arjun'), (2, 'reddy')]
schema = ['id', 'name']
df = spark.createDataFrame(data, schema)
df.show()

# COMMAND ----------

df = spark.read.csv(path="/FileStore/tables/customers_100_csv.csv", header=True)
display(df)

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls /FileStore/tables/customers_100_csv.csv

# COMMAND ----------

df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/arjun37ca@gmail.com/tst/customers_100_csv.csv")

# COMMAND ----------

# from pyspark.sql.types import array, array_contains
# from pyspark.sql.functions import lit, col, array, array_contains, split, explode
# schema = ['id', 'name', 'skills']
# df = spark.createDataFrame([(1, 'arjun', ['html','css','javascript']), (2, 'reddy', ['python','sql','pandas'])], schema=schema)
# df = df.withColumn('skills', explode(col('skills')))
# df.show(truncate=False)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, MapType, StringType, IntegerType
data = [('Arjun', {'eyes':'black', 'hair':'black'}, 1000), ('reddy', {'eyes':'green', 'hair':'purple'}, 2000)]
schema = StructType([StructField('name', StringType()),\
                      StructField('pops',MapType(StringType(), StringType())),\
                         StructField('salary', IntegerType())])
df2 = spark.createDataFrame(data, schema)
df2.withColumn('hair', df2.pops.hair).withColumn('eyes', df2.pops.eyes).withColumn('salary', df2.salary*2).show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import map_keys, map_values, explode
df3 = df2.select('name', 'salary', map_keys('pops').alias('keys'), map_values('pops').alias('values'))


# COMMAND ----------

from pyspark.sql import Row
man = [Row(name = 'arjun',prop = Row(age=28, death=2046)),\
    Row(name = 'reddy',prop = Row(age=50, death=2046)),\
        Row(name = 'meka',prop = Row(age=55, death=2046))]
df4 = spark.createDataFrame(man)
df4.printSchema()

# COMMAND ----------

from pyspark.sql.functions import when
df5 = df4.select('name', when(df4.prop.age<=40, 'adult')\
                            .when(df4.prop.age<=50, 'man')\
                                .otherwise('old').alias('class'))
df5.show()

# COMMAND ----------

df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/arjun37ca@gmail.com/tst/customers_100_csv.csv")
display(df1.filter(df1.Country.like('S%')).sort(df1.City))

# COMMAND ----------

display(df1.filter(df1.Country == 'Swaziland'))
display(df1.where(df1.Country =='Sri Lanka'))

# COMMAND ----------

from pyspark.sql.functions import count, avg, min, max
data = [(1, 'arjun', 'IT', 2000), (2, 'reddy', 'HR', 3000), (3, 'meka', 'IT', 4000), (4, 'goku', 'FT', 3000), (5, 'whis', 'FT', 5000)]
schema = ['id', 'name', 'dept', 'salary']
df1 = spark.createDataFrame(data, schema)
display(df1.groupBy('dept').agg(count('*').alias('emp'),\
                                max('salary').alias('max'),\
                                min('salary').alias('min'),\
                                avg('salary').alias('avg')))

# COMMAND ----------

data1 = [(1, 'arjun', 2000, 2), (2, 'reddy', 3000, 1), (3, 'meka', 1000, 4)]
schema1 = ['id', 'name', 'salary', 'dep']
data2 = [(1, 'IT'), (2, 'HR'), (3, 'Payroll')]
schema2 = ['id', 'name']
empDf = spark.createDataFrame(data1, schema1)
depDf = spark.createDataFrame(data2, schema2)
display(empDf)
display(depDf)
display(empDf.join(depDf, empDf.dep == depDf.id, 'inner'))
display(empDf.join(depDf, empDf.dep == depDf.id, 'left'))
display(empDf.join(depDf, empDf.dep == depDf.id, 'right'))
display(empDf.join(depDf, empDf.dep == depDf.id, 'full'))


# COMMAND ----------

data = [(1, 'arjun', 'CEO', 'M'),(1, 'katy', 'HR', 'F'),(1, 'juli', 'HR', 'F'),(1, 'goku', 'IT', 'M'),(1, 'whis', 'IT', 'M'),]
schema = ['id', 'name', 'dept', 'sex']
df = spark.createDataFrame(data, schema)
display(df)

# COMMAND ----------

df = spark.createDataFrame([
    ("Product A", "East", 100),
    ("Product B", "West", 200),
    ("Product A", "West", 150),
    ("Product B", "East", 50)
], ["Product", "Region", "Sales"])

pivot_df = df.groupBy("Product").pivot("Region", ['East']).sum("Sales")

pivot_df.show()

# COMMAND ----------

data = [("Spain", 101, 201, 301), \
        ("Taiwan", 102, 202, 302), \
        ("Italy", 103, 203, 303), \
        ("China", 104, 204, 304)]
columns= ["Country", "2018x", "2019x", "2020x"]
df = spark.createDataFrame(data, columns)
display(df)

# COMMAND ----------

from pyspark.sql.functions import  *
 
unPivotDF = df.select("Country", expr("stack(3, '2018', 2018x, '2019', 2019x, '2020', 2020x) as (year, cpi)"))
unPivotDF.show()

# COMMAND ----------

data = [("Spain", 101, 201, 301), \
        ("Taiwan", 102, None, 302), \
        ("Italy", None, 203, 303), \
        ("China", 104, 204, None)]
columns= ["Country", "2018x", "2019x", "2020x"]
df = spark.createDataFrame(data, columns)
display(df)

# COMMAND ----------

# m = df.fillna(0)
# display(m)


# COMMAND ----------



# COMMAND ----------

df = spark.range(1, 101)
df1 = df.sample(0.1, 9)
df2 = df.sample(0.1, 9)
display(df2)
display(df1)

# COMMAND ----------

data = [(1, 'arjun', 2000), (2, 'reddy', 3000)]
schema = ['id', 'name', 'salary']
df = spark.createDataFrame(data, schema)
display(df)

# COMMAND ----------

# from pyspark.sql.functions import upper
# def cu(df):
#     return df.withColumn('upper', upper(df.name))
# def ds(df):
#     return df.withColumn('d_salary', df.salary*2)
# df1 = df.transform(cu).transform(ds)
# display(df1)

# COMMAND ----------

# from pyspark.sql.functions import transform, lower
# data = [(1, ['python', 'sql']), (2, ['react', 'node'])]
# schema = ['id', 'skills']
# df = spark.createDataFrame(data, schema)
# df1 = df.select('id', transform('skills', lambda x: upper(x)).alias('SKILLS'))
# df1.show(truncate = False)
# def con_up(x):
#     return lower(x)
# df1.select('id', transform('skills', con_up).alias('lower')).show(truncate=False)
#

# COMMAND ----------

# data = [(1, 'arjun', 2000), (2, 'reddy', 3000)]
# schema = ['id', 'name', 'salary']
# df = spark.createDataFrame(data, schema)
# df.createOrReplaceTempView('employees')
# df1 = spark.sql('select * from employees')
# df1.show()

# COMMAND ----------

# # data = [(1, 'arjun', 2000, 1000), (2, 'reddy', 3000, 1500)]
# # schema = ['id', 'name', 'salary', 'bonus']
# # df = spark.createDataFrame(data, schema)
# # from pyspark.sql.functions import udf, col
# # from pyspark.sql.types import IntegerType
# # def total_pay(x, y):
# #     return x+y
# # Total_pay = udf(lambda x, y: total_pay(x, y), IntegerType())
# # df.select('*', Total_pay(col('salary'), col('bonus')).alias('total_pay')).show()
# # df.withColumn('totpay', Total_pay(df.salary, df.bonus)).show()
#
#
# # COMMAND ----------
#
# data = [(1, 'arjun', 20000, 1000), (2, 'reddy', 35000, 1500)]
# schema = ['id', 'name', 'salary', 'bonus']
# df = spark.createDataFrame(data, schema)
# df.createOrReplaceTempView('employee')
# def total_pay(x, y):
#     return x+y
# spark.udf.register(name = 'TotalPay', f = total_pay, returnType=IntegerType())
#

# COMMAND ----------

# MAGIC %sql
# MAGIC select * , TotalPay(salary, bonus) as totpay from employee

# COMMAND ----------

data = [(1, 'arjun'), (2, 'reddy')]
rdd = spark.sparkContext.parallelize(data)
print(rdd.collect())

# COMMAND ----------


