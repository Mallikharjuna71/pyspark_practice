# Databricks notebook source
r = sc.parallelize([(1,), (2,), (3,), (4,)])
r.collect()

# COMMAND ----------

df = spark.createDataFrame(r, ['id'])
df.show()

# COMMAND ----------

df1 = r.toDF(['id'])
df1.show()

# COMMAND ----------

r = df1.rdd
r.collect()

# COMMAND ----------

df = spark.read.csv(path="/FileStore/tables/customers_100_csv.csv", header=True)
display(df)

# COMMAND ----------

df.count()

# COMMAND ----------

df.select(df.Index, (df.Index+10).alias('new_index')).take(10)

# COMMAND ----------

display(df.select('*').take(10))

# COMMAND ----------

display(df.select('*').where(df.Country == 'Bulgaria'))

# COMMAND ----------

display(df.select('Index', 'Company', 'Country').filter(df.Country.like('B%')))

# COMMAND ----------

display(df.select('Index', 'Company', 'Country').filter(df.Country.isin(['Bolivia', 'Bulgaria'])))

# COMMAND ----------

display(df.describe())

# COMMAND ----------

df1 = spark.createDataFrame(r, ['id'])
# display(df1.summary())
display(df1.describe())

# COMMAND ----------

df.columns

# COMMAND ----------

df = spark.createDataFrame([(2, "Alice"), (5, "Bob")], schema=["age", "name"])
df = df.withColumns({'age2': df.age + 2, 'age3': df.age + 3})
display(df)
# df1 = df.withColumnsRenamed({'age2': 'age4', 'age3': 'age5'}).show()
# display(df1)

# COMMAND ----------

from pyspark.sql.functions import when
data = [("Alice", 25), ("Bob", 35), ("Charlie", 20)]
df = spark.createDataFrame(data, ["name", "age"])
display(df)
df = df.withColumn("category", 
                   when(df.age < 25, "Young")
                   .when(df.age >= 25, "Adult")
                   .otherwise("Senior"))
# display(df.select('*', df.withColumn("category", 
#                    when(df.age < 25, "Young")
#                    .when(df.age >= 25, "Adult")
#                    .otherwise("Senior"))))

display(df)

# COMMAND ----------

display(df.select('name', 'category', (df.age+10).alias('new_age')))
display(df)
