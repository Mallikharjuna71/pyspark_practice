# Databricks notebook source
json_data = spark.read.format("json").options(multiLine=True).load("dbfs:/FileStore/shared_uploads/arjun37ca@gmail.com/json/nested_json_file.json")
json_data.display()

# COMMAND ----------

from pyspark.sql.functions import *
fltten_json_data = json_data.select('id', 'properties', explode('employees').alias('employee')).select( '*','employee.empId', 'employee.empName', "properties.name", "properties.storeSize").drop('properties', 'employee')
fltten_json_data.display()

# COMMAND ----------

print(json_data.count())
print(fltten_json_data.count())

# COMMAND ----------

data = [
    (1, ["apple", "banana", None]),
    (2, ["orange", None, "grape"]),
    (3, None),
    (None, ["pear", "peach"])
]

d = spark.createDataFrame(data, ["id", "fruits"])
d.display()
d.select('id', explode(d.fruits).alias('fruits')).display()
d.select('id', explode_outer(d.fruits).alias('fruits')).display()
d.select('id', posexplode(d.fruits).alias('index','fruits')).display()


# COMMAND ----------

fltten_json_data.filter(fltten_json_data.empId==1001).display()

# COMMAND ----------

def convertColumnName(d):
    for i in d.columns:
        sc = ''
        for c in i:
            if c.isupper():
                sc += '_'+c.lower()
            else:
                sc += c
        d = d.withColumnRenamed(i, sc)
    return d
fltten_json_data = convertColumnName(fltten_json_data)
fltten_json_data.display()


# COMMAND ----------

fltten_json_data = fltten_json_data.withColumn('load_date', current_date())
fltten_json_data.display()

# COMMAND ----------

fltten_json_data = fltten_json_data.withColumn('year', year(col('load_date'))).withColumn('month', month(col('load_date'))).withColumn('day', date_format(col('load_date'), 'dd').alias('day'))
fltten_json_data.display()
