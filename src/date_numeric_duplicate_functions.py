# Databricks notebook source
from pyspark.sql.types import *
data=[["1","2020-02-01"],["2","2019-03-01"],["3","2021-03-01"]]
schema = StructType([
    StructField('id', IntegerType()),\
     StructField('date', DateType())      
])
df=spark.createDataFrame(data,["id","input"])
display(df)

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

data=[["1","2020-02-01"],["2","2019-03-01"],["3","2021-03-01"]]
schema = StructType([
    StructField('id', IntegerType()),\
     StructField('date', StringType())      
])
df=spark.createDataFrame(data,["id","date"])
# display(df.select(to_date('date').alias('new_date')))
df = df.withColumn('new_date', to_date('date'))

# COMMAND ----------

display(df.select(date_format('new_date', 'dd:MM:yyyy').alias('date-nn')))

# COMMAND ----------

display(df.select(datediff('new_date', current_date()).alias('date-diff')))
display(df.select(abs(datediff('new_date', current_date())).alias('date-diff')))

# COMMAND ----------

display(df.select(date_add('new_date', 35).alias('added_date')))
display(df)
display(df.select(date_add('new_date', -35).alias('added_date')))


# COMMAND ----------

display(df.select(round(abs(months_between('new_date', current_date())),2).alias('month-diff')))

# COMMAND ----------

display(df.select(round(abs(months_between('new_date', current_date()))/lit(12),2).alias('years-diff')))

# COMMAND ----------

display(df)
# display(df.select(days('new_date').alias('date')))
display(df.select(month('new_date').alias('month')))
display(df.select(year('new_date').alias('year')))


# COMMAND ----------

df.select(date_format('new_date', 'dd').alias('date')).display()

# COMMAND ----------

data = [
    (1, 'Alice', 25),
    (2, 'Bob', 30),
    (3, 'Cathy', 22),
    (1, 'Alice', 25),  # Duplicate
    (2, 'Bob', 33),    # Duplicate
    (4, 'David', 35)
]
dd = spark.createDataFrame(data, ['id', 'name', 'age'])
dd.distinct().display()

# COMMAND ----------

dd.dropDuplicates(['id', 'age']).display()

# COMMAND ----------

df.select(current_date().alias('cd'), current_timestamp().alias('cts')).display()


# COMMAND ----------

df = df.withColumn('ts', lit(current_timestamp()))
display(df)
df.select(to_timestamp('ts', 'HH-mm-ss yy-dd-MM').alias('csf')).display()

# COMMAND ----------

df = df.withColumn('ts', to_timestamp('date'))
df.display()

# COMMAND ----------

display(df.select(month('ts').alias('month'),year('ts').alias('year')))

# COMMAND ----------

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("scores", IntegerType(), True)  # ArrayType column
])

# Sample data with an array
data = [
    (1, "Alice", 80),
    (2, "Bob", 22),
    (3, "Cathy", 33),
    (3, "Cathy", 37)

]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)
df.agg(sum('scores').alias('sum'), avg('scores').alias('avg'), max('scores').alias('max'), min('scores').alias('min')).display()

# COMMAND ----------

df.filter(df.id==3).display()
df.filter((df.id==3) & (df.scores==33)).display()
df.filter((df.id==1) | (df.scores.isin([22, 33, 37]))).display()


# COMMAND ----------



# COMMAND ----------

import unittest
def fr(df):
    return df.select('id').filter(df.id==3)

class Testn(unittest.TestCase):
    def test_fil(self):
        schema = StructType([StructField('id', IntegerType())],)
        self.assertEqual(fr(df), spark.createDataFrame([(3,), (3,)],schema))

r = unittest.main(argv=[''], verbosity=2, exit=False)
# assert r.result.wasSuccessful(), 'Test failed; see logs above'

# COMMAND ----------

schema = StructType([StructField('id', IntegerType())],)

# COMMAND ----------


