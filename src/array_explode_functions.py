# Databricks notebook source
d = spark.createDataFrame([(1, 'a', 5), (2, 'b', 5)], ['id', 'n', 'ii'])
d.display()

# COMMAND ----------

from pyspark.sql.functions import array, col
d1 = d.withColumn('ar',array(col('id'), col('ii')))
d1.display()

# COMMAND ----------

from pyspark.sql.functions import array_contains

d1.select(array_contains(col('ar'), 1).alias('ind')).display()

# COMMAND ----------

from pyspark.sql.functions import *

d1.select(size(col('ar')).alias('len')).display()

# COMMAND ----------

from pyspark.sql.functions import *

d1.select(array_position(col('ar'), 5).alias('ind')).display()

# COMMAND ----------

from pyspark.sql.functions import *

d1.select(array_remove(col('ar'), 5).alias('len')).display()

# COMMAND ----------

data = [
    (1, ["apple", "banana", None]),
    (2, ["orange", None, "grape"]),
    (3, None),
    (None, ["pear", "peach"])
]

# Create DataFrame
d = spark.createDataFrame(data, ["id", "fruits"])
d.display()

# COMMAND ----------

from pyspark.sql.functions import *
d.select(d.id, explode(d.fruits).alias('fruit')).show()

# COMMAND ----------

d.select(d.id, explode_outer(d.fruits).alias('fruit')).show()

# COMMAND ----------

d.select(d.id, posexplode(d.fruits).alias('ind','fruit')).show()

# COMMAND ----------

d.select(d.id, posexplode_outer(d.fruits).alias('ind','fruit')).show()

# COMMAND ----------


