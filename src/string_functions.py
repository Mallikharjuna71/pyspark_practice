# Databricks notebook source
df = spark.read.csv(path="/FileStore/tables/customers_100_csv.csv", header=True)
display(df.take(10))

# COMMAND ----------

from pyspark.sql.functions import count
df.groupBy('Company', 'Country').agg(count('*').alias('count')).show()


# COMMAND ----------

display(df.orderBy('Country', ascending=False))

# COMMAND ----------

df.groupBy('Country').agg(count('*').alias('count')).orderBy('count', ascending=False).show()


# COMMAND ----------

display(df.sort(df.Country.desc()).take(10))

# COMMAND ----------

from pyspark.sql.types import StringType
display(df.select(df.Index.cast(StringType()).alias('SI')).take(5))

# COMMAND ----------

from pyspark.sql.functions import *
df.select(upper(df.Country).alias('CU')).show(10)
df.select(lower(df.Country).alias('CU')).show(10)

# COMMAND ----------

d = [(1, ' india '),(2, ' ap ')]
d1 = spark.createDataFrame(d, ['id', "name"])
d1.collect()

# COMMAND ----------

d1.select(ltrim(d1.name).alias('ln')).show()
d1.select(rtrim(d1.name).alias('rn')).show()
d1.select(trim(d1.name).alias('tn')).show()



# COMMAND ----------

d = [(' india ',),(' ap ',)]
d2 = spark.createDataFrame(d,['text'])
d2.select(translate('text', ' ', '_').alias('n_text')).collect()

# COMMAND ----------

from pyspark.sql.functions import *
d3 = spark.createDataFrame([("Hello World",)], ["text"])
d3.select(substring('text', 0,5).alias('ss')).show()
d3.select(substring_index('text', 'o',1).alias('si')).show()
d3.select(substring_index('text', 'o',2).alias('si')).show()


# COMMAND ----------

# DBTITLE 1,ext
d3 = spark.createDataFrame([("Hello welcome to the World ",)], ["text"])
d3.select(split('text', ' ', -1).alias('st')).show(truncate=False)
d3.select(split('text', ' ', 3).alias('st')).show(truncate=False)
d3.select(split('text', ' ', 1).alias('st')).show(truncate=False)

# COMMAND ----------

d = [('india',),('ap',)]
d2 = spark.createDataFrame(d,['text'])
d2.select(repeat('text', 2).alias('rt')).show()

# COMMAND ----------

d2.select(lpad('text', 3, '-').alias('lpt')).show()
d2.select(lpad('text', 6, '-').alias('lpt')).show()
d2.select(rpad('text', 3, '-').alias('lpt')).show()
d2.select(rpad('text', 6, '-').alias('lpt')).show()

# COMMAND ----------

d2.select(length('text').alias('lnt')).show()

# COMMAND ----------

d3 = spark.createDataFrame([("John Doe", "john.doe@example.com"), ("Jane Smith", "jane.smith@example.com")], ["name", "email"])

d3.select(regexp_extract("email", r"@(\w+)\.", 0).alias("domain")).show()
d3.select(regexp_extract("email", r"@(\w+)\.", 1).alias("domain")).show()

# COMMAND ----------

d3.select(regexp_replace('email', r".com", '.in').alias('rr')).show(truncate=False)
