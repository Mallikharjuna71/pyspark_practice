# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
data = [("1234567891234567",),("5678912345671234",),("9123456712345678",),("1234567812341122",),("1234567812341342",)]
schema = ['card_number',]
credit_card_df = spark.createDataFrame(data, schema)
credit_card_df.display()

# COMMAND ----------

credit_card_df.rdd.getNumPartitions()

# COMMAND ----------

rp_df = credit_card_df.rdd.coalesce(5)

# COMMAND ----------

rp_df.getNumPartitions()

# COMMAND ----------

od = rp_df.repartition(8)
od.getNumPartitions()

# COMMAND ----------

def mask(k):
    return f"{'*'*(len(k)-4)+k[-4:]}"
mask_udf = udf(lambda x: mask(x))
credit_card_df.withColumn('masked_card_number', mask_udf(credit_card_df.card_number)).display()
