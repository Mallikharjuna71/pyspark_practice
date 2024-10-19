# Databricks notebook source
df = spark.read.format("json").load("dbfs:/FileStore/shared_uploads/arjun37ca@gmail.com/organizations_100.json")
display(df)

# COMMAND ----------

   df = spark.read.json("dbfs:/FileStore/shared_uploads/arjun37ca@gmail.com/organizations_100.json", multiLine=True)
   display(df.take(10))

# COMMAND ----------

d = [(1, 'A', True, 10.5, 'X'),
    (2, None, False, 20.3, 'Y'),
    (None, 'C', None, 30.1, None),
    (4, 'D', True, None, 'Z'),
    (None, None, False, 40.5, None),
    (6, 'A', None, 50.5, 'X'),
    (7, 'B', True, None, 'Y'),
    (None, 'C', False, 60.2, 'Z'),
    (9, None, None, 70.7, None),
    (None, 'D', True, 80.3, 'X'),
    (11, 'A', False, None, 'Y'),
    (None, None, None, 90.1, None),
    (13, 'B', True, None, 'Z'),
    (None, 'C', False, 100.0, None),
    (15, None, None, 110.5, 'X'),
    (16, 'D', True, None, 'Y'),
    (None, 'A', False, None, 'Z'),
    (18, 'B', None, 120.5, None),
    (None, 'C', True, None, 'X'),
    (20, None, False, 130.1, 'Y'),
    (None, None, None, None, None),
]

# Define the schema
columns = ['Column1', 'Column2', 'Column3', 'Column4', 'Column5']
d2 = spark.createDataFrame(d, columns)
display(d2)

# COMMAND ----------

display(d2.filter(d2.Column2.isNull() & d2.Column3.isNull()))

# COMMAND ----------

display(d2.filter(d2.Column1.isNotNull() & d2.Column4.isNotNull()))

# COMMAND ----------

display(d2.na.drop())

# COMMAND ----------

display(d2.fillna(69, subset=['Column4', 'column1']))
display(d2.fillna('###', subset=['Column2', 'column3']))
d2.printSchema()

# COMMAND ----------

display(d2.replace('A', 'tt').show(10))


# COMMAND ----------

# d2.na.replace(None, 99, 'Column1').show(10)

# COMMAND ----------

from pyspark.sql.functions import coalesce
d3 = d2.withColumn('nc1',coalesce('Column1', 99))
d3.show()
