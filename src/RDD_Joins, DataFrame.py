# Databricks notebook source
bd = {1: "Alice",2: "Bob",3: "Charlie"}
bv = sc.broadcast(bd)
r = sc.parallelize([1, 2, 3, 4, 5, 6])
re = r.map(lambda x: bv.value.get(x, 'null'))
re.collect()

# COMMAND ----------

r = sc.parallelize([("a", 1), ("b", 4), ('c', 3)])
r1 = sc.parallelize([("a", 2), ('b', 5),('a',8), ('d', 6)])
print(r.join(r1).collect())
print(r1.join(r).collect())

# COMMAND ----------

r = sc.parallelize([("a", 1), ("b", 4), ('c', 3)])
r1 = sc.parallelize([("a", 2), ('b', 5),('a',8), ('d', 6)])
'''
For each element (k, v) in self, the resulting RDD will either contain all pairs (k, (v, w)) for w in other, or the pair (k, (v, None)) if no elements in other have key k.
'''
print(r.leftOuterJoin(r1).collect())
print(r1.leftOuterJoin(r).collect())

# COMMAND ----------

r = sc.parallelize([("a", 1), ("b", 4), ('c', 3)])
r1 = sc.parallelize([("a", 2), ('b', 5),('a',8), ('d', 6)])
'''
For each element (k, w) in other, the resulting RDD will either contain all pairs (k, (v, w)) for v in this, or the pair (k, (None, w)) if no elements in self have key k.
'''
print(r.rightOuterJoin(r1).collect())
print(r1.rightOuterJoin(r).collect())

# COMMAND ----------

r = sc.parallelize([("a", 1), ("b", 4), ('c', 3)])
r1 = sc.parallelize([("a", 2), ('b', 5),('a',8), ('d', 6)])
'''
For each element (k, v) in self, the resulting RDD will either contain all pairs (k, (v, w)) for w in other, or the pair (k, (v, None)) if no elements in other have key k.

Similarly, for each element (k, w) in other, the resulting RDD will either contain all pairs (k, (v, w)) for v in self, or the pair (k, (None, w)) if no elements in self have key k.
'''
print(r.fullOuterJoin(r1).collect())
print(r1.fullOuterJoin(r).collect())

# COMMAND ----------

data = [(1, 'arjun'), (2, 'reddy')]
schema = ['id', 'name']
df = spark.createDataFrame(data, schema)
df.show()

# COMMAND ----------

key_value_pairs = [("Alice",), ("Bob",), ("Charlie",)]
r = sc.parallelize(key_value_pairs)
df=r.toDF(["Name"])
df.show()

# COMMAND ----------

data = [("Alice",), ("Bob",), (789,)]
d = spark.createDataFrame(data, ['name'])
d.show()

# COMMAND ----------

df = spark.read.csv(path="/FileStore/tables/customers_100_csv.csv", header=True)
display(df)

# COMMAND ----------

df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/arjun37ca@gmail.com/tst/customers_100_csv.csv")

# COMMAND ----------

display(df1.take(3))


# COMMAND ----------

from pyspark.sql.types import StructType, StructField, MapType, StringType, IntegerType
data = [('Arjun', {'eyes':'black', 'hair':'black'}, 1000), ('reddy', {'eyes':'green', 'hair':'purple'}, 2000)]
schema = StructType([StructField('name', StringType()),\
                      StructField('pops',MapType(StringType(), StringType())),\
                         StructField('salary', IntegerType())])
df2 = spark.createDataFrame(data, schema)
df2.show()
df2.withColumn('hair', df2.pops.hair).withColumn('eyes', df2.pops.eyes).withColumn('salary', df2.salary*2).show(truncate=False)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, MapType, StringType, IntegerType
from pyspark.sql.functions import lit
data = [('Arjun', {'eyes':'black', 'hair':'black'}, 1000), ('reddy', {'eyes':'green', 'hair':'purple'}, 2000)]
schema = StructType([StructField('name', StringType()),\
                      StructField('pops',MapType(StringType(), StringType())),\
                         StructField('salary', IntegerType())])
df2 = spark.createDataFrame(data, schema)
df2.show()
df3 = df2.withColumn('kills', lit(10))
df2.show()
df3.collect()

# COMMAND ----------

from pyspark.sql.functions import col, when
dfw = df3.withColumn('level', when(col('salary')<=1000, 'junior').when(col('salary')>1000, 'senior'))
dfw.collect()
dfw.show(1)

# COMMAND ----------

display(dfw)
df5 = dfw.withColumnRenamed('pops', 'properties')
display(df5)

# COMMAND ----------

df2.printSchema()
df3.printSchema()

