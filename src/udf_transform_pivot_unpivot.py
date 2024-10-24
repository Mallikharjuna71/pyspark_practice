# Databricks notebook source
data = [('im a human',),(None,),('i dint know whats happening',), (None,)]
schema = ['txt',]
df = spark.createDataFrame(data, schema)
df.display()

# COMMAND ----------

def capt(k):
    rs = ''
    if k is None:
        return None
    else:
        sd = k.split(' ')
        for x in sd:
            rs = rs+x[0:1].upper()+x[1:]+" "
        return rs
# df.show()

# COMMAND ----------

# def capt1(k):
#     cl = []
#     rs = ''
#     if k is None:
#         return None
#     else:
#         sd = k.split(' ')
#         for x in sd:
#             rs = rs+x[0:1].upper()+x[1:]+" "
#         return rs
# df.show()

# COMMAND ----------

from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import udf
uc = udf(lambda a: capt(a))
# uc1 = udf(lambda x: capt1(x))

df.withColumn('txt', uc(df.txt)).display()
df.select(uc(df.txt).alias('uc')).display()

# df.withColumn('txt1', uc1(df.txt)).display()

# COMMAND ----------

from pyspark.sql.functions import upper, lit
simpleData = (("Java",4000,5), \
    ("Python", 4600,10),  \
    ("Scala", 4100,15),   \
    ("Scala", 4500,15),   \
    ("PHP", 3000,20),  \
        (None, None,20)
  )
columns= ["CourseName", "fee", "discount"]
df = spark.createDataFrame(simpleData, columns)
def up(df):
    return df.withColumn('CourseName',upper(df.CourseName))

def np(df):
    return df.withColumn('np', lit(3000))

def dp(df):
    return df.withColumn('dp', df.fee-(df.fee*df.discount/100))

df.transform(up).transform(np).transform(dp).display()
# ndf.display()




# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import from_json, to_json
d = [(1,Row(name='arjun', score='37'))]
d = spark.createDataFrame(d, ['key', 'value'])
d.withColumn('jc', to_json(d.value)).display()

# COMMAND ----------

data = [("Cherry",1000,"USA"), ("Carrots",1500,"USA"), (None,1600,"USA"), \
      ("Orange",2000,"USA"),("Orange",2000,"USA"),("Cherry",400,"China"), \
      ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"),("Cherry",1000,"USA"),\
      ("Cherry",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico")]
 
columns= ["Product","Amount","Country"]

d = spark.createDataFrame(data, columns)
d.display()

# COMMAND ----------

pd = d.groupby("Product").pivot("Country").sum("Amount")
pd1 = d.groupby("Country").pivot("Product").sum("Amount")
countries = ['China', 'USA']
pivotDF = d.groupBy("Product").pivot("Country", countries).sum("Amount")
pivotDF.display()
pd.display()
pd1.display()

# COMMAND ----------

df2 = spark.createDataFrame([
    Row(training="expert", sales=Row(course="dotNET", year=2012, earnings=10000)),
    Row(training="junior", sales=Row(course="Java", year=2012, earnings=20000)),
    Row(training="expert", sales=Row(course="dotNET", year=2012, earnings=5000)),
    Row(training="junior", sales=Row(course="dotNET", year=2013, earnings=48000)),
    Row(training="expert", sales=Row(course="Java", year=2013, earnings=30000)),
])  
df2.show()
df2.groupby('training').pivot('sales.course').sum('sales.earnings').display()
df2.groupby('training').pivot('sales.course', ['dotNET']).sum('sales.earnings').display()

# COMMAND ----------

data = [("Spain", 101, 201, 301), \
        ("Taiwan", 102, 202, 302), \
        ("Italy", 103, 203, 303), \
        ("China", 104, 204, 304)]
columns= ["Country", "2018x", "2019x", "2020x"]
df = spark.createDataFrame(data, columns)
df.display()

# COMMAND ----------

from pyspark.sql.functions import *
df.select('Country', expr("stack(3, '2018', 2018x, '2019', 2019x, '2020', 2020x) as(country, idn)")).display()

# COMMAND ----------

jd = spark.read.json('dbfs:/FileStore/shared_uploads/arjun37ca@gmail.com/sample_json_3_2.json', multiLine=True)
jd.printSchema()
# jd.display()

# COMMAND ----------

jd.columns
