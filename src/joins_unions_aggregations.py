# Databricks notebook source
emp = [(1,"Smith",-1,"2018","10","M",3000), \
       (2,"Rose",1,"2010","20","M",4000), \
       (1,"Williams",1,"2010","30","M",1000), \
       (4,"Jones",2,"2005","10","F",2000), \
       (5,"Brown",2,"2010","40","",-1), \
       (6,"Brown",2,"2010","50","",-1), \
        (3,"Brown",2,"2010",None,"",-1), \
       (None,"Brown",2,"2010","50","",-1) \

  ]
empColumns = ["emp_id","name","superior_emp_id","year_joined", \
       "emp_dept_id","gender","salary"]

dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]
deptColumns = ["dept_name","dept_id"]


# COMMAND ----------

d1 = spark.createDataFrame(emp,empColumns)
d2 = spark.createDataFrame(dept,deptColumns)
d1.join(d2,d1.emp_dept_id== d2.dept_id, 'inner').show()
d2.join(d1,d1.emp_dept_id== d2.dept_id, 'inner').show()

d1.join(d2,d1.emp_dept_id== d2.dept_id, 'leftsemi').show()
d2.join(d1,d1.emp_dept_id== d2.dept_id, 'leftsemi').show()


# COMMAND ----------

d1.join(d2,d1.emp_dept_id== d2.dept_id, 'leftanti').show()
d2.join(d1,d1.emp_dept_id== d2.dept_id, 'leftanti').show()

# COMMAND ----------

from pyspark.sql.functions import col
d3 = d1.alias('d3')
d4 = d1.alias('d4')

d3.join(d4, col('d3.superior_emp_id')==col('d4.emp_id'), 'inner').select(col('d3.emp_id'), col('d3.name'), col('d4.name').alias('s_name')).show()

# COMMAND ----------

pd = spark.read.parquet('dbfs:/FileStore/shared_uploads/arjun37ca@gmail.com/mtcars.parquet')
pd.display()

# COMMAND ----------

from pyspark.sql.functions import broadcast
d1.join(broadcast(d2), d1.emp_dept_id== d2.dept_id, 'leftanti').show()

# COMMAND ----------

data1 = [
    (1, 'Alice', 30),
    (2, 'Bob', 25),
    (3, 'Charlie', 35)
]

columns1 = ['emp_id', 'name', 'age']
u1 = spark.createDataFrame(data1, schema=columns1)

data2 = [
    (3, 'Charlie', 35),
    (4, 'David', 28),
    (5, 'Eve', 32)
]

columns2 = ['age', 'name', 'emp_id']
u2 = spark.createDataFrame(data2, schema=columns2)



# COMMAND ----------

u1.union(u2).distinct().display()
u1.unionAll(u2).display()


# COMMAND ----------

df1 = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
df2 = spark.createDataFrame([("Charlie", 3, 25)], ["name","id", "age"])

result = df1.unionByName(df2, allowMissingColumns=True)
result.show()

# COMMAND ----------

from pyspark.sql.functions import collect_list, collect_set
data = [("Alice", "Apple"), ("Alice", "Banana"), ("Bob", "Apple"), ("Bob", "Cherry"), ("Bob", "Cherry")]
c = spark.createDataFrame(data, ["Name", "Fruit"])
c.groupBy('Name').agg(collect_list('Fruit').alias('clf'), collect_set('Fruit').alias('csf')).show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import lag, lead
from pyspark.sql import Window
w = Window.partitionBy('Name').orderBy('Fruit')
c.withColumn('lag', lag('Fruit', 1).over(w)).show()
c.withColumn('lag', lag('Fruit', 1, 'unknown').over(w)).show()
c.withColumn('lead', lead('Fruit', 1).over(w)).show()
c.withColumn('lead', lead('Fruit', 1, 'unknown').over(w)).show()

# COMMAND ----------

c.withColumn('lead', lead('Fruit', 1, 'unknown').over(w)).withColumn('lag', lag('Fruit', 1, 'unknown').over(w)).show()

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct
c.groupBy('Name').agg(count('Fruit').alias('clf'), countDistinct('Fruit').alias('csf')).show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import first, last
c.groupBy('Name').agg(first('Fruit').alias('first'), last('Fruit').alias('last')).show(truncate=False)

# COMMAND ----------


