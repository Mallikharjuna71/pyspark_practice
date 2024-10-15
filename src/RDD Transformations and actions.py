# Databricks notebook source
r1 = sc.parallelize([1, 2,3, 4], 2)
print(r1.glom().collect())

# COMMAND ----------

r2 = sc.textFile('dbfs:/FileStore/shared_uploads/arjun37ca@gmail.com/organizations_100.csv')
r2.take(3)


# COMMAND ----------

r = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
r1 = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 2)
r2 = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 3)

print(r.getNumPartitions())
print(r1.getNumPartitions())
print(r2.getNumPartitions())

# COMMAND ----------

r3 = r.map(lambda x: x*2)
print(r3.collect())

# COMMAND ----------

r4 = r.filter(lambda x: x%2==0)
print(r3.collect())

# COMMAND ----------

r4 = r.reduce(lambda x, y: x+y)
print(r4)

# COMMAND ----------

r = sc.parallelize([1, 2,3, 4, 5, 6], 3)
def sm(i, s):
    s = sum(s)
    return [i, s]
print(r.mapPartitionsWithIndex(sm).collect())


# COMMAND ----------

r = sc.parallelize([1, 2,3, 4, 5, 6, 7, 8], 4)
def sm(i):
    yield sum(i)
print(r.mapPartitions(sm).collect())

# COMMAND ----------

r = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
r1 = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 2)
r2 = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 3)

print(r.glom().collect())
print(r1.glom().collect())
print(r2.glom().collect())

# COMMAND ----------

r = sc.parallelize([1, 2, 3, 4, 5, 6])
r1 = sc.parallelize([1, 2, 4, 6, 7, 8])
r2 = r.union(r1).distinct()
print(r2.collect())
r3 = r.intersection(r1)
print(r3.collect())

# COMMAND ----------

d = [
    ("Alice", 1),
    ("Bob", 2),
    ("Charlie", 3),
    ("David", 4),
    ("Eva", 5),
    ("Frank", 6),
    ("Alice", 7),   
    ("Bob", 8),     
    ("Heidi", 9),
    ("Judy", 10)
]
r = sc.parallelize(d)
r1 = r.groupByKey()
# r1 = r.groupByKey().mapValues(list)
print([(x, list(y)) for x, y in r1.collect()])
# print(r1.collect())

# COMMAND ----------

d = [
    ("Alice", 1),
    ("Bob", 2),
    ("Charlie", 3),
    ("David", 4),
    ("Eva", 5),
    ("Frank", 6),
    ("Alice", 7),   
    ("Bob", 8),     
    ("Heidi", 9),
    ("Judy", 10)
]
r = sc.parallelize(d)
r.reduceByKey(lambda x, y: x+y).collect()

# COMMAND ----------

r2 = r.reduceByKey(lambda x, y: x+y)
print(r2.collect())

# COMMAND ----------

r = sc.parallelize([1, 2, 3, 4, 5, 6])
r1 = r.flatMap(lambda x: (x, x*3))
print(r1.collect())

# COMMAND ----------

rdd = sc.parallelize([(1, "a"), (2, "b"), (1, "b"), (3, "d"), (2, "e")])
r1 = rdd.groupBy(lambda x: x[1])
r2 = rdd.groupBy(lambda x: x[0])
print([(x, list(y)) for x, y in r1.collect()])
print([(x, list(y)) for x, y in r2.collect()])

# COMMAND ----------

#print(r.count())
r = sc.parallelize([1, 2,3, 40,5, 6, 7, 8, 10, 4], 5)
print(r.top(3))# gives you highest values in descending order
print(r.take(5)) # gives you first specified no of values 

# COMMAND ----------

r = sc.parallelize([(1, "b"), (2, "b"), (1, "b"), (3, "d"), (2, "e")])
print(r.countByKey())
print(r.countByValue())


# COMMAND ----------

key_value_pairs = [("Alice",), ("Bob",), ("Charlie",)]
r = sc.parallelize(key_value_pairs)

df=r.toDF(["Name"])

df.show()



# COMMAND ----------

r = sc.parallelize([1,2,3,4,5,6,7,8], 8)
print(r.fold(0, lambda x, y: max(x, y)))
print(r.fold(float('inf'), lambda x, y: min(x, y)))
print(r.fold(0, lambda x, y: x+y))


# COMMAND ----------

r = sc.range(20,10,step=-1)
r1 = sc.range(0,10, numSlices=2)
print(r.collect())
print(r1.getNumPartitions())

# COMMAND ----------

r = sc.parallelize([(1, "Alice"),(2, "Bob"),(3, "Charlie")])
r1 = sc.parallelize([(1, "F"), (2, "M"), (4, "M")])
r2 = r.join(r1)
r3 = r.leftOuterJoin(r1)
r4 = r.rightOuterJoin(r1)
# r5 = r.leftInnerJoin(r1)
# r6 = r.rightInnerJoin(r1)

print(r2.collect())
print(r3.collect())
print(r4.collect())
# print(r5.collect())
# print(r6.collect())

# COMMAND ----------

bd = {1: "Alice",2: "Bob",3: "Charlie"}
bv = sc.broadcast(bd)
r = sc.parallelize([1, 2, 3, 4, 5, 6])
re = r.map(lambda x: bv.value.get(x, 'null'))
re.collect()


# COMMAND ----------

r = sc.parallelize(range(0, 21))
r1 = r.sample(False, 0.2)
r1.collect()
