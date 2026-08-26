from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Exercise07").getOrCreate()
sc = spark.sparkContext

rdd = sc.parallelize([1,2,3,4,5,6])

print("Map:", rdd.map(lambda x: x * 2).collect())
print("Filter:", rdd.filter(lambda x: x % 2 == 0).collect())

pairs = sc.parallelize([
    ("A",10),
    ("B",20),
    ("A",30),
    ("B",40),
    ("C",50)
])

print("reduceByKey:", pairs.reduceByKey(lambda a,b: a+b).collect())
print("groupByKey:", pairs.groupByKey().mapValues(list).collect())

spark.stop()
