from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Exercise03").getOrCreate()
sc = spark.sparkContext

rdd = sc.parallelize([10, 20, 30, 40, 50])
doubled_rdd = rdd.map(lambda x: x * 2)
total = doubled_rdd.reduce(lambda a, b: a + b)

print("Original RDD:", rdd.collect())
print("Doubled RDD:", doubled_rdd.collect())
print("Sum:", total)

spark.stop()
