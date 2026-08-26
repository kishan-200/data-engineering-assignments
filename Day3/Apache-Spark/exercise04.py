from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Exercise04").getOrCreate()
sc = spark.sparkContext

rdd = sc.parallelize([1,2,3,4,5,6,7,8,9,10])

filtered = rdd.filter(lambda x: x % 2 == 0)
mapped = filtered.map(lambda x: x * 10)
result = mapped.reduce(lambda a, b: a + b)

print("Original:", rdd.collect())
print("Filtered:", filtered.collect())
print("Mapped:", mapped.collect())
print("Result:", result)

spark.stop()
