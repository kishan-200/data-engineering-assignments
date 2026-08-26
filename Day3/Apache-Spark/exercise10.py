from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Exercise10") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext

numbers = sc.parallelize([1, 2, 3, 4, 5, 6], 2)

mapped = numbers.map(lambda x: x * 2)

filtered = mapped.filter(lambda x: x > 5)

result = filtered.collect()

print("Final Result:", result)

spark.stop()
