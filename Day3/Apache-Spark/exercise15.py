from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Exercise15").getOrCreate()
sc = spark.sparkContext

rdd8 = sc.parallelize(range(1,101), 8)

rdd3 = rdd8.coalesce(3)
rdd12 = rdd8.repartition(12)

print("Original partitions:", rdd8.getNumPartitions())
print("After coalesce:", rdd3.getNumPartitions())
print("After repartition:", rdd12.getNumPartitions())

spark.stop()
