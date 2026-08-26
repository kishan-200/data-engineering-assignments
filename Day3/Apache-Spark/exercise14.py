from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Exercise14").getOrCreate()
sc = spark.sparkContext

rdd = sc.parallelize(range(1,21), 4)

print("Default Parallelism:", sc.defaultParallelism)
print("Number of Partitions:", rdd.getNumPartitions())

for i, data in enumerate(rdd.glom().collect()):
    print("Partition", i, ":", data)

print("Count:", rdd.count())

spark.stop()
