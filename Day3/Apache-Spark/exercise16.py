from pyspark.sql import SparkSession
from pyspark import StorageLevel

spark = SparkSession.builder.appName("Exercise16").getOrCreate()
sc = spark.sparkContext

rdd = sc.parallelize(range(1,1000001), 4)

processed = rdd.map(lambda x: x * 2).filter(lambda x: x % 3 == 0)

print("Without Cache Count:", processed.count())
print("Without Cache Sum:", processed.sum())

cached = processed.cache()
print("Cached Count:", cached.count())
print("Cached Sum:", cached.sum())
print("Is Cached:", cached.is_cached)

cached.unpersist()

persisted = processed.persist(StorageLevel.MEMORY_AND_DISK)
print("Persist Count:", persisted.count())
print("Persist Sum:", persisted.sum())
print("Storage Level:", persisted.getStorageLevel())

spark.stop()
