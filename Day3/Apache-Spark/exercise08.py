from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Exercise08").getOrCreate()
sc = spark.sparkContext

pair_rdd = sc.parallelize([
    ("Java",10),
    ("Spark",20),
    ("Java",15),
    ("Python",25)
])

result = pair_rdd.reduceByKey(lambda a,b: a+b)

print(result.collect())

spark.stop()
