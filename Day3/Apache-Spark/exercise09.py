from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Exercise09").getOrCreate()
sc = spark.sparkContext

numbers = sc.parallelize([10,20,30,40,50])
print("Reduce Result:", numbers.reduce(lambda a,b: a+b))

salaries = sc.parallelize([
    ("IT",50000),
    ("HR",40000),
    ("IT",60000),
    ("HR",45000),
    ("Finance",55000)
])

print("reduceByKey Result:",
      salaries.reduceByKey(lambda a,b: a+b).collect())

spark.stop()
