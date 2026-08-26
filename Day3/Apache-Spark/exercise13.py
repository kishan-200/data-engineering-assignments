from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Exercise13").getOrCreate()
sc = spark.sparkContext

transactions = sc.parallelize([100, -50, 200, -20, 300])

negative_count = sc.accumulator(0)

def check_negative(value):
    global negative_count
    if value < 0:
        negative_count += 1

transactions.foreach(check_negative)

print("Negative Transactions:", negative_count.value)

spark.stop()
