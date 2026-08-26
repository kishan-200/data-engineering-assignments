from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Exercise11_DAG") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext

lines = sc.textFile("input.txt")

words = lines.flatMap(lambda line: line.split())

filtered = words.filter(lambda word: len(word) > 2)

pairs = filtered.map(lambda word: (word, 1))

counts = pairs.reduceByKey(lambda a, b: a + b)

print("DAG / Lineage:")
print(counts.toDebugString().decode())

counts.saveAsTextFile("exercise11_output")

spark.stop()
