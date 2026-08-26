from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Exercise05").getOrCreate()
sc = spark.sparkContext

sentences = [
    "Spark is fast",
    "Spark is distributed",
    "Hadoop is distributed",
    "Spark is powerful"
]

rdd = sc.parallelize(sentences)

counts = (
    rdd.flatMap(lambda line: line.split())
       .map(lambda word: (word, 1))
       .reduceByKey(lambda a, b: a + b)
)

for item in sorted(counts.collect()):
    print(item)

spark.stop()
