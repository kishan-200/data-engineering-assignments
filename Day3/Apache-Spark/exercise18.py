from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

spark = SparkSession.builder.appName("Exercise18").getOrCreate()

data = [
    (1,"Kishan","IT",60000),
    (2,"Rahul","HR",45000),
    (3,"Priya","IT",70000),
    (4,"Ananya","Finance",55000),
    (5,"Rohit","HR",52000)
]

df = spark.createDataFrame(
    data,
    ["employee_id","name","department","salary"]
)

df2 = df.withColumn("annual_salary", col("salary") * 12)
df2.show()

filtered = df2.filter(col("salary") > 50000)
filtered.show()

result = (
    df.groupBy("department")
      .agg(avg("salary").alias("average_salary"))
      .orderBy(col("average_salary").desc())
)

result.show()

result.write.mode("overwrite").parquet("exercise18_parquet")

spark.stop()
