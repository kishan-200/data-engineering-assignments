from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Exercise12").getOrCreate()
sc = spark.sparkContext

departments = {
    101: "IT",
    102: "HR",
    103: "Finance"
}

broadcast_dept = sc.broadcast(departments)

employees = sc.parallelize([
    (1,"Kishan",101),
    (2,"Rahul",102),
    (3,"Priya",103),
    (4,"Ananya",101)
])

enriched = employees.map(
    lambda x: (
        x[0],
        x[1],
        x[2],
        broadcast_dept.value.get(x[2], "Unknown")
    )
)

for record in enriched.collect():
    print(record)

spark.stop()
