from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

spark = SparkSession.builder.appName("Exercise19").getOrCreate()

employees = [
    (1,"Kishan",101),
    (2,"Rahul",102),
    (3,"Priya",103),
    (4,"Ananya",101),
    (5,"Rohit",104)
]

departments = [
    (101,"IT"),
    (102,"HR"),
    (103,"Finance")
]

employees_df = spark.createDataFrame(
    employees,
    ["employee_id","name","department_id"]
)

departments_df = spark.createDataFrame(
    departments,
    ["department_id","department_name"]
)

print("INNER JOIN")
employees_df.join(
    departments_df,
    "department_id",
    "inner"
).show()

print("LEFT JOIN")
employees_df.join(
    departments_df,
    "department_id",
    "left"
).show()

print("BROADCAST JOIN")
broadcast_df = employees_df.join(
    broadcast(departments_df),
    "department_id",
    "inner"
)

broadcast_df.show()
broadcast_df.explain()

spark.stop()
