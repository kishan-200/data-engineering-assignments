from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    udf,
    sum as spark_sum,
    avg,
    broadcast
)
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Exercise20_Transaction_Project") \
    .master("local[*]") \
    .getOrCreate()

# ---------------------------------------------------
# 1. Transaction Data
# ---------------------------------------------------

transactions_data = [
    (1, 101, 5000.0, "Credit", "2026-08-26 10:00:00"),
    (2, 101, 2000.0, "Debit", "2026-08-26 10:10:00"),
    (3, 102, 7000.0, "Credit", "2026-08-26 10:20:00"),
    (4, 103, None, "Debit", "2026-08-26 10:30:00"),
    (5, 102, 3000.0, "Debit", "2026-08-26 10:40:00"),
    (6, 103, 9000.0, "Credit", "2026-08-26 10:50:00")
]

columns = [
    "transaction_id",
    "customer_id",
    "amount",
    "transaction_type",
    "timestamp"
]

df = spark.createDataFrame(transactions_data, columns)

print("Original Data:")
df.show()

# ---------------------------------------------------
# 2. Handle Null Values
# ---------------------------------------------------

df = df.fillna({"amount": 0.0})

print("After Handling Nulls:")
df.show()

# ---------------------------------------------------
# 3. Add Derived Column
# ---------------------------------------------------

df = df.withColumn(
    "amount_category",
    when(col("amount") >= 5000, "High")
    .otherwise("Low")
)

print("After Adding Derived Column:")
df.show()

# ---------------------------------------------------
# 4. UDF
# ---------------------------------------------------

def transaction_status(amount):
    if amount >= 5000:
        return "Large Transaction"
    else:
        return "Normal Transaction"

status_udf = udf(transaction_status, StringType())

df = df.withColumn(
    "transaction_status",
    status_udf(col("amount"))
)

print("After Applying UDF:")
df.show()

# ---------------------------------------------------
# 5. Aggregate By Customer
# ---------------------------------------------------

customer_summary = df.groupBy("customer_id").agg(
    spark_sum("amount").alias("total_amount"),
    avg("amount").alias("average_amount")
)

print("Customer Aggregation:")
customer_summary.show()

# ---------------------------------------------------
# 6. Window Aggregation
# ---------------------------------------------------

window_spec = Window.partitionBy("customer_id") \
    .orderBy("transaction_id") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

window_df = df.withColumn(
    "running_total",
    spark_sum("amount").over(window_spec)
)

print("Window Aggregation:")
window_df.show()

# ---------------------------------------------------
# 7. Lookup DataFrame
# ---------------------------------------------------

customer_data = [
    (101, "Kishan", "Bangalore"),
    (102, "Rahul", "Hyderabad"),
    (103, "Priya", "Chennai")
]

customer_df = spark.createDataFrame(
    customer_data,
    ["customer_id", "customer_name", "city"]
)

print("Customer Lookup:")
customer_df.show()

# ---------------------------------------------------
# 8. Broadcast Join
# ---------------------------------------------------

joined_df = window_df.join(
    broadcast(customer_df),
    on="customer_id",
    how="left"
)

print("Broadcast Join Result:")
joined_df.show()

# ---------------------------------------------------
# 9. Repartition
# ---------------------------------------------------

final_df = joined_df.repartition(3)

print("Number of Partitions:")
print(final_df.rdd.getNumPartitions())

# ---------------------------------------------------
# 10. Execution Plan
# ---------------------------------------------------

print("Execution Plan:")
final_df.explain()

# ---------------------------------------------------
# 11. Write to Parquet
# ---------------------------------------------------

final_df.write \
    .mode("overwrite") \
    .parquet("exercise20_output")

print("Output successfully written to exercise20_output")

spark.stop()
