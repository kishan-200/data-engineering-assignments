import time
from pyspark.sql import SparkSession

SALES_PATH = "data_lake/silver/sales_clean"

spark = (
    SparkSession.builder
    .appName("ReduceByKeyVsGroupByKey")
    .master("local[2]")
    .config("spark.driver.memory", "1g")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("\n===== LOADING SALES DATA =====")

sales = (
    spark.read.parquet(SALES_PATH)
    .select("sku", "quantity")
)

total_rows = sales.count()

print(f"Input sales rows: {total_rows:,}")

sales_rdd = sales.rdd.map(
    lambda row: (row["sku"], int(row["quantity"]))
)

# -------------------------------------------------
# reduceByKey
# -------------------------------------------------
print("\n===== reduceByKey BENCHMARK =====")

start = time.perf_counter()

reduce_result = (
    sales_rdd
    .reduceByKey(lambda a, b: a + b)
    .collect()
)

reduce_time = time.perf_counter() - start

print(f"SKUs produced: {len(reduce_result):,}")
print(f"reduceByKey time: {reduce_time:.2f} seconds")


# -------------------------------------------------
# groupByKey
# -------------------------------------------------
print("\n===== groupByKey BENCHMARK =====")

start = time.perf_counter()

group_result = (
    sales_rdd
    .groupByKey()
    .mapValues(lambda values: sum(values))
    .collect()
)

group_time = time.perf_counter() - start

print(f"SKUs produced: {len(group_result):,}")
print(f"groupByKey time: {group_time:.2f} seconds")


# -------------------------------------------------
# Comparison
# -------------------------------------------------
print("\n===== FINAL COMPARISON =====")

print(f"Input rows       : {total_rows:,}")
print(f"reduceByKey      : {reduce_time:.2f} sec")
print(f"groupByKey       : {group_time:.2f} sec")

if reduce_time < group_time:
    improvement = ((group_time - reduce_time) / group_time) * 100
    print(f"Winner           : reduceByKey")
    print(f"Time improvement : {improvement:.2f}%")
else:
    difference = ((reduce_time - group_time) / reduce_time) * 100
    print(f"Winner           : groupByKey on this run")
    print(f"Difference       : {difference:.2f}%")

print("""
Explanation:
reduceByKey performs local aggregation before shuffle,
so less data normally needs to move between partitions.

groupByKey moves all values belonging to a key before
performing the aggregation, requiring more shuffle data
and generally more memory.
""")

spark.stop()
