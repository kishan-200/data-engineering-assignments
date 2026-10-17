from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

BASE = Path.home() / "omnichannel-inventory" / "validation" / "spark_restart"
INPUT = str(BASE / "input")
OUTPUT = str(BASE / "output")
CHECKPOINT = str(BASE / "checkpoint")

schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("store_id", StringType(), True),
    StructField("sku", StringType(), True),
    StructField("quantity", IntegerType(), True),
])

spark = (
    SparkSession.builder
    .appName("Checkpoint-Restart-Test")
    .master("local[1]")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

stream = (
    spark.readStream
    .schema(schema)
    .json(INPUT)
)

query = (
    stream.writeStream
    .format("parquet")
    .option("path", OUTPUT)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()

count = spark.read.parquet(OUTPUT).count()

print()
print("=" * 58)
print("SPARK CHECKPOINT / RESTART TEST")
print("=" * 58)
print("Rows currently in output :", count)
print("Checkpoint directory      :", CHECKPOINT)
print("=" * 58)

spark.stop()
