import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SmartMeterStreaming {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Smart Meter Energy Monitoring")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // ------------------------------------------------------------
    // 1. Define smart-meter event schema
    // ------------------------------------------------------------

    val schema = new StructType()
      .add("meter_id", StringType)
      .add("customer_id", StringType)
      .add("units", DoubleType)
      .add("timestamp", StringType)

    // ------------------------------------------------------------
    // 2. Read events from Kafka
    // ------------------------------------------------------------

    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "smart-meter-events")
      .option("startingOffsets", "latest")
      .load()

    // ------------------------------------------------------------
    // 3. Store raw Kafka events in HDFS
    // ------------------------------------------------------------

    val rawEvents = kafkaStream
      .selectExpr("CAST(value AS STRING) AS json")

    val rawHdfsQuery = rawEvents
      .writeStream
      .format("json")
      .outputMode("append")
      .option(
        "path",
        "hdfs://localhost:9000/smart-meter/raw"
      )
      .option(
        "checkpointLocation",
        "/tmp/smart-meter-raw-hdfs-checkpoint"
      )
      .start()

    // ------------------------------------------------------------
    // 4. Parse JSON events
    // ------------------------------------------------------------

    val events = rawEvents
      .select(
        col("json"),
        from_json(col("json"), schema).alias("data")
      )
      .select(
        col("json"),
        col("data.meter_id").alias("meter_id"),
        col("data.customer_id").alias("customer_id"),
        col("data.units").alias("units"),
        col("data.timestamp").alias("timestamp")
      )
      .withColumn(
        "event_time",
        to_timestamp(
          col("timestamp"),
          "yyyy-MM-dd HH:mm:ss"
        )
      )

    // ------------------------------------------------------------
    // 5. Validate readings
    // ------------------------------------------------------------

    val validatedEvents = events
      .withColumn(
        "validation_status",
        when(col("meter_id").isNull, "INVALID")
          .when(col("customer_id").isNull, "INVALID")
          .when(col("units").isNull, "INVALID")
          .when(col("units") < 0, "INVALID")
          .when(col("event_time").isNull, "INVALID")
          .otherwise("VALID")
      )
      .withColumn(
        "error_reason",
        when(col("meter_id").isNull, "Missing meter_id")
          .when(col("customer_id").isNull, "Missing customer_id")
          .when(col("units").isNull, "Missing or invalid units")
          .when(col("units") < 0, "Negative units")
          .when(col("event_time").isNull, "Invalid timestamp or JSON")
          .otherwise("None")
      )

    // ------------------------------------------------------------
    // 6. Keep only valid events for analytics
    // ------------------------------------------------------------

    val validEvents = validatedEvents
      .filter(col("validation_status") === "VALID")

    // ------------------------------------------------------------
    // 7. Show invalid events
    // ------------------------------------------------------------

    val invalidEvents = validatedEvents
      .filter(col("validation_status") === "INVALID")
      .select(
        col("json"),
        col("meter_id"),
        col("customer_id"),
        col("units"),
        col("timestamp"),
        col("validation_status"),
        col("error_reason")
      )

    val validationQuery = invalidEvents.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .option(
        "checkpointLocation",
        "/tmp/smart-meter-validation-checkpoint"
      )
      .start()

    // ------------------------------------------------------------
    // 8. One-minute tumbling window
    // ------------------------------------------------------------

    val windowedConsumption = validEvents
      .withWatermark("event_time", "30 seconds")
      .groupBy(
        window(col("event_time"), "1 minute"),
        col("customer_id")
      )
      .agg(
        sum("units").alias("total_units"),
        avg("units").alias("average_units"),
        max("units").alias("peak_units"),
        count("*").alias("reading_count")
      )

    // ------------------------------------------------------------
    // 9. Identify abnormal consumption
    // ------------------------------------------------------------

    val result = windowedConsumption
      .withColumn(
        "consumption_status",
        when(col("peak_units") > 20, "ABNORMAL")
          .otherwise("NORMAL")
      )

    // ------------------------------------------------------------
    // 10. Display processed result on console
    // ------------------------------------------------------------

    val consumptionQuery = result.writeStream
      .outputMode("update")
      .format("console")
      .option("truncate", "false")
      .option(
        "checkpointLocation",
        "/tmp/smart-meter-consumption-checkpoint"
      )
      .start()

    // ------------------------------------------------------------
    // 11. Store processed aggregation in HDFS
    // ------------------------------------------------------------

    val processedHdfsQuery = result
      .writeStream
      .outputMode("append")
      .format("parquet")
      .option(
        "path",
        "hdfs://localhost:9000/smart-meter/processed"
      )
      .option(
        "checkpointLocation",
        "/tmp/smart-meter-processed-hdfs-checkpoint"
      )
      .start()

    // ------------------------------------------------------------
    // 12. Wait for streaming queries
    // ------------------------------------------------------------

    spark.streams.awaitAnyTermination()
  }
}
