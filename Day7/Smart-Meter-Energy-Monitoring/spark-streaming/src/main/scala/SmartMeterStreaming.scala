import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.Trigger

object SmartMeterStreaming {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Smart Meter Energy Monitoring")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("==============================================")
    println(" SMART METER ENERGY MONITORING")
    println("==============================================")
    println("Kafka Topic: smart-meter-events")
    println("Window: 1 minute")
    println("Watermark: 30 seconds")
    println("==============================================")

    // 1. Smart Meter Event Schema

    val schema = new StructType()
      .add("meter_id", StringType)
      .add("customer_id", StringType)
      .add("units", DoubleType)
      .add("timestamp", StringType)

    // 2. Read Kafka Stream

    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "smart-meter-events")
      .option("startingOffsets", "latest")
      .load()

    // 3. Convert Kafka value to JSON

    val rawEvents = kafkaStream
      .selectExpr("CAST(value AS STRING) AS json")

    // 4. Parse JSON

    val parsedEvents = rawEvents
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

    // 5. Validate Events

    val validatedEvents = parsedEvents
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
          .when(
            col("event_time").isNull,
            "Invalid timestamp or JSON"
          )
          .otherwise("None")
      )

    // 6. Invalid Event Stream

    val invalidEvents = validatedEvents
      .filter(
        col("validation_status") === "INVALID"
      )
      .select(
        "json",
        "meter_id",
        "customer_id",
        "units",
        "timestamp",
        "validation_status",
        "error_reason"
      )

    val validationQuery = invalidEvents
      .writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", "false")
      .option(
        "checkpointLocation",
        "/tmp/smart-meter-validation-checkpoint-v2"
      )
      .trigger(
        Trigger.ProcessingTime("10 seconds")
      )
      .start()

    // 7. Valid Events

    val validEvents = validatedEvents
      .filter(
        col("validation_status") === "VALID"
      )

    // 8. One-Minute Tumbling Window

    val windowedConsumption = validEvents
      .withWatermark(
        "event_time",
        "30 seconds"
      )
      .groupBy(
        window(
          col("event_time"),
          "1 minute"
        ),
        col("customer_id")
      )
      .agg(
        sum("units").alias("total_units"),
        avg("units").alias("average_units"),
        max("units").alias("peak_units"),
        count("*").alias("reading_count")
      )

    // 9. Abnormal Consumption Detection

    val result = windowedConsumption
      .withColumn(
        "consumption_status",
        when(
          col("peak_units") > 20,
          "ABNORMAL"
        ).otherwise("NORMAL")
      )
      .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("customer_id"),
        col("total_units"),
        col("average_units"),
        col("peak_units"),
        col("reading_count"),
        col("consumption_status")
      )

    // 10. Display Streaming Results

    val consumptionQuery = result
      .writeStream
      .format("console")
      .outputMode("update")
      .option("truncate", "false")
      .option(
        "checkpointLocation",
        "/tmp/smart-meter-consumption-checkpoint-v2"
      )
      .trigger(
        Trigger.ProcessingTime("10 seconds")
      )
      .start()

    // 11. Save Results to HDFS

    val hdfsQuery = result
      .writeStream
      .outputMode("update")
      .option(
        "checkpointLocation",
        "/tmp/smart-meter-hdfs-checkpoint-v2"
      )
      .trigger(
        Trigger.ProcessingTime("10 seconds")
      )
      .foreachBatch(
        (batchDF: Dataset[Row], batchId: Long) => {

          if (!batchDF.isEmpty) {

            println(
              s"Writing batch $batchId to HDFS..."
            )

            batchDF
              .write
              .mode("append")
              .format("parquet")
              .save(
                "hdfs://localhost:9000/smart-meter/processed"
              )

            println(
              s"Batch $batchId written to HDFS."
            )
          }
        }
      )
      .start()

    // 12. Keep Streaming Application Running

    spark.streams.awaitAnyTermination()
  }
}
