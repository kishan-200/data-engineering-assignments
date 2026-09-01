import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object LogAnalysis {

  def main(args: Array[String]): Unit = {

    // ---------------------------------------------
    // 1. Create Spark Session
    // ---------------------------------------------

    val spark = SparkSession.builder()
      .appName("Spark Log Analysis")
      .master("local[*]")
      .getOrCreate()

    println("\n========== SPARK STARTED ==========\n")


    // ---------------------------------------------
    // 2. Read log file
    // ---------------------------------------------

    val df = spark.read
      .option("header", "false")
      .option("inferSchema", "true")
      .csv("data/application.log")


    // ---------------------------------------------
    // 3. Rename columns
    // ---------------------------------------------

    val logs = df.toDF(
      "timestamp",
      "level",
      "ip",
      "url",
      "status",
      "response_time"
    )


    // ---------------------------------------------
    // 4. Convert timestamp
    // ---------------------------------------------

    val cleanLogs = logs.withColumn(
      "timestamp",
      to_timestamp(
        col("timestamp"),
        "yyyy-MM-dd HH:mm:ss"
      )
    )


    // ---------------------------------------------
    // 5. Display data
    // ---------------------------------------------

    println("========== LOG DATA ==========")

    cleanLogs.show(
      truncate = false
    )


    // ---------------------------------------------
    // 6. Display schema
    // ---------------------------------------------

    println("========== SCHEMA ==========")

    cleanLogs.printSchema()


    // ---------------------------------------------
    // 7. Total requests
    // ---------------------------------------------

    val totalRequests = cleanLogs.count()

    println(
      s"Total Requests = $totalRequests"
    )


    // ---------------------------------------------
    // 8. Total errors
    // ---------------------------------------------

    val totalErrors = cleanLogs
      .filter(
        col("level") === "ERROR"
      )
      .count()

    println(
      s"Total Errors = $totalErrors"
    )


    // ---------------------------------------------
    // 9. Successful requests
    // ---------------------------------------------

    val successfulRequests = cleanLogs
      .filter(
        col("status") === 200
      )
      .count()

    println(
      s"Successful Requests = $successfulRequests"
    )


    // ---------------------------------------------
    // 10. Most accessed URLs
    // ---------------------------------------------

    println(
      "\n========== MOST ACCESSED URLs =========="
    )

    val urlCount = cleanLogs
      .groupBy("url")
      .count()
      .orderBy(
        desc("count")
      )

    urlCount.show()


    // ---------------------------------------------
    // 11. Most active IP addresses
    // ---------------------------------------------

    println(
      "\n========== MOST ACTIVE IPs =========="
    )

    val ipCount = cleanLogs
      .groupBy("ip")
      .count()
      .orderBy(
        desc("count")
      )

    ipCount.show()


    // ---------------------------------------------
    // 12. Average response time
    // ---------------------------------------------

    println(
      "\n========== AVERAGE RESPONSE TIME =========="
    )

    cleanLogs
      .select(
        avg("response_time")
          .alias("average_response_time")
      )
      .show()


    // ---------------------------------------------
    // 13. Slow requests
    // ---------------------------------------------

    println(
      "\n========== SLOW REQUESTS =========="
    )

    val slowRequests = cleanLogs
      .filter(
        col("response_time") > 400
      )

    slowRequests.show(
      truncate = false
    )


    // ---------------------------------------------
    // 14. URL performance
    // ---------------------------------------------

    println(
      "\n========== URL PERFORMANCE =========="
    )

    val urlPerformance = cleanLogs
      .groupBy("url")
      .agg(
        avg("response_time")
          .alias("avg_response_time")
      )
      .orderBy(
        desc("avg_response_time")
      )

    urlPerformance.show()


    // ---------------------------------------------
    // 15. Errors by URL
    // ---------------------------------------------

    println(
      "\n========== ERRORS BY URL =========="
    )

    val errorsByUrl = cleanLogs
      .filter(
        col("level") === "ERROR"
      )
      .groupBy("url")
      .count()
      .orderBy(
        desc("count")
      )

    errorsByUrl.show()


    // ---------------------------------------------
    // 16. HTTP status analysis
    // ---------------------------------------------

    println(
      "\n========== HTTP STATUS ANALYSIS =========="
    )

    val statusAnalysis = cleanLogs
      .groupBy("status")
      .count()
      .orderBy("status")

    statusAnalysis.show()


    // ---------------------------------------------
    // 17. Error percentage
    // ---------------------------------------------

    println(
      "\n========== ERROR PERCENTAGE =========="
    )

    val summary = cleanLogs
      .agg(
        count("*")
          .alias("total_requests"),

        count(
          when(
            col("level") === "ERROR",
            true
          )
        ).alias("total_errors")
      )
      .withColumn(
        "error_percentage",
        col("total_errors") /
          col("total_requests") * 100
      )

    summary.show()


    // ---------------------------------------------
    // 18. Stop Spark
    // ---------------------------------------------

    spark.stop()

    println(
      "\n========== SPARK FINISHED ==========\n"
    )
  }
}
