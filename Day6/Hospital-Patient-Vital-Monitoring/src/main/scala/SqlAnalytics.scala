import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._

object SqlAnalytics {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Hospital Patient Vital Monitoring - SQL Analytics")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    // ------------------------------------------------------------
    // 1. Define schemas
    // ------------------------------------------------------------

    val vitalSchema = StructType(
      Seq(
        StructField("patientId", StringType, nullable = false),
        StructField("timestamp", StringType, nullable = false),
        StructField("heartRate", DoubleType, nullable = false),
        StructField("spo2", DoubleType, nullable = false),
        StructField("temperature", DoubleType, nullable = false),
        StructField("systolic", DoubleType, nullable = false),
        StructField("diastolic", DoubleType, nullable = false)
      )
    )

    val patientSchema = StructType(
      Seq(
        StructField("patientId", StringType, nullable = false),
        StructField("name", StringType, nullable = false),
        StructField("ward", StringType, nullable = false),
        StructField("age", IntegerType, nullable = false)
      )
    )

    // ------------------------------------------------------------
    // 2. Read patient vital data
    // ------------------------------------------------------------

    val vitals = spark.read
      .option("header", "true")
      .schema(vitalSchema)
      .csv("data/input/patient_vitals.csv")

    // ------------------------------------------------------------
    // 3. Read patient information
    // ------------------------------------------------------------

    val patients = spark.read
      .option("header", "true")
      .schema(patientSchema)
      .csv("data/reference/patient_info.csv")

    println("\n===== PATIENT VITAL DATA =====")

    vitals.show(false)

    println("\n===== PATIENT INFORMATION =====")

    patients.show(false)

    // ------------------------------------------------------------
    // 4. Filter valid vital records
    // ------------------------------------------------------------

    val validVitals = vitals.filter(
      col("heartRate") > 0 &&
      col("spo2") > 0 &&
      col("temperature") > 0 &&
      col("systolic") > 0 &&
      col("diastolic") > 0
    )

    println("\n===== VALID VITAL EVENTS =====")

    validVitals.show(false)

    // ------------------------------------------------------------
    // 5. JOIN patient information with vital records
    // ------------------------------------------------------------

    val patientVitals = validVitals
      .join(
        patients,
        validVitals("patientId") === patients("patientId"),
        "inner"
      )
      .select(
        validVitals("patientId"),
        patients("name"),
        patients("ward"),
        patients("age"),
        validVitals("timestamp"),
        validVitals("heartRate"),
        validVitals("spo2"),
        validVitals("temperature"),
        validVitals("systolic"),
        validVitals("diastolic")
      )

    println("\n===== PATIENT + VITAL JOIN RESULT =====")

    patientVitals.show(false)

    // ------------------------------------------------------------
    // 6. SQL aggregation
    // ------------------------------------------------------------

    patientVitals.createOrReplaceTempView("patient_vitals")

    val wardStatistics = spark.sql(
      """
        SELECT
          ward,
          COUNT(*) AS total_events,
          ROUND(AVG(heartRate), 2) AS average_heart_rate,
          ROUND(AVG(spo2), 2) AS average_spo2,
          ROUND(MAX(temperature), 2) AS maximum_temperature
        FROM patient_vitals
        GROUP BY ward
        ORDER BY ward
      """
    )

    println("\n===== WARD-WISE SQL AGGREGATION =====")

    wardStatistics.show(false)

    // ------------------------------------------------------------
    // 7. SQL window function
    //    Find the latest vital event for each patient
    // ------------------------------------------------------------

    val latestWindow = Window
      .partitionBy("patientId")
      .orderBy(col("timestamp").desc)

    val latestVitals = patientVitals
      .withColumn(
        "row_number",
        row_number().over(latestWindow)
      )
      .filter(col("row_number") === 1)
      .drop("row_number")

    println("\n===== LATEST VITAL EVENT PER PATIENT =====")

    latestVitals.show(false)

    // ------------------------------------------------------------
    // 8. Register a UDF
    // ------------------------------------------------------------

    val riskLevel = udf(
      (
        heartRate: Double,
        spo2: Double,
        temperature: Double,
        systolic: Double,
        diastolic: Double
      ) => {

        val critical =
          heartRate > 120 ||
          spo2 < 90 ||
          temperature > 39.0 ||
          systolic > 160 ||
          diastolic > 100

        val warning =
          heartRate < 60 ||
          heartRate > 100 ||
          spo2 < 95 ||
          temperature < 36.0 ||
          temperature > 37.5 ||
          systolic < 90 ||
          systolic > 140 ||
          diastolic < 60 ||
          diastolic > 90

        if (critical) {
          "CRITICAL"
        } else if (warning) {
          "WARNING"
        } else {
          "NORMAL"
        }
      }
    )

    spark.udf.register("risk_level", riskLevel)

    // ------------------------------------------------------------
    // 9. Apply UDF using Spark SQL
    // ------------------------------------------------------------

    val riskAnalysis = spark.sql(
      """
        SELECT
          patientId,
          name,
          ward,
          timestamp,
          heartRate,
          spo2,
          temperature,
          systolic,
          diastolic,
          risk_level(
            heartRate,
            spo2,
            temperature,
            systolic,
            diastolic
          ) AS risk_level
        FROM patient_vitals
        ORDER BY patientId, timestamp
      """
    )

    println("\n===== UDF RISK CLASSIFICATION =====")

    riskAnalysis.show(false)

    // ------------------------------------------------------------
    // 10. Count patients by risk level
    // ------------------------------------------------------------

    val riskSummary = riskAnalysis
      .groupBy("risk_level")
      .agg(
        count("*").alias("event_count")
      )
      .orderBy("risk_level")

    println("\n===== RISK LEVEL SUMMARY =====")

    riskSummary.show(false)

    // ------------------------------------------------------------
    // 11. Save important SQL outputs
    // ------------------------------------------------------------

    wardStatistics.write
      .mode("overwrite")
      .option("header", "true")
      .csv("data/output/sql/ward_statistics")

    latestVitals.write
      .mode("overwrite")
      .option("header", "true")
      .csv("data/output/sql/latest_vitals")

    riskAnalysis.write
      .mode("overwrite")
      .option("header", "true")
      .csv("data/output/sql/risk_analysis")

    riskSummary.write
      .mode("overwrite")
      .option("header", "true")
      .csv("data/output/sql/risk_summary")

    // ------------------------------------------------------------
    // 12. Stop Spark
    // ------------------------------------------------------------

    spark.stop()
  }
}
