import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

object BatchProcessor {

  def main(args: Array[String]): Unit = {

    // ------------------------------------------------------------
    // STEP 1: Create Spark Configuration
    // ------------------------------------------------------------

    val conf = new SparkConf()
      .setAppName("Hospital Patient Vital Monitoring - Batch")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    // ------------------------------------------------------------
    // STEP 2: Define Input Paths
    // ------------------------------------------------------------

    val inputPath = "data/input/patient_vitals.csv"
    val referencePath = "data/reference/normal_ranges.csv"

    // ------------------------------------------------------------
    // STEP 3: Read Patient Vital Data
    // ------------------------------------------------------------

    val lines = sc.textFile(inputPath)

    // ------------------------------------------------------------
    // STEP 4: Remove Header
    // Narrow Transformation: filter()
    // ------------------------------------------------------------

    val data = lines
      .filter(line => !line.startsWith("patientId"))

    // ------------------------------------------------------------
    // STEP 5: Create Accumulator
    // Used to count invalid events
    // ------------------------------------------------------------

    val invalidEvents = sc.longAccumulator("Invalid Events")

    // ------------------------------------------------------------
    // STEP 6: Parse Input Records
    // Narrow Transformation: map()
    // ------------------------------------------------------------

    val parsedVitals = data.map { line =>
      Utils.parseVital(line)
    }

    // ------------------------------------------------------------
    // STEP 7: Validate Records
    // Narrow Transformation: flatMap()
    // ------------------------------------------------------------

    val vitals = parsedVitals.flatMap {

      case Some(vital) =>

        if (Utils.isValid(vital)) {
          Some(vital)
        } else {
          invalidEvents.add(1)
          None
        }

      case None =>
        invalidEvents.add(1)
        None
    }

    // ------------------------------------------------------------
    // STEP 8: Persist Valid Vital Records
    // ------------------------------------------------------------

    vitals.persist(StorageLevel.MEMORY_ONLY)

    // ------------------------------------------------------------
    // STEP 9: Display Valid Patient Vital Events
    // ------------------------------------------------------------

    println("===== PATIENT VITAL EVENTS =====")

    vitals.collect().foreach(println)

    // ------------------------------------------------------------
    // STEP 10: Count Valid Events
    // ------------------------------------------------------------

    println("\n===== TOTAL VALID EVENTS =====")

    println(vitals.count())

    // ------------------------------------------------------------
    // STEP 11: Create Pair RDD
    // Key   = patientId
    // Value = heartRate
    // ------------------------------------------------------------

    val patientHeartRates = vitals.map(vital =>
      (vital.patientId, vital.heartRate)
    )

    // ------------------------------------------------------------
    // STEP 12: Pair RDD mapValues()
    // ------------------------------------------------------------

    val heartRateTotals = patientHeartRates
      .mapValues(heartRate => (heartRate, 1))

    // ------------------------------------------------------------
    // STEP 13: Pair RDD reduceByKey()
    // Wide Transformation
    // Causes a shuffle
    // ------------------------------------------------------------

    val reducedHeartRates = heartRateTotals
      .reduceByKey {
        case ((sum1, count1), (sum2, count2)) =>
          (sum1 + sum2, count1 + count2)
      }

    // ------------------------------------------------------------
    // STEP 14: Calculate Average Heart Rate
    // ------------------------------------------------------------

    val averageHeartRate = reducedHeartRates.mapValues {
      case (total, count) =>
        total / count
    }

    // ------------------------------------------------------------
    // STEP 15: Display Average Heart Rate
    // ------------------------------------------------------------

    println("\n===== AVERAGE HEART RATE BY PATIENT =====")

    averageHeartRate
      .sortByKey()
      .collect()
      .foreach {
        case (patientId, average) =>
          println(f"$patientId -> $average%.2f")
      }

    // ------------------------------------------------------------
    // STEP 16: Read Normal Vital Ranges
    // ------------------------------------------------------------

    val rangeLines = sc.textFile(referencePath)

    val normalRanges = rangeLines
      .filter(line => !line.startsWith("vital"))
      .map { line =>
        val parts = line.split(",")

        val vital = parts(0)
        val min = parts(1).toDouble
        val max = parts(2).toDouble

        (vital, (min, max))
      }
      .collect()
      .toMap

    // ------------------------------------------------------------
    // STEP 17: Broadcast Normal Ranges
    // ------------------------------------------------------------

    val broadcastRanges = sc.broadcast(normalRanges)

    println("\n===== BROADCAST NORMAL RANGES =====")

    broadcastRanges.value
      .toSeq
      .sortBy(_._1)
      .foreach {
        case (vital, (min, max)) =>
          println(f"$vital -> $min%.1f to $max%.1f")
      }

    // ------------------------------------------------------------
    // STEP 18: Generate Vital Alerts
    // Uses Broadcast Variable
    // ------------------------------------------------------------

    val alerts = vitals.flatMap { vital =>

      val ranges = broadcastRanges.value

      var patientAlerts = List.empty[String]

      // Heart Rate

      val heartRateRange = ranges("heartRate")

      if (
        vital.heartRate < heartRateRange._1 ||
        vital.heartRate > heartRateRange._2
      ) {

        patientAlerts =
          patientAlerts :+
            s"${vital.patientId} -> ABNORMAL HEART RATE: ${vital.heartRate}"
      }

      // SpO2

      val spo2Range = ranges("spo2")

      if (
        vital.spo2 < spo2Range._1 ||
        vital.spo2 > spo2Range._2
      ) {

        patientAlerts =
          patientAlerts :+
            s"${vital.patientId} -> ABNORMAL SPO2: ${vital.spo2}"
      }

      // Temperature

      val temperatureRange = ranges("temperature")

      if (
        vital.temperature < temperatureRange._1 ||
        vital.temperature > temperatureRange._2
      ) {

        patientAlerts =
          patientAlerts :+
            s"${vital.patientId} -> ABNORMAL TEMPERATURE: ${vital.temperature}"
      }

      // Systolic Blood Pressure

      val systolicRange = ranges("systolic")

      if (
        vital.systolic < systolicRange._1 ||
        vital.systolic > systolicRange._2
      ) {

        patientAlerts =
          patientAlerts :+
            s"${vital.patientId} -> ABNORMAL SYSTOLIC BP: ${vital.systolic}"
      }

      // Diastolic Blood Pressure

      val diastolicRange = ranges("diastolic")

      if (
        vital.diastolic < diastolicRange._1 ||
        vital.diastolic > diastolicRange._2
      ) {

        patientAlerts =
          patientAlerts :+
            s"${vital.patientId} -> ABNORMAL DIASTOLIC BP: ${vital.diastolic}"
      }

      patientAlerts
    }

    // ------------------------------------------------------------
    // STEP 19: Display Vital Alerts
    // ------------------------------------------------------------

    println("\n===== VITAL ALERTS =====")

    alerts.collect().foreach(println)

    // ------------------------------------------------------------
    // STEP 20: Display Invalid Event Count
    // ------------------------------------------------------------

    println("\n===== INVALID EVENT COUNT =====")

    println(invalidEvents.value)

    // ------------------------------------------------------------
    // STEP 21: Save Batch Results
    // ------------------------------------------------------------

    println("\n===== SAVING BATCH OUTPUTS =====")

    averageHeartRate
      .map {
        case (patientId, average) =>
          s"$patientId,$average"
      }
      .coalesce(1)
      .saveAsTextFile(
        "data/output/batch/average_heart_rate"
      )

    alerts
      .coalesce(1)
      .saveAsTextFile(
        "data/output/batch/vital_alerts"
      )

    sc.parallelize(
      Seq(
        s"Invalid Events,${invalidEvents.value}"
      )
    )
      .coalesce(1)
      .saveAsTextFile(
        "data/output/batch/invalid_events"
      )

    println("Batch outputs saved successfully.")

    // ------------------------------------------------------------
    // STEP 22: Partitioning Demonstration
    // ------------------------------------------------------------

    println("\n===== PARTITIONING DEMONSTRATION =====")

    println(
      "Original vitals partitions: " +
        vitals.getNumPartitions
    )

    // ------------------------------------------------------------
    // STEP 23: partitionBy()
    // Wide Transformation
    // ------------------------------------------------------------

    val patientVitals = vitals.map(vital =>
      (vital.patientId, vital)
    )

    val partitionedVitals =
      patientVitals.partitionBy(
        new HashPartitioner(2)
      )

    println(
      "After partitionBy(2): " +
        partitionedVitals.getNumPartitions
    )

    // ------------------------------------------------------------
    // STEP 24: repartition()
    // Wide Transformation
    // Causes a shuffle
    // ------------------------------------------------------------

    val repartitionedVitals =
      vitals.repartition(4)

    println(
      "After repartition(4): " +
        repartitionedVitals.getNumPartitions
    )

    // ------------------------------------------------------------
    // STEP 25: coalesce()
    // Reduces partitions with minimal shuffle
    // ------------------------------------------------------------

    val coalescedVitals =
      repartitionedVitals.coalesce(2)

    println(
      "After coalesce(2): " +
        coalescedVitals.getNumPartitions
    )

    // ------------------------------------------------------------
    // STEP 26: Release Cached RDD
    // ------------------------------------------------------------

    vitals.unpersist()

    // ------------------------------------------------------------
    // STEP 27: Release Broadcast Variable
    // ------------------------------------------------------------

    broadcastRanges.destroy()

    // ------------------------------------------------------------
    // STEP 28: Stop Spark
    // ------------------------------------------------------------

    sc.stop()
  }
}
