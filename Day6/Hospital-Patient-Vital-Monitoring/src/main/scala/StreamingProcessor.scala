import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel

object StreamingProcessor {

  def main(args: Array[String]): Unit = {

    // --------------------------------------------------
    // STEP 1: Create Spark Configuration
    // --------------------------------------------------

    val conf = new SparkConf()
      .setAppName("Hospital Patient Vital Monitoring - Streaming")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    // Reduce Spark log messages
    sc.setLogLevel("WARN")

    // --------------------------------------------------
    // STEP 2: Create Streaming Context
    // --------------------------------------------------
    // Each micro-batch is processed every 5 seconds.

    val ssc = new StreamingContext(sc, Seconds(5))

    // Checkpoint is required for updateStateByKey()
    ssc.checkpoint("data/streaming/checkpoint")

    // --------------------------------------------------
    // STEP 3: Create Streaming Input
    // --------------------------------------------------

    val inputPath = "data/streaming/input"

    val lines = ssc.textFileStream(inputPath)

    // --------------------------------------------------
    // STEP 4: Create Accumulator
    // --------------------------------------------------

    val invalidEvents =
      sc.longAccumulator("Streaming Invalid Events")

    // --------------------------------------------------
    // STEP 5: Read Normal Vital Ranges
    // --------------------------------------------------

    val referencePath = "data/reference/normal_ranges.csv"

    val normalRanges =
      sc.textFile(referencePath)
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

    // --------------------------------------------------
    // STEP 6: Broadcast Normal Ranges
    // --------------------------------------------------

    val broadcastRanges =
      sc.broadcast(normalRanges)

    // --------------------------------------------------
    // STEP 7: Parse Incoming Events
    // --------------------------------------------------
    // We first parse every incoming line.
    //
    // Some records become Some(PatientVital)
    // and invalid records become None.

    val parsedResults = lines.map { line =>
      Utils.parseVital(line)
    }

    // --------------------------------------------------
    // STEP 8: Count Invalid Events and Keep Valid Events
    // --------------------------------------------------
    //
    // Invalid records are counted here BEFORE they are
    // removed from the stream.
    //
    // This fixes the previous problem where "abc" was
    // already converted to None by parseVital().

    val vitals = parsedResults.flatMap {

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

    // Persist valid vital events because several
    // downstream operations use this DStream.

    vitals.persist(StorageLevel.MEMORY_ONLY)

    // --------------------------------------------------
    // STEP 9: Convert to Pair DStream
    // --------------------------------------------------

    val patientVitals = vitals.map { vital =>
      (vital.patientId, vital)
    }

    // --------------------------------------------------
    // STEP 10: Generate Vital Alerts
    // --------------------------------------------------

    val alertEvents = vitals.flatMap { vital =>

      val ranges = broadcastRanges.value

      var alerts = List.empty[String]

      // Heart Rate
      val heartRateRange = ranges("heartRate")

      if (vital.heartRate < heartRateRange._1 ||
          vital.heartRate > heartRateRange._2) {

        alerts = alerts :+
          s"${vital.patientId} -> ABNORMAL HEART RATE: ${vital.heartRate}"
      }

      // SpO2
      val spo2Range = ranges("spo2")

      if (vital.spo2 < spo2Range._1 ||
          vital.spo2 > spo2Range._2) {

        alerts = alerts :+
          s"${vital.patientId} -> ABNORMAL SPO2: ${vital.spo2}"
      }

      // Temperature
      val temperatureRange = ranges("temperature")

      if (vital.temperature < temperatureRange._1 ||
          vital.temperature > temperatureRange._2) {

        alerts = alerts :+
          s"${vital.patientId} -> ABNORMAL TEMPERATURE: ${vital.temperature}"
      }

      // Systolic Blood Pressure
      val systolicRange = ranges("systolic")

      if (vital.systolic < systolicRange._1 ||
          vital.systolic > systolicRange._2) {

        alerts = alerts :+
          s"${vital.patientId} -> ABNORMAL SYSTOLIC BP: ${vital.systolic}"
      }

      // Diastolic Blood Pressure
      val diastolicRange = ranges("diastolic")

      if (vital.diastolic < diastolicRange._1 ||
          vital.diastolic > diastolicRange._2) {

        alerts = alerts :+
          s"${vital.patientId} -> ABNORMAL DIASTOLIC BP: ${vital.diastolic}"
      }

      alerts
    }

    // --------------------------------------------------
    // STEP 11: Create Patient Status
    // --------------------------------------------------

    val patientStatus = vitals.map { vital =>

      val ranges = broadcastRanges.value

      var abnormal = false
      var reasons = List.empty[String]

      // Heart Rate
      val heartRateRange = ranges("heartRate")

      if (vital.heartRate < heartRateRange._1 ||
          vital.heartRate > heartRateRange._2) {

        abnormal = true
        reasons = reasons :+ "Heart Rate"
      }

      // SpO2
      val spo2Range = ranges("spo2")

      if (vital.spo2 < spo2Range._1 ||
          vital.spo2 > spo2Range._2) {

        abnormal = true
        reasons = reasons :+ "SpO2"
      }

      // Temperature
      val temperatureRange = ranges("temperature")

      if (vital.temperature < temperatureRange._1 ||
          vital.temperature > temperatureRange._2) {

        abnormal = true
        reasons = reasons :+ "Temperature"
      }

      // Systolic BP
      val systolicRange = ranges("systolic")

      if (vital.systolic < systolicRange._1 ||
          vital.systolic > systolicRange._2) {

        abnormal = true
        reasons = reasons :+ "Systolic BP"
      }

      // Diastolic BP
      val diastolicRange = ranges("diastolic")

      if (vital.diastolic < diastolicRange._1 ||
          vital.diastolic > diastolicRange._2) {

        abnormal = true
        reasons = reasons :+ "Diastolic BP"
      }

      (
        vital.patientId,
        (abnormal, reasons)
      )
    }

    // --------------------------------------------------
    // STEP 12: Stateful Patient Alerts
    // --------------------------------------------------
    //
    // State:
    // (number of consecutive abnormal events, last reasons)

    val statefulAlerts =
      patientStatus.updateStateByKey[(Int, List[String])] {

        (
          newValues: Seq[(Boolean, List[String])],
          previousState: Option[(Int, List[String])]
        ) => {

          val previous =
            previousState.getOrElse(
              (0, List.empty[String])
            )

          var consecutiveAbnormal = previous._1
          var lastReasons = previous._2

          newValues.foreach {

            case (abnormal, reasons) =>

              if (abnormal) {

                consecutiveAbnormal += 1
                lastReasons = reasons

              } else {

                consecutiveAbnormal = 0
                lastReasons = List.empty[String]
              }
          }

          Some(
            (
              consecutiveAbnormal,
              lastReasons
            )
          )
        }
      }

    // --------------------------------------------------
    // STEP 13: Rolling Heart Rate Average
    // --------------------------------------------------
    //
    // 15-second window with a 5-second slide.

    val heartRatePairs = vitals.map { vital =>
      (
        vital.patientId,
        (vital.heartRate, 1)
      )
    }

    val rollingHeartRate =
      heartRatePairs.reduceByKeyAndWindow(
        (a: (Double, Int), b: (Double, Int)) =>
          (
            a._1 + b._1,
            a._2 + b._2
          ),
        Seconds(15),
        Seconds(5)
      )

    val rollingAverage =
      rollingHeartRate.mapValues {
        case (sum, count) =>
          sum / count
      }

    // --------------------------------------------------
    // STEP 14: Print Incoming Micro-Batches
    // --------------------------------------------------

    vitals.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println()
        println("==============================================")
        println("        NEW MICRO-BATCH RECEIVED")
        println("==============================================")

        rdd.collect().foreach { vital =>
          println(vital)
        }

        println(
          "Invalid events so far: " +
            invalidEvents.value
        )
      }
    }

    // --------------------------------------------------
    // STEP 15: Print Vital Alerts
    // --------------------------------------------------

    alertEvents.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println()
        println("===== STREAMING VITAL ALERTS =====")

        rdd.collect().foreach(println)
      }
    }

    // --------------------------------------------------
    // STEP 16: Print Stateful Patient Alerts
    // --------------------------------------------------

    statefulAlerts.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        val alerts =
          rdd.filter {
            case (_, (consecutive, _)) =>
              consecutive >= 2
          }

        if (!alerts.isEmpty()) {

          println()
          println("===== STATEFUL PATIENT ALERTS =====")

          alerts.collect().foreach {

            case (patientId, (consecutive, reasons)) =>

              println(
                s"$patientId -> " +
                  s"$consecutive consecutive abnormal events " +
                  s"(${reasons.mkString(", ")})"
              )
          }
        }
      }
    }

    // --------------------------------------------------
    // STEP 17: Print Rolling Average
    // --------------------------------------------------

    rollingAverage.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println()
        println(
          "===== 15-SECOND ROLLING HEART RATE AVERAGE ====="
        )

        rdd.sortByKey().collect().foreach {

          case (patientId, average) =>

            println(
              f"$patientId -> $average%.2f"
            )
        }
      }
    }

    // --------------------------------------------------
    // STEP 18: Start Streaming
    // --------------------------------------------------

    println()
    println("==============================================")
    println("      HOSPITAL VITAL STREAMING STARTED")
    println("      Batch Interval: 5 seconds")
    println("      Waiting for input files...")
    println("==============================================")

    ssc.start()

    // Run for 120 seconds

    ssc.awaitTerminationOrTimeout(120000)

    // --------------------------------------------------
    // STEP 19: Stop Streaming Gracefully
    // --------------------------------------------------

    println()
    println("==============================================")
    println("      STREAMING APPLICATION STOPPING")
    println("==============================================")

    ssc.stop(
      stopSparkContext = true,
      stopGracefully = true
    )
  }
}
