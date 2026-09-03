import org.apache.spark.{SparkConf, SparkContext}

object DagAnalysis {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Hospital Patient Vital Monitoring - DAG Analysis")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    // ------------------------------------------------------------
    // Create base RDD
    // ------------------------------------------------------------

    val lines = sc.textFile("data/input/patient_vitals.csv")

    // ------------------------------------------------------------
    // Narrow transformations
    // filter() and map()
    // ------------------------------------------------------------

    val data = lines
      .filter(line => !line.startsWith("patientId"))
      .map(line => line.split(","))

    // ------------------------------------------------------------
    // Another narrow transformation
    // flatMap()
    // ------------------------------------------------------------

    val patientHeartRates = data.flatMap { parts =>

      if (parts.length == 7) {
        try {
          Some((parts(0), parts(2).toDouble))
        } catch {
          case _: Exception => None
        }
      } else {
        None
      }
    }

    // ------------------------------------------------------------
    // Wide transformation
    // reduceByKey() causes a shuffle
    // ------------------------------------------------------------

    val totalHeartRates = patientHeartRates
      .reduceByKey(_ + _)

    // ------------------------------------------------------------
    // Wide transformation
    // sortByKey() causes a shuffle
    // ------------------------------------------------------------

    val sortedHeartRates = totalHeartRates.sortByKey()

    // ------------------------------------------------------------
    // Print partition information
    // ------------------------------------------------------------

    println("\n===== DAG / PARTITION ANALYSIS =====")

    println(
      "Input partitions: " +
        lines.getNumPartitions
    )

    println(
      "After filter + map partitions: " +
        data.getNumPartitions
    )

    println(
      "After flatMap partitions: " +
        patientHeartRates.getNumPartitions
    )

    println(
      "After reduceByKey partitions: " +
        totalHeartRates.getNumPartitions
    )

    println(
      "After sortByKey partitions: " +
        sortedHeartRates.getNumPartitions
    )

    // ------------------------------------------------------------
    // Print RDD lineage
    // ------------------------------------------------------------

    println("\n===== RDD LINEAGE / DAG =====")

    println(
      sortedHeartRates.toDebugString
    )

    // ------------------------------------------------------------
    // Execute an action
    // ------------------------------------------------------------

    println("\n===== FINAL RESULT =====")

    sortedHeartRates.collect().foreach {
      case (patientId, totalHeartRate) =>
        println(
          s"$patientId -> Total Heart Rate: $totalHeartRate"
        )
    }

    sc.stop()
  }
}
