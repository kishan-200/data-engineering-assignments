import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties
import scala.util.Random

object SmartMeterProducer {

  def main(args: Array[String]): Unit = {

    val topic = "smart-meter-events"

    val properties = new Properties()

    properties.put(
      "bootstrap.servers",
      "localhost:9092"
    )

    properties.put(
      "key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer"
    )

    properties.put(
      "value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer"
    )

    val producer = new KafkaProducer[String, String](properties)

    val random = new Random()

    println("======================================")
    println(" Smart Meter Kafka Producer")
    println("======================================")
    println(s"Topic: $topic")
    println("Generating smart-meter events...")
    println("Press Ctrl+C to stop")
    println()

    try {

      while (true) {

        val meterNumber = 1001 + random.nextInt(5)
        val customerNumber = 101 + (meterNumber - 1001)

        val meterId = s"M$meterNumber"
        val customerId = s"C$customerNumber"

        val units = 2.0 + random.nextDouble() * 20.0

        val timestamp =
          java.time.LocalDateTime.now()
            .format(
              java.time.format.DateTimeFormatter.ofPattern(
                "yyyy-MM-dd HH:mm:ss"
              )
            )

        val json =
          s"""{"meter_id":"$meterId","customer_id":"$customerId","units":${"%.2f".format(units)},"timestamp":"$timestamp"}"""

        val record =
          new ProducerRecord[String, String](
            topic,
            customerId,
            json
          )

        producer.send(record)

        println(s"Sent: $json")

        Thread.sleep(2000)
      }

    } finally {

      producer.close()

      println()
      println("Producer stopped.")
    }
  }
}
