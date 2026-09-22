ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "smart-meter-producer",
    version := "1.0"
  )

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "3.6.1"
)
