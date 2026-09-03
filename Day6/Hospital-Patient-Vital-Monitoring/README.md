# Hospital Patient Vital Monitoring using Apache Spark

## 1. Project Overview

This project implements a hospital patient vital monitoring system using Apache Spark and Scala.

The system processes patient vital signs in both batch and streaming modes and identifies abnormal patient conditions.

The monitored vital signs are:

- Heart Rate
- SpO2
- Temperature
- Systolic Blood Pressure
- Diastolic Blood Pressure

The project demonstrates Apache Spark RDD, Pair RDD, Spark Streaming DStreams, Spark SQL/DataFrames, broadcast variables, accumulators, persistence, partitioning, window operations, stateful processing, joins, SQL aggregation, SQL window functions, and UDFs.

---

## 2. Technologies Used

- Apache Spark 3.5.3
- Scala 2.12.18
- SBT
- Java 17
- Spark Core
- Spark Streaming
- Spark SQL
- Ubuntu / WSL2
- Local Spark execution

---

## 3. Project Structure

```text
hospital-patient-vital-monitoring/
│
├── build.sbt
├── README.md
├── .gitignore
│
├── data/
│   ├── input/
│   │   └── patient_vitals.csv
│   │
│   ├── reference/
│   │   ├── normal_ranges.csv
│   │   └── patient_info.csv
│   │
│   ├── streaming/
│   │   ├── source/
│   │   │   ├── batch1.csv
│   │   │   └── batch2.csv
│   │   │
│   │   ├── input/
│   │   └── checkpoint/
│   │
│   └── output/
│       ├── batch/
│       ├── sql/
│       └── streaming/
│
└── src/
    └── main/
        └── scala/
            ├── Models.scala
            ├── Utils.scala
            ├── BatchProcessor.scala
            ├── StreamingProcessor.scala
            ├── SqlAnalytics.scala
            └── DagAnalysis.scala
