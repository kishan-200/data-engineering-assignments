# Hospital Patient Vital Monitoring using Apache Spark

A Spark-based patient vital monitoring system developed using **Scala and Apache Spark**. The project processes patient vital signs in both **batch and streaming modes** and identifies abnormal patient conditions.

## Overview

The system monitors:

- Heart Rate
- SpO2
- Temperature
- Systolic Blood Pressure
- Diastolic Blood Pressure

It demonstrates important Apache Spark concepts including **RDDs, Pair RDDs, Spark Streaming DStreams, stateful processing, window operations, broadcast variables, accumulators, persistence, partitioning, and Spark SQL/DataFrames**.

## Technologies

- Apache Spark 3.5.3
- Scala 2.12.18
- Java 17
- SBT
- Spark Core
- Spark Streaming
- Spark SQL
- Ubuntu / WSL2

## Project Structure

```text
Hospital-Patient-Vital-Monitoring/
│
├── data/
│   ├── input/
│   ├── reference/
│   └── streaming/
│
├── screenshots/
│
├── src/main/scala/
│   ├── Models.scala
│   ├── Utils.scala
│   ├── BatchProcessor.scala
│   ├── StreamingProcessor.scala
│   ├── SqlAnalytics.scala
│   └── DagAnalysis.scala
│
├── build.sbt
├── README.md
└── .gitignore
```

## Key Features

### Batch Processing
- RDD and Pair RDD processing
- `map`, `filter`, and `flatMap`
- Patient-wise heart-rate analysis
- Vital-sign alert detection
- Invalid event counting
- RDD persistence

### Streaming Processing
- Spark Streaming DStreams
- 5-second micro-batches
- Stateful patient monitoring
- Rolling heart-rate averages
- Real-time abnormal vital alerts

### Spark Features
- Broadcast variables for normal vital ranges
- Accumulators for invalid events
- `reduceByKey` and `mapValues`
- `partitionBy`, `repartition`, and `coalesce`
- Narrow and wide transformations
- DAG and shuffle analysis

### Spark SQL
- DataFrame processing
- Patient information join
- Ward-level aggregation
- Latest patient vital analysis
- Window functions
- Risk-level UDF

## Normal Vital Ranges

| Vital | Minimum | Maximum |
|---|---:|---:|
| Heart Rate | 60 | 100 |
| SpO2 | 95 | 100 |
| Temperature | 36.0 | 37.5 |
| Systolic | 90 | 140 |
| Diastolic | 60 | 90 |

## Execution

Compile the project:

```bash
sbt compile
```

Run batch processing:

```bash
sbt "runMain BatchProcessor"
```

Run streaming processing:

```bash
sbt "runMain StreamingProcessor"
```

Run Spark SQL analysis:

```bash
sbt "runMain SqlAnalytics"
```

Run DAG analysis:

```bash
sbt "runMain DagAnalysis"
```

## Results

The project successfully demonstrates:

- Patient-wise vital monitoring
- Abnormal vital detection
- Stateful streaming alerts
- Rolling heart-rate analysis
- Invalid event handling
- RDD partition management
- Spark SQL analytics and risk classification

## Execution Evidence

Screenshots of the project execution and results are available in the [`screenshots`](screenshots/) directory.

## Repository

This project is part of the **Day6** Data Engineering assignments and demonstrates practical implementation of Apache Spark concepts using Scala.
