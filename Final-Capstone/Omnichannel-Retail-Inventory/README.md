# Omnichannel Retail Inventory & Auto-Replenishment Platform

A data engineering capstone project for near-real-time inventory tracking, demand forecasting, automated replenishment, analytics, and GenAI-assisted inventory operations.

## Overview

This project simulates an omnichannel retailer with:

- 200 physical stores + e-commerce
- 5,000 SKUs
- 100 suppliers
- 5 million historical sales records
- 471,000 inventory records

The platform combines batch ingestion, real-time streaming, distributed processing, forecasting, orchestration, cloud services, dashboards, and GenAI.

## Technology Stack

| Area | Technologies |
|---|---|
| Database | MySQL |
| Ingestion | Apache Sqoop, Kafka |
| Storage | HDFS, HBase |
| Processing | Apache Spark, PySpark |
| Streaming | Spark Structured Streaming |
| Analytics | Hive, Databricks |
| Forecasting | Spark MLlib |
| Orchestration | Apache Airflow |
| Cloud | AWS S3, Glue, EMR |
| Visualization | Power BI |
| GenAI | OpenAI API |
| Containerization | Docker |

## Key Features

- MySQL to HDFS batch ingestion
- Real-time POS streaming through Kafka
- Stateful inventory updates using Spark Structured Streaming
- Low-latency stock lookup using HBase
- Bronze, Silver, and Gold data processing
- Demand forecasting using Random Forest
- Automated replenishment recommendations
- Draft purchase order generation
- Airflow workflow orchestration
- AWS and Databricks integration
- Power BI dashboards
- GenAI stock lookup and draft PO tools

## Key Results

| Metric | Result |
|---|---:|
| Forecast MAPE | 16.71% |
| Streaming Latency | 29.499 sec |
| POS Retries Removed | 1,000 / 1,000 |
| Replenishment Recommendations | 26,545 |
| Draft Purchase Orders | 14,398 |
| Kafka Failure Test | 10 / 10 messages retained |

## Repository Structure

```text
Omnichannel-Retail-Inventory/
├── airflow/
├── analytics/
├── aws_emr/
├── data_generator/
├── genai/
├── infrastructure/
├── ingestion/
├── integrations/
├── spark_jobs/
├── sql/
├── streaming/
├── validation/
├── requirements.txt
└── README.md
