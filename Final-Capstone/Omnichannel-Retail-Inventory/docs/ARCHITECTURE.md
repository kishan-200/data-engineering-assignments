\# System Architecture



\## Project Overview



The Omnichannel Retail Inventory and Auto-Replenishment Platform is a data engineering system designed for near-real-time inventory tracking, demand forecasting, automated replenishment, and business analytics.



\## 1. Data Sources



The platform integrates data from:



\- MySQL retail databases

\- Supplier CSV files

\- REST APIs

\- Point-of-sale (POS) events

\- E-commerce transactions



\## 2. Data Ingestion



\- Apache Sqoop handles batch ingestion from MySQL.

\- Python scripts process supplier and API data.

\- Apache Kafka receives real-time POS events.



\## 3. Data Storage



The platform uses:



\- HDFS Bronze layer for raw data.

\- Silver layer for cleaned and validated data.

\- Gold layer for business-ready datasets.

\- HBase for current inventory lookups.

\- Hive for analytical queries.



\## 4. Data Processing



Apache Spark and PySpark perform:



\- Data cleaning

\- Data transformation

\- Aggregation

\- Inventory calculations

\- Streaming event processing



\## 5. Streaming Architecture



POS Events → Kafka → Spark Structured Streaming → Inventory Updates → HBase



Spark Structured Streaming processes incoming POS events and updates store-SKU inventory information.



\## 6. Demand Forecasting



The forecasting component uses Spark MLlib.



Model: RandomForestRegressor



Reported MAPE: 16.71%



\## 7. Automated Replenishment



Replenishment calculations consider:



\- Current inventory

\- Forecast demand

\- Reorder point

\- Maximum stock level

\- Supplier information



The platform generates draft purchase orders for review.



\## 8. Workflow Orchestration



Apache Airflow coordinates scheduled ingestion, transformation, forecasting, and replenishment workflows.



\## 9. Dashboard and AI Assistant



\- React provides the frontend dashboard.

\- FastAPI provides backend endpoints.

\- The GenAI assistant supports inventory-related questions.



\## 10. Cloud Technologies



The project includes AWS S3, AWS Glue, AWS EMR, and Databricks in its technology stack.



\## End-to-End Data Flow



MySQL / Supplier Files / APIs / POS Events



↓



Sqoop / Python / Kafka



↓



HDFS Bronze Layer



↓



Spark Processing



↓



Silver Layer



↓



Gold Layer



↓



HBase / Hive



↓



Forecasting and Replenishment



↓



FastAPI



↓



React Dashboard / GenAI Assistant



