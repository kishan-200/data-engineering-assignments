-- Smart Meter Energy Monitoring
-- Hive database and external table

CREATE DATABASE IF NOT EXISTS smart_meter_db;

USE smart_meter_db;

CREATE EXTERNAL TABLE IF NOT EXISTS smart_meter_consumption (
    customer_id STRING,
    total_units DOUBLE,
    average_units DOUBLE,
    peak_units DOUBLE,
    reading_count BIGINT,
    consumption_status STRING
)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/smart-meter/processed';

-- Verify table
SHOW TABLES;

-- View processed smart-meter consumption
SELECT *
FROM smart_meter_consumption
LIMIT 5;

-- Example analytical query
SELECT
    customer_id,
    SUM(total_units) AS total_consumption
FROM smart_meter_consumption
GROUP BY customer_id;
