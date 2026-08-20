CREATE DATABASE IF NOT EXISTS customer_db;

USE customer_db;

CREATE EXTERNAL TABLE customer_activity_hive (
    customer_id STRING,
    name STRING,
    city STRING,
    product STRING,
    activity_type STRING,
    activity_timestamp STRING
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    "hbase.columns.mapping" =
    ":key,profile:name,profile:city,activity:product,activity:type,activity:timestamp"
)
TBLPROPERTIES (
    "hbase.table.name" = "customer_activity"
);
