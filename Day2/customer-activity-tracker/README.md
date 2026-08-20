# Customer Activity Tracker using HBase and Hive

## 1. Project Overview

This mini project demonstrates how Apache HBase can be used for storing and retrieving customer activity data in a distributed environment, while Apache Hive can be used for analytical queries on the stored data.

The project implements customer profile storage, activity tracking, filtering, point lookups, deletion of obsolete records, and Hive-based analytics.

---

## 2. Technologies Used

* Apache Hadoop
* Apache HBase
* Apache Hive
* Apache ZooKeeper
* Java
* Ubuntu Linux

---

## 3. Architecture

```text
              Customer / Application
                       |
                       v
              +------------------+
              |      HBase       |
              | Real-time Storage|
              +------------------+
                       |
                       v
              +------------------+
              |       Hive       |
              |    Analytics     |
              +------------------+
```

HBase is used for operational access to customer activity, while Hive is used to perform analytical queries.

---

## 4. HBase Table Design

### Table Name

```text
customer_activity
```

### Column Families

#### `profile`

Stores basic customer information.

| Column | Description   |
| ------ | ------------- |
| name   | Customer name |
| city   | Customer city |

#### `activity`

Stores customer activity information.

| Column    | Description                                    |
| --------- | ---------------------------------------------- |
| product   | Product involved in the activity               |
| type      | Activity type such as VIEW, CLICK, or PURCHASE |
| timestamp | Time at which the activity occurred            |

### Example Row

```text
Row Key: C001

profile:name              Rahul
profile:city              Bangalore

activity:product          iPhone 15
activity:type             PURCHASE
activity:timestamp        2026-08-20 10:15:00
```

---

## 5. HBase Operations Implemented

The following HBase operations were implemented:

### Table Creation

```text
create 'customer_activity', 'profile', 'activity'
```

### Data Insertion

Customer profile and activity information was inserted using HBase `put` operations.

### Point GET

Individual customer records were retrieved using:

```text
get 'customer_activity', 'C001'
```

### Scanning

All customer activity records were retrieved using:

```text
scan 'customer_activity'
```

### Server-side Filtering

Activity records were filtered based on activity type.

Example:

```text
PURCHASE
```

### Time-based Filtering

Activity records were filtered using their timestamps to demonstrate time-based activity retrieval.

### Deletion

Obsolete activity columns were deleted using HBase `delete` operations.

---

## 6. Hive Integration

The HBase table was mapped to Hive using the HBase Storage Handler.

The Hive table provides the following fields:

```text
customer_id
name
city
product
activity_type
activity_timestamp
```

This allows HBase data to be queried using Hive SQL.

---

## 7. Hive Analytics

The project performs analytical queries such as:

### View all customer activity

```sql
SELECT * FROM customer_activity_hive;
```

### Count activities by type

```sql
SELECT activity_type, COUNT(*)
FROM customer_activity_hive
GROUP BY activity_type;
```

### Count activities by city

```sql
SELECT city, COUNT(*)
FROM customer_activity_hive
GROUP BY city;
```

### Find customers who purchased products

```sql
SELECT customer_id, name, city, product
FROM customer_activity_hive
WHERE activity_type = 'PURCHASE';
```

---

## 8. Project Screenshots

### 1. HBase Table Creation

Shows the `customer_activity` table and its `profile` and `activity` column families.

![HBase Table Creation](screenshots/01_hbase_table_creation.png)

### 2. HBase Data

Shows customer activity records stored in HBase.

![HBase Data](screenshots/02_hbase_data.png)

### 3. Point GET

Shows retrieval of an individual customer using the row key.

![Point GET](screenshots/03_hbase_point_get.png)

### 4. Activity Type Filtering

Shows server-side filtering of customer activities based on activity type.

![Activity Filter](screenshots/04_hbase_activity_filter.png)

### 5. Time-based Filtering

Shows retrieval of activity records based on timestamps.

![Time Filter](screenshots/05_hbase_time_filter.png)

### 6. Delete Operation

Shows deletion of obsolete customer activity records.

![Delete Operation](screenshots/06_hbase_delete.png)

### 7. Hive Analytics

Shows Hive queries and analytical results using the HBase data.

![Hive Analytics](screenshots/07_hive_analytics.png)

---

## 9. Project Files

```text
customer-activity-tracker/
│
├── data/
│   └── sample_customers.csv
│
├── hbase/
│   ├── create_table.hbase
│   ├── insert_data.hbase
│   ├── queries.hbase
│   └── delete_data.hbase
│
├── hive/
│   ├── hbase_mapping.sql
│   └── analytics.sql
│
├── screenshots/
│   ├── 01_hbase_table_creation.png
│   ├── 02_hbase_data.png
│   ├── 03_hbase_point_get.png
│   ├── 04_hbase_activity_filter.png
│   ├── 05_hbase_time_filter.png
│   ├── 06_hbase_delete.png
│   └── 07_hive_analytics.png
│
└── README.md
```

---

## 10. Conclusion

This project demonstrates an end-to-end workflow for customer activity tracking using HBase and Hive.

HBase provides efficient row-level access and filtering for customer activity data, while Hive provides a SQL-based interface for analytical processing and aggregation.

