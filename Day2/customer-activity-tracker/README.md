# Customer Activity Tracker

## Overview

A mini project demonstrating real-time customer activity tracking using Apache HBase and analytical processing using Apache Hive.

## Technologies

- Hadoop
- Apache HBase
- Apache Hive
- Apache ZooKeeper
- Ubuntu Linux

## Architecture

Web / App Client
        ↓
HBase - Real-time Data Storage
        ↓
Hive - Analytics Engine

## HBase Schema

### Table

customer_activity

### Column Family: profile

- name
- city

### Column Family: activity

- product
- type
- timestamp

## Features

1. Create HBase table
2. Insert customer profile and activity records
3. Retrieve individual customers using Point GET
4. Scan activity records
5. Filter activities by activity type
6. Delete obsolete activity records
7. Map HBase table to Hive
8. Perform analytical queries using Hive

## Activity Types

- VIEW
- CLICK
- PURCHASE

## Sample Customers

| ID | Name | City | Product | Activity |
|---|---|---|---|---|
| C001 | Rahul | Bangalore | iPhone 15 | PURCHASE |
| C002 | Priya | Hyderabad | Samsung Galaxy S24 | VIEW |
| C003 | Arjun | Mumbai | Dell Inspiron 15 | CLICK |
| C004 | Sneha | Chennai | Sony WH-1000XM5 | PURCHASE |
| C005 | Kiran | Delhi | MacBook Air | VIEW |

## Results

The HBase table was successfully mapped to Hive.

Hive queries were used to analyze:

- Activity types
- Customer cities
- Products
- Purchase activities
