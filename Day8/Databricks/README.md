# Databricks Data Engineering Labs

A hands-on collection of Databricks and data engineering labs covering SQL analytics, Delta Lake, Auto Loader, Medallion Architecture, Delta Live Tables, and Job Orchestration.

## Overview

This section of the repository contains the Databricks work completed as part of the Data Engineering learning and hands-on practice.

The labs focus on building practical understanding of modern data engineering workflows using Databricks, Apache Spark, Delta Lake, Unity Catalog, and related Databricks capabilities.

## Learning Objectives

Through these labs, the following concepts were practiced:

- Databricks SQL
- SQL DDL and DML operations
- Advanced SQL
- Delta Lake
- Schema Evolution
- Delta Time Travel and Cloning
- Delta Lake Optimization
- Auto Loader
- Bronze-to-Silver data processing
- Silver-to-Gold transformations
- SCD Type 2 processing
- Medallion Architecture
- Delta Live Tables (DLT)
- Materialized Views
- Databricks Jobs and Workflow Orchestration
- Unity Catalog permissions
- Serverless compute

## Lab Structure

| Lab | Topic |
|---|---|
| 01 | SQL DDL & DML |
| 02 | Advanced SQL & AI |
| 03 | Delta Schema Evolution |
| 04 | Delta Time Travel & Cloning |
| 05 | Delta Optimization |
| 06 | Auto Loader |
| 07 | Bronze to Silver |
| 08 | Silver to Gold – SCD Type 2 |
| 09 | DLT Medallion Pipeline |
| 10 | Jobs Orchestration |

## Architecture

The labs progressively demonstrate a modern data engineering workflow:

```text
                    Data Sources
                         |
                         v
                  Databricks Ingestion
                         |
                         v
                   Bronze Layer
                         |
                         v
                   Silver Layer
                         |
                         v
                    Gold Layer
                         |
                         v
                  Analytics / SQL