# Day 11 – Apache Airflow Workflow Orchestration

## Overview

This project introduces Apache Airflow, an open-source platform used to author, schedule, and monitor workflows. It demonstrates how Python-based DAGs define tasks and how task dependencies control execution order.

## Objectives

- Understand Directed Acyclic Graphs (DAGs) in Apache Airflow.
- Create and execute tasks using `PythonOperator`.
- Define task dependencies and execution order.
- Build a simple ETL workflow with Extract, Transform, and Load tasks.
- Explore workflow execution, task status, and logs through the Airflow UI.
- Understand how one DAG can trigger another DAG.

## Technologies Used

- **Apache Airflow 2.8.1**
- **Python 3.11**
- **SQLite** – local Airflow metadata database
- **Ubuntu / WSL**
- **Git and GitHub**

## Project Structure

```text
Day11-Airflow/
├── README.md
└── dags/
    ├── hello_world.py
    ├── simple_etl_dag.py
    ├── helloworld1.py
    ├── helloworld2.py
    └── helloworld3.py
```

*The structure above represents the intended project layout. Only files included in the repository are part of the submitted project.*

## Workflows Implemented

### 1. Hello World DAG

A basic workflow that executes a Python function and prints a message. It introduces DAG definitions and task execution.

### 2. Simple ETL DAG

A workflow containing three tasks:

- **Extract:** Retrieves or creates sample input data.
- **Transform:** Processes the extracted data.
- **Load:** Represents the final loading step.

The dependency chain is:

`extract → transform → load`

### 3. Multi-DAG Orchestration

Three separate DAGs demonstrate how workflows can trigger other workflows:

`helloworld1 → helloworld2 → helloworld3`

When configured with `TriggerDagRunOperator`, the first workflow triggers the second, and the second triggers the third. This sequence requires all three DAGs to be available and enabled in Airflow.

## Setup and Execution

### 1. Create and activate a Python virtual environment

```bash
python3 -m venv venv
source venv/bin/activate
```

### 2. Install Apache Airflow

For a reproducible installation, use the official constraints file for the chosen Airflow version and Python version. For example, with Airflow 2.8.1 and Python 3.11:

```bash
AIRFLOW_VERSION=2.8.1
PYTHON_VERSION=3.11

pip install "apache-airflow==${AIRFLOW_VERSION}" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
```

### 3. Configure the Airflow home directory

```bash
export AIRFLOW_HOME="$HOME/airflow"
```

Place the DAG files in the configured DAG directory:

```bash
mkdir -p "$AIRFLOW_HOME/dags"
```

### 4. Start Airflow

```bash
airflow standalone
```

Open the Airflow web interface:

http://localhost:8080

Use the administrator credentials configured during startup.

### 5. Run a workflow

1. Open the Airflow web interface.
2. Locate the required DAG.
3. Enable or unpause it if necessary.
4. Trigger the DAG manually.
5. Inspect the task states and logs to verify execution.

## Key Learnings

- Creating DAGs and defining Python tasks.
- Managing dependencies between tasks.
- Understanding the Extract–Transform–Load workflow pattern.
- Triggering one DAG from another.
- Monitoring workflows through the Airflow UI.
- Organizing and version-controlling workflow code with Git.

## Notes

- Airflow's metadata database and DAG source files serve different purposes. The database stores workflow metadata and run history, while the DAG files define the workflows.
- Do not commit virtual environments, database files, passwords, API keys, or local logs.
- This project is intended for local learning and development, not production deployment.



