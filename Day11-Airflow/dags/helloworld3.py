from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def hello3():
    print("Hello World 3")


with DAG(
    dag_id="helloworld3",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    task3 = PythonOperator(
        task_id="hello3",
        python_callable=hello3,
    )
