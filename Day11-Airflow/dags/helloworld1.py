from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


def hello1():
    print("Hello World 1")


with DAG(
    dag_id="helloworld1",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    task1 = PythonOperator(
        task_id="hello1",
        python_callable=hello1,
    )

    trigger_hello2 = TriggerDagRunOperator(
        task_id="trigger_helloworld2",
        trigger_dag_id="helloworld2",
        wait_for_completion=True,
    )

    task1 >> trigger_hello2
