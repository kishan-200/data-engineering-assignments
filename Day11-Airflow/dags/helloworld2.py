from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


def hello2():
    print("Hello World 2")


with DAG(
    dag_id="helloworld2",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    task2 = PythonOperator(
        task_id="hello2",
        python_callable=hello2,
    )

    trigger_hello3 = TriggerDagRunOperator(
        task_id="trigger_helloworld3",
        trigger_dag_id="helloworld3",
        wait_for_completion=True,
    )

    task2 >> trigger_hello3
