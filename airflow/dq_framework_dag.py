from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys

sys.path.append("/opt/dq_framework/src")

from src.core.dq_runner import DQRunner


def run_dq():

    runner = DQRunner()
    runner.run()


with DAG(
    dag_id="dq_framework",
    start_date=datetime(2026, 3, 13),
    schedule="@daily",
    catchup=False
) as dag:

    dq_task = PythonOperator(
        task_id="run_dq_framework",
        python_callable=run_dq
    )