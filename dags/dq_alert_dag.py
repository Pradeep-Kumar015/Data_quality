from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Paths based on your setup
VENV_PYTHON = "/Users/206908417/Documents/Dq-Airflow-Orchestration/airflow-venv/bin/python"
SCRIPT_PATH = "/Users/206908417/Documents/Dq-Airflow-Orchestration/Data_quality/email_alert/email_alert.py"

default_args = {
    'owner': 'Priya',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dq_alert_system',
    default_args=default_args,
    description='Triggers Data Quality check and sends Email Alerts',
    schedule_interval=None, 
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['finance', 'dq'],
) as dag:

    run_dq_report = BashOperator(
        task_id='execute_dq_script',
        bash_command=f'{VENV_PYTHON} {SCRIPT_PATH}',
    )