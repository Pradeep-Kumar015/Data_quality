from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

# Add your DQ project path so Airflow can import your framework
sys.path.append("/Users/206909593/DQ")

from src.core.dq_runner import DQRunner


# ---------------------------------------------------
# DEFAULT DAG SETTINGS
# ---------------------------------------------------
default_args = {

    "owner": "data_quality_team",

    "depends_on_past": False,

    "retries": 1,

    "retry_delay": timedelta(minutes=5),

    "execution_timeout": timedelta(minutes=30)
}


# ---------------------------------------------------
# MAIN EXECUTION FUNCTION
# ---------------------------------------------------
def run_dq():

    runner = DQRunner()

    tables_checked, rules_executed, pass_count, fail_count = runner.run()

    # Fail DAG if rules fail
    if fail_count > 0:

        raise Exception(
            f"DQ validation failed | Failed rules: {fail_count}"
        )


# ---------------------------------------------------
# DAG DEFINITION
# ---------------------------------------------------
with DAG(

    dag_id="dq_framework",

    default_args=default_args,

    description="Daily Data Quality Framework Execution",

    schedule="@daily",

    start_date=datetime(2026, 3, 13),  # ✅ Airflow 3 compatible

    catchup=False,

    tags=["data-quality", "snowflake"]

) as dag:


    dq_task = PythonOperator(

        task_id="run_dq_framework",

        python_callable=run_dq
    )