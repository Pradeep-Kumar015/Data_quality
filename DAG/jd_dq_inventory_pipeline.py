from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone
from datetime import timedelta

try:
    from jd_dq_email_task import send_dq_failure_email
except Exception:
    try:
        from DAG.jd_dq_email_task import send_dq_failure_email
    except Exception:
        import sys, pathlib
        sys.path.append(str(pathlib.Path(__file__).resolve().parent))
        from jd_dq_email_task import send_dq_failure_email


default_args = {
    "owner": "dq_team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}


with DAG(
    dag_id="dq_inventory_pipeline",
    default_args=default_args,
    schedule="@daily",
    start_date=timezone.utcnow() - timedelta(days=1),
    catchup=False,
    tags=["DQ", "Snowflake"]
) as dag:


    # -------------------------------------
    # Task 1: Run Snowflake Procedure
    # -------------------------------------

    run_dq = SnowflakeOperator(
        task_id="run_dq_inventory",
        snowflake_conn_id="snowflake_default",
        sql="CALL DQ.DATA_QUALITY.RUN_DQ_FOR_INVENTORY_without_email();",
        autocommit=True
    )


    # -------------------------------------
    # Task 2: Send Email If Failures Exist
    # -------------------------------------

    send_email_task = PythonOperator(
        task_id="send_dq_failure_email",
        python_callable=send_dq_failure_email
    )


    run_dq >> send_email_task