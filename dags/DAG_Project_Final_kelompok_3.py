from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

SCRIPTS_DIR = "/workspace/scripts/project3"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="dc_electricity_olap_pipeline",
    default_args=default_args,
    description="Pipeline ETL Data Center & Listrik Ember (Extract -> Transform -> Validate -> Load)",
    schedule="@weekly",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    tags=["etl", "kaggle", "pyspark", "great_expectations", "postgres"],
) as dag:

    # Extract
    extract_task = BashOperator(
        task_id="extract_from_kaggle",
        bash_command=f"python3 {SCRIPTS_DIR}/extract.py",
    )

    # Transform
    transform_task = BashOperator(
        task_id="transform_to_olap",
        bash_command=f"python3 {SCRIPTS_DIR}/transform.py",
    )

    # Validate
    validate_task = BashOperator(
        task_id="validate_with_ge",
        bash_command=f"python3 {SCRIPTS_DIR}/validate.py",
    )

    # Load
    load_task = BashOperator(
        task_id="load_to_postgres",
        bash_command=f"python3 {SCRIPTS_DIR}/load.py",
    )

    extract_task >> transform_task >> validate_task >> load_task