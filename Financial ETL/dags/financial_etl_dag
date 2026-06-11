"""
Example Airflow DAG for the advanced financial ETL project.
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = "/opt/airflow/dags/advanced_financial_etl_project"

with DAG(
    dag_id="advanced_financial_etl_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["etl", "financial", "pyspark", "portfolio"],
) as dag:

    profile_raw_data = BashOperator(
        task_id="profile_raw_data",
        bash_command=f"cd {PROJECT_DIR} && python src/python_etl/00_profile_raw_data.py",
    )

    run_python_etl = BashOperator(
        task_id="run_python_etl",
        bash_command=f"cd {PROJECT_DIR} && python src/python_etl/01_run_financial_python_etl.py",
    )

    profile_raw_data >> run_python_etl
