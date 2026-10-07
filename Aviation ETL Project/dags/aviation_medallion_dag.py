from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR="/opt/airflow/dags/aviation_medallion_etl_project_v2"

with DAG("aviation_medallion_etl",start_date=datetime(2026,1,1),schedule="@daily",catchup=False,tags=["aviation","medallion","etl"]) as dag:
    profile=BashOperator(task_id="profile_sources",bash_command=f"cd {PROJECT_DIR} && python src/python_etl/00_profile_sources.py")
    medallion=BashOperator(task_id="run_medallion_etl",bash_command=f"cd {PROJECT_DIR} && python src/python_etl/01_run_medallion_etl.py")
    profile >> medallion
