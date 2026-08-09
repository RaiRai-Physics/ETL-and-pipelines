from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR="/opt/airflow/dags/walmart_retail_enterprise_etl_v2"

with DAG("walmart_style_retail_etl",start_date=datetime(2026,1,1),schedule="@daily",catchup=False,tags=["retail","etl"]) as dag:
    profile=BashOperator(task_id="profile_sources",bash_command=f"cd {PROJECT_DIR} && python src/python_etl/00_profile_sources.py")
    etl=BashOperator(task_id="run_retail_etl",bash_command=f"cd {PROJECT_DIR} && python src/python_etl/01_run_retail_etl.py")
    profile >> etl
