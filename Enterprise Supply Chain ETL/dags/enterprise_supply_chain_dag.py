from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
PROJECT_DIR = "/opt/airflow/dags/enterprise_supply_chain_etl_project"
with DAG(dag_id="enterprise_supply_chain_etl", start_date=datetime(2026,1,1), schedule="@daily", catchup=False) as dag:
    profile = BashOperator(task_id="profile_sources", bash_command=f"cd {PROJECT_DIR} && python src/python_etl/00_profile_sources.py")
    etl = BashOperator(task_id="run_enterprise_etl", bash_command=f"cd {PROJECT_DIR} && python src/python_etl/01_run_enterprise_etl.py")
    profile >> etl
