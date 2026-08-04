from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
P='/opt/airflow/dags/smart_grid_energy_etl_project'
with DAG('smart_grid_energy_etl',start_date=datetime(2026,1,1),schedule='@daily',catchup=False) as dag:
    profile=BashOperator(task_id='profile',bash_command=f'cd {P} && python src/python_etl/00_profile_sources.py')
    etl=BashOperator(task_id='etl',bash_command=f'cd {P} && python src/python_etl/01_run_smart_grid_etl.py')
    profile >> etl
