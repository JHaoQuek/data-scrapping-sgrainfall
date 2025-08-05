from datetime import timedelta
from airflow import DAG 
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago 
from datetime import datetime 

from rainfall_etl import run_rainfall_etl 


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 8),
    'email': ['jhsosch@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

rainfall_dag = DAG(
    'rainfall_dag',
    default_args=default_args,
    description='ETL code for rainfall data',
    schedule_interval='*/5 * * * *',  # Every 5 minutes
    catchup=False
)

run_etl = PythonOperator(
    task_id='perform_rainfall_etl',
    python_callable=run_rainfall_etl,
    dag=rainfall_dag
)


run_etl 

