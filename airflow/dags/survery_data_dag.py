# dags/bash_extract_dag.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner': 'wesonline',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    'survey_data',
    default_args=default_args,
    description='Run extract.py using BashOperator',
    start_date=datetime(2026, 3, 1),
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    start = EmptyOperator(
        task_id='start')

    end = EmptyOperator(
        task_id='end'
    )


    bronze_job = BashOperator(
        task_id='bronze_data',
        bash_command='python /opt/airflow/scripts/survey_data_pipeline.py 1'
    )
    silver_job = BashOperator(
        task_id='silver_data',
        bash_command='python /opt/airflow/scripts/survey_data_pipeline.py 2'
    )
    gold_task = BashOperator(
    task_id='gold_layer',
        bash_command='python /opt/airflow/scripts/survey_data_pipeline.py 3'
    )

    start >> bronze_job >> silver_job >> gold_task >> end