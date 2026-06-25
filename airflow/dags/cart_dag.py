from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


default_args = {
    "owner": "bright",
    "start_date": datetime(2025, 7, 2),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="customer_carts",
    description="fecthing customers cart records",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["customers", "carts", "airflowdags"],
) as testdag:

    start = EmptyOperator(
        task_id='start')
    
    end = EmptyOperator(
        task_id='end')


    cart = BashOperator(
        task_id="Cart_data",
        bash_command="python /opt/airflow/scripts/ecom_sales_data.py cart"
    )
    cust = BashOperator(
        task_id="Customer_data",
        bash_command="python /opt/airflow/scripts/ecom_sales_data.py cust"
    )
    enrich = BashOperator(
        task_id="Enrinched_data",
        bash_command="python /opt/airflow/scripts/ecom_sales_data.py enrich"
    )
    start >> [cart, cust] >> enrich >> end
