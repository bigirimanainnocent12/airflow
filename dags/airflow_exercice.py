from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.standard.operators.empty import EmptyOperator



default_args = {
    'owner': 'innocent',
    'depends_on_past': False,      
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
                  }


with DAG(
    'tutorial_dag1',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule='@daily',
    catchup=False,
) as dag:         

    debut = EmptyOperator(
        task_id='start_pipeline'
    )

    fin = EmptyOperator(
        task_id='end_pipeline'
    )



debut >> fin


    
