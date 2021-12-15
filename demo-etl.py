from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 12, 2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def sample_python_function():
    print("Hello")

dag = DAG('demo-etl',
            max_active_runs=3,
            catchup=True,
            schedule_interval='@daily',
            default_args=default_args)
            
with dag:

    stage1 = DummyOperator(task_id='start')

    stage2 = PythonOperator( 
            task_id='python_function',
            python_callable=sample_python_function,
            dag=dag)
            
    stage3 = S3CreateBucketOperator(
            task_id='s3_bucket_dag_create',
            bucket_name="awsedrgt1234098"
            region_name='us-west-2')

        stage1 >> stage2 >> stage3
