from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3_bucket import S3CreateBucketOperator, S3DeleteBucketOperator
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
bucket_name="awsedrftgyhu1324"
def sample_python_function():
    s3_client = boto3.client('s3')
    s3_client.create_bucket(Bucket=bucket_name)

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
            )

    stage1 >> stage2 >> stage3
