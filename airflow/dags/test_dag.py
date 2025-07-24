from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

dag = DAG(
    'example_s3_dag',
    default_args=default_args,
)

def list_s3_objects():
    s3_hook = S3Hook(aws_conn_id='aws_default')
    bucket_name = 'my-bucket'
    prefix = 'path/to/files/'
    
    # List objects in the S3 bucket
    files = s3_hook.list_keys(bucket_name=bucket_name, prefix=prefix)
    print(f"Files in S3: {files}")

list_s3_task = PythonOperator(
    task_id='list_s3_objects',
    python_callable=list_s3_objects,
    dag=dag,
)