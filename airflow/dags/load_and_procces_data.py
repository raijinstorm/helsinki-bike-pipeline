from airflow.decorators import dag, task, task_group
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from datetime import datetime, timedelta
import os
import logging
import glob

AWS_CONN_ID = "aws_default"
S3_BUCKET_NAME = "helsinki-bike-bucket"
S3_PREFIX = "data_and_metrics"
LOCAL_DATA_DIR = os.getenv("LOCAL_DATA_DIR", "/opt/airflow/data")


@dag(
    dag_id = "load_file_and_metrics_to_s3",
    start_date = datetime(2025, 7, 25),
    catchup=False,
    schedule = "0 12 * * *"
)
def load_and_procces_data_dag():
    @task.sensor( poke_interval = 30,
        timeout= 60 * 60,
        soft_fail = True
    )
    def wait_for_files_and_get_names(ti):
        files = glob.glob(os.path.join(LOCAL_DATA_DIR, "*.csv"))
        if files:
            filenames = [os.path.basename(_file) for _file in files]
            ti.xcom_push(key="filenames", value=filenames)
            for _file in filenames: 
                logging.info(f"Csv file found with filename: {_file}")
            return True
        else:
            logging.info("No csv file detected yet")
            return False
        
        
    @task
    def process_each_file(ti):
        filenames = ti.xcom_pull(task_ids="wait_for_files_and_get_names", key="filenames")
        return filenames

    
    @task_group(group_id = "process_single_file")
    def process_single_file_group(filename):
        @task
        def load_file_to_s3(file_to_load):
            year = file_to_load[:4]
            month = file_to_load[5:7]
            
            s3_hook = S3Hook(aws_conn_id =AWS_CONN_ID)
            s3_key = f"{S3_PREFIX}/{year}/{month}/data_{file_to_load}"
            
            logging.info(f"Attempting to load {file_to_load} to s3://{S3_BUCKET_NAME}/{s3_key}")
            
            try:
                s3_hook.load_file(
                    filename = os.path.join(LOCAL_DATA_DIR, file_to_load),
                    key=s3_key,
                    bucket_name = S3_BUCKET_NAME,
                    replace = True
                )
                logging.info("File was successfully loaded!")
            except Exception as e:
                logging.warning(f"Error wile trying to upload the file: {e}")
        
        calculate_metrics_with_spark = SparkSubmitOperator(
            task_id = "calculate_metrics_with_spark",
            name = "calculate_metrics",
            conn_id = "spark_default",
            application = "/opt/airflow/dags/include/spark_script.py",
            application_args=[filename]
        )
        
        @task
        def load_metric_file_to_s3(file_to_load):
            year = file_to_load[:4]
            month = file_to_load[5:7]
            
            s3_hook = S3Hook(aws_conn_id =AWS_CONN_ID)
            s3_key = f"{S3_PREFIX}/{year}/{month}/metric_{file_to_load}"
            
            logging.info(f"Attempting to load {file_to_load} to s3://{S3_BUCKET_NAME}/{s3_key}")
            
            try:
                s3_hook.load_file(
                    filename = os.path.join(os.path.join(LOCAL_DATA_DIR, "metrics"), file_to_load),
                    key=s3_key,
                    bucket_name = S3_BUCKET_NAME,
                    replace = True
                )
                logging.info("Metrics file was successfully loaded!")
            except Exception as e:
                logging.warning(f"Error wile trying to upload the file: {e}")
             
        load_file_to_s3(filename)
        calculate_metrics_with_spark >> load_metric_file_to_s3(filename)
        
    filenames_list = process_each_file()
    wait_for_files_and_get_names() >> filenames_list
    process_single_file_group.expand(filename = filenames_list)
    
    
    

load_and_procces_data_dag()