from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from datetime import datetime, timedelta
import os
import logging
import glob

AWS_CONN_ID = "aws_default"
S3_BUCKET_NAME = "helsinki-bike-bucket"
S3_PREFIX = "data_and_metrics/"
LOCAL_DATA_DIR = "data/"


with DAG(
    dag_id = "load_file_and_metrics_to_s3",
    start_date = datetime(2025, 7, 25),
    schedule = "0 12 * * *"
) as dag:
    
    def find_matching_file(ti):
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
            
    wait_for_file_and_get_names = PythonSensor(
        task_id = "wait_for_file_and_get_names",
        python_callable = find_matching_file,
        poke_interval = 30,
        timeout= 60 * 60,
        soft_fail = True
    )
    
    def prepare_files(ti):
        filenames = ti.xcom_pull(task_ids="wait_for_file_and_get_names", key="filenames")
        return  [{"filename": f} for f in filenames] 
    
    prepare_files_to_process = PythonOperator(
        task_id = "prepare_files_to_process",
        python_callable = prepare_files
    )
    
    def process_single_csv_file(filename):
        logging.info(f"Processing file: {filename}")
        return filename
        
    process_single_file = PythonOperator.partial(
        task_id = "process_single_file",
        python_callable = process_single_csv_file
    ).expand(
        op_kwargs= prepare_files_to_process.output
    )
    
    def load_file_to_s3(filename):
        year = filename[:4]
        month = filename[5:7]
        
        s3_hook = S3Hook(aws_conn_id =AWS_CONN_ID)
        s3_key = f"{S3_PREFIX}/{year}/{month}/data_{filename}"
        
        logging.info(f"Attempting to load {filename} to s3://{S3_BUCKET_NAME}/{s3_key}")
        
        try:
            s3_hook.load_file(
                filename = os.path.join(LOCAL_DATA_DIR, filename),
                key=s3_key,
                bucket_name = S3_BUCKET_NAME,
                replace = True
            )
            logging.info("File was successfully loaded!")
        except Exception as e:
            logging.warning(f"Error wile trying to upload the file: {e}")
        
    load_to_s3 = PythonOperator.partial(
        task_id = "load_to_s3",
        python_callable = load_file_to_s3,
    ).expand(
        op_kwargs=process_single_file.output.map(lambda f: {"filename": f})
    )
    
    all_files_loaded_to_s3 = EmptyOperator(task_id = "all_files_loaded_to_s3")
    
    wait_for_file_and_get_names >> prepare_files_to_process >> process_single_file
    process_single_file >> load_to_s3 >> all_files_loaded_to_s3
    
    
    
    
    