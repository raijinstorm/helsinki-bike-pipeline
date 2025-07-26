from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


with DAG(
    dag_id='test_spark_submit',
) as dag:

    submit_pyspark = SparkSubmitOperator(
        task_id='run_example_pyspark',
        application='/opt/airflow/dags/include/spark_script.py',  
        conn_id='spark_default',                                       
    )

    submit_pyspark
