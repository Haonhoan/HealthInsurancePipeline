from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.hooks.S3_hook import S3Hook
import os

local_directory = '/home/noahchoe77/healthinsurance'
s3_bucket = 'healthinsurancepipeline'

def upload_csv_files():
    s3_hook = S3Hook(aws_conn_id='AWS_CONN')

    for root, dirs, files in os.walk(local_directory):
        for file in files:
            if file.endswith('.csv'):
                file_path = os.path.join(root, file)
                object_key = f'{file}'
                s3_hook.load_file(file_path, object_key, s3_bucket, replace=True)
                print(f"File {file_path} uploaded to S3 bucket {s3_bucket} with key {object_key}")

default_args = {
    'start_date': datetime(2023, 7, 7),
    'catchup': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'upload_csv_to_s3',
    default_args=default_args,
    description='Upload CSV files to S3',
    schedule_interval='@daily'
)

start_task = DummyOperator(
    task_id='start_task',
    dag=dag
)

upload_task = PythonOperator(
    task_id='upload_task',
    python_callable=upload_csv_files,
    dag=dag
)

start_task >> upload_task
