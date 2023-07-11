from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
import os
from airflow.hooks.S3_hook import S3Hook
import boto3

local_directory = '/home/noahchoe77/healthinsurance'
s3_bucket = 'healthinsurancepipeline'

def trigger_glue_job():
    client = boto3.client('glue', region_name='us-east-2')
    response = client.start_job_run(
        JobName='your-glue-job-name',
        Arguments={
            '--key': 'value',  # Pass any additional arguments needed by the Glue job
        }
    )
    print("Glue job triggered. Job Run ID:", response['JobRunId'])

def upload_and_trigger():
    s3_hook = S3Hook(aws_conn_id='AWS_CONN')

    for root, dirs, files in os.walk(local_directory):
        for file in files:
            if file.endswith('.csv'):
                file_path = os.path.join(root, file)
                object_key = f'{file}'
                s3_hook.load_file(file_path, object_key, s3_bucket, replace=True)
                trigger_glue_job()

default_args = {
    'start_date': datetime(2023, 7, 7),
    'catchup': False
}

dag = DAG(
    'local_to_s3_and_glue_trigger',
    default_args=default_args,
    description='Upload local file to S3 and trigger Glue job on S3 upload',
    schedule_interval='@daily'
)

start_task = DummyOperator(
    task_id='start_task',
    dag=dag
)

upload_and_trigger_task = PythonOperator(
    task_id='upload_and_trigger_task',
    python_callable=upload_and_trigger,
    dag=dag
)

start_task >> upload_and_trigger_task
