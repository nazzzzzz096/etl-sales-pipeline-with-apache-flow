from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from datetime import datetime, timedelta
from airflow.hooks.base import BaseHook
import json
import random
import boto3


# Default DAG arguments

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Task 1: Upload sales data to S3 

def upload_sales_to_s3():
    today = datetime.utcnow().strftime('%Y-%m-%d')
    sales_data = [{
        "order_id": f"{i}-{today}",
        "order_date": today,
        "customer": f"Customer_{i}",
        "amount": round(random.uniform(100, 1000), 2)
    } for i in range(10)]

    ndjson_string = "\n".join(json.dumps(record) for record in sales_data)

    bucket_name = 'sales-data-pipeline-buckett'
    key_name = f'raw/orders_{today}.json'

    s3_hook = S3Hook(aws_conn_id='aws_default')
    s3_hook.load_string(
        string_data=ndjson_string,
        key=key_name,
        bucket_name=bucket_name,
        replace=True
    )
    print(f"✅ Uploaded {key_name} to S3")


# Task 2: Trigger AWS Glue Job

def run_glue_job():

    conn = BaseHook.get_connection('aws_default')
    glue_client = boto3.client(
        'glue',
        region_name='us-east-1',
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password
    )

    response = glue_client.start_job_run(JobName='clean_raw_sales_data')
    print("Glue job started:", response['JobRunId'])


# Task 3: Load from S3 to Redshift

def load_to_redshift():
    redshift = PostgresHook(postgres_conn_id='redshift_conn_id')  
    conn = redshift.get_conn()
    cursor = conn.cursor()

    copy_sql = f"""
        COPY public.sales_orders
        FROM 's3://sales-data-pipeline-buckett/processed/'
        IAM_ROLE 'arn:aws:iam::151707281278:role/service-role/AmazonRedshift-CommandsAccessRole-20250415T125655'
        FORMAT AS JSON 'auto'
        TIMEFORMAT 'auto';
    """

    cursor.execute(copy_sql)
    conn.commit()
    cursor.close()
    print("✅ Redshift COPY completed")


# DAG Definition

with DAG(
    dag_id='etl_sales_pipeline',
    default_args=default_args,
    description='Automated ETL pipeline: S3 -> Glue -> Redshift',
    start_date=datetime.now(),
    schedule_interval='0 2 * * *',
    catchup=False,
    tags=['etl', 's3', 'glue', 'redshift'],
) as dag:

    task_upload = PythonOperator(
        task_id='upload_sales_to_s3',
        python_callable=upload_sales_to_s3
    )

    task_glue = PythonOperator(
        task_id='run_glue_job',
        python_callable=run_glue_job
    )

    task_redshift = PythonOperator(
        task_id='load_to_redshift',
        python_callable=load_to_redshift
    )

    task_upload >> task_glue >> task_redshift


