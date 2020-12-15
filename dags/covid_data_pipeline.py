from airflow import DAG
from airflow.sensors import HttpSensor, S3KeySensor
from airflow.operators import PythonOperator
from airflow.hooks import S3Hook

import scripts.config as cfg
import requests
import pandas as pd
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 1, 1),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'email': cfg.email,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


def download_data(url, headers, file_name):
    response = requests.request("GET", url, headers=headers)
    covid_data_json = response.json()['response']
    covid_data_df = pd.json_normalize(covid_data_json)
    covid_data_df.to_csv(file_name, index=False)


def upload_to_s3(file_name, key, bucket_name, conn_id):
    s3_hook = S3Hook(conn_id)
    s3_hook.load_file(file_name, key, bucket_name)


with DAG(
        dag_id='covid_data_pipeline',
        schedule_interval='@daily',
        default_args=default_args,
        catchup=False) as dag:

    is_covid_data_available = HttpSensor(
        task_id='is_covid_data_available',
        method='GET',
        http_conn_id=cfg.covid_api_conn_id,
        endpoint='latest',
        response_check=lambda response: 'response' in response.text,
        headers=cfg.covid_api_headers,
        poke_interval=1,
        timeout=5
    )

    download_covid_data = PythonOperator(
        task_id='download_covid_data',
        python_callable=download_data,
        op_args=[cfg.covid_api_url, cfg.covid_api_headers, cfg.file_name]
    )

    upload_data_to_s3 = PythonOperator(
        task_id='upload_data_to_S3',
        python_callable=upload_to_s3,
        op_args=[cfg.file_name, cfg.s3_key, cfg.covid_data_bucket, cfg.s3_conn_id]
    )

is_covid_data_available >> download_covid_data >> upload_data_to_s3
