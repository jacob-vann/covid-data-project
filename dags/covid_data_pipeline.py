from airflow import DAG
from airflow.sensors import HttpSensor, S3KeySensor
from airflow.operators import PythonOperator
from airflow.hooks import S3Hook

import conf.config as cfg
import requests
import pandas as pd
import json
import os
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 1, 1),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'email': cfg.AIRFLOW_EMAIL,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


def download_data(url, headers, file):
    response = requests.request("GET", url, headers=headers)
    covid_data_json = response.json()['response']

    with open(file, 'w') as f:
        json.dump(covid_data_json, f)


def process_data(file_json, file_csv):
    with open(file_json, 'r') as file:
        covid_data_json = file.read()
    covid_data_json = json.loads(covid_data_json)
    df = pd.json_normalize(covid_data_json)
    df = df[df['continent'] != df['country']]
    df = df.dropna(axis=0, subset=['continent'])
    df.to_csv(file_csv, index=False)


def upload_to_s3(file_name, key, bucket_name, conn_id):
    s3_hook = S3Hook(conn_id)
    s3_hook.load_file(file_name, key, bucket_name)


def remove_local_files(dir):
    file_list = [f for f in os.listdir(dir)]
    for f in file_list:
        os.remove(os.path.join(dir, f))


with DAG(
        dag_id='covid_data_pipeline',
        schedule_interval='@daily',
        default_args=default_args,
        catchup=False) as dag:

    is_covid_data_available = HttpSensor(
        task_id='is_covid_data_available',
        method='GET',
        http_conn_id=cfg.COVID_API_CONN_ID,
        endpoint='latest',
        response_check=lambda response: 'response' in response.text,
        headers=cfg.COVID_API_HEADERS,
        poke_interval=1,
        timeout=5
    )

    download_covid_data = PythonOperator(
        task_id='download_covid_data',
        python_callable=download_data,
        op_args=[cfg.COVID_API_URL, cfg.COVID_API_HEADERS, cfg.COVID_DATA_JSON]
    )

    process_covid_data = PythonOperator(
        task_id='process_covid_data',
        python_callable=process_data,
        op_args=[cfg.COVID_DATA_JSON, cfg.COVID_DATA_CSV]
    )

    upload_data_to_s3 = PythonOperator(
        task_id='upload_data_to_S3',
        python_callable=upload_to_s3,
        op_args=[cfg.COVID_DATA_CSV, cfg.S3_KEY, cfg.COVID_DATA_BUCKET, cfg.S3_CONN_ID]
    )

    clean_local = PythonOperator(
        task_id='clean_local',
        python_callable=remove_local_files,
        op_args=[cfg.TEMP_DATA_DIR]
    )

is_covid_data_available >> download_covid_data >> process_covid_data >> upload_data_to_s3 >> clean_local
