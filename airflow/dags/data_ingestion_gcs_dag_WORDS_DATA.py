import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
import pyarrow as p
import pyarrow.parquet as pq
import pandas as pd 
import json 
import gzip


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

# test_data = "{{ dag_run.conf['input_data_date'] }}"
# dataset_file = "2022-03-18-23.json.gz"
file_date = "{{ (execution_date - macros.timedelta(hours=1)).strftime(\'%Y-%m-%d\') }}"
file_hour = "{{ (execution_date - macros.timedelta(hours=1)).strftime(\'%-H\') }}"
dataset_file = "" + file_date + "-" + file_hour + ".json.gz"
dataset_url = f"https://data.gharchive.org/{dataset_file}"
parquet_file = dataset_file.replace('.json.gz', '.parquet')
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'de_final_data')






default_args = {
    "owner": "airflow",
    # "start_date": datetime(2022, 3, 21, 20),
    "start_date": datetime.now()-timedelta(hours = 2),
    # "end_date": datetime.now(),
    "depends_on_past": False,
    "retries": 5,
    "retry_delay": timedelta(minutes=10)
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="data_ingestion_gcs_dag_WORDS_DATA_21",
    schedule_interval='30 * * * *',
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    bigquery_update_words_table_task = GoogleCloudStorageToBigQueryOperator(
        task_id = 'bigquery_update_words_table_task',
        bucket = "spark_github_words_razor-project-339321",
        source_objects = ["files/*.parquet"],
        destination_project_dataset_table = f'{PROJECT_ID}:{BIGQUERY_DATASET}.words_data',
        # schema_object = 'cities/us_cities_demo.json',
        # schema_fields=[
        #    {'name': 'STATE_ID', 'type': 'STRING', 'mode': 'NULLABLE'},
        #     {'name': 'STATE_NAME', 'type': 'STRING', 'mode': 'NULLABLE'},
        # ],
        # write_disposition='WRITE_APPEND',
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        source_format = 'PARQUET',
        skip_leading_rows = 0,
        max_bad_records=1000,
        ignore_unknown_values=True,
        autodetect = True
    )

    bigquery_update_words_table_task