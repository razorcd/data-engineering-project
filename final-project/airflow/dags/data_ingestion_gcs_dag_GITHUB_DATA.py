import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime
from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
# import pyarrow.json as pv
# import pyarrow.parquet as pq
# import gzip

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

# test_data = "{{ dag_run.conf['input_data_date'] }}"
# dataset_file = "2022-03-18-23.json.gz"
file_date = "{{ execution_date.strftime(\'%Y-%m-%d\') }}"
file_hour = "{{ execution_date.strftime(\'%-H\') }}"
# dataset_file = "yellow_tripdata_" + file_date + ".csv"
dataset_file = "" + file_date + "-" + file_hour + ".json.gz"
# dataset_url = f"https://s3.amazonaws.com/nyc-tlc/trip+data/{dataset_file}"
dataset_url = f"https://data.gharchive.org/{dataset_file}"


path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
parquet_file = dataset_file.replace('.csv', '.parquet')
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')


# def format_to_parquet(src_file):
#     if not src_file.endswith('.json.gz'):
#         logging.error("Can only accept source files in json.gz format, for the moment")
#         return
#     logging.info("Converting file to parquet: "+src_file)

#     with gzip.open(src_file) as fp:
#         table = pv.read_json(fp)
#         pq.write_table(table, src_file.replace('.json.gz', '.parquet'))


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "start_date": datetime(2022, 3, 20, 17),
    "end_date": datetime.now(),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="data_ingestion_gcs_dag_GITHUB_DATA_4",
    schedule_interval="@hourly",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSf {dataset_url} > {path_to_local_home}/{dataset_file}"
    )

    # format_to_parquet_task = PythonOperator(
    #     task_id="format_to_parquet_task",
    #     python_callable=format_to_parquet,
    #     op_kwargs={
    #         "src_file": f"{path_to_local_home}/{dataset_file}",
    #     },
    # )

    # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw_data/{dataset_file}",
            "local_file": f"{path_to_local_home}/{dataset_file}",
        },
    )


    # bigquery_update_table_task = GoogleCloudStorageToBigQueryOperator(
    #     task_id = 'bigquery_update_table_task',
    #     bucket = BUCKET,
    #     source_objects = [f"raw_yellow_tripdata/{parquet_file}"],
    #     destination_project_dataset_table = f'{PROJECT_ID}:{BIGQUERY_DATASET}.yellow_tripdata4',
    #     # schema_object = 'cities/us_cities_demo.json',
    #     write_disposition='WRITE_APPEND',
    #     source_format = 'parquet',
    #     skip_leading_rows = 1,
    #     autodetect = True
    # )

    download_dataset_task >> local_to_gcs_task