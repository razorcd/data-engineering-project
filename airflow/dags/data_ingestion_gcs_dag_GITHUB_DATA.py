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
file_date = "{{ (execution_date - macros.timedelta(hours=0)).strftime(\'%Y-%m-%d\') }}"
file_hour = "{{ (execution_date - macros.timedelta(hours=0)).strftime(\'%-H\') }}"
dataset_file = "" + file_date + "-" + file_hour + ".json.gz"
dataset_url = f"https://data.gharchive.org/{dataset_file}"
parquet_file = dataset_file.replace('.json.gz', '.parquet')
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'de_final_data')




def flatten_json(y):
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            out[name[:-1]] = json.dumps(x)
#             i = 0
#             for a in x:
#                 flatten(a, name + str(i) + '_')
#                 i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out

def format_to_parquet(src_file):
    if not src_file.endswith('.json.gz'):
        logging.error("Can only accept source files in json.gz format, for the moment")
        return
    
    logging.info("Reading Json file as flatten dict array: "+src_file)
    data_flatten = []
    with gzip.open(src_file, 'rb') as f:
        for line in f:
            j = json.loads(line)
            data_flatten.append(flatten_json(j))
            
    logging.info("Converting flatten dict array to Pandas DataFrame.")
    selected_columns = ["id", "created_at", "type", "actor_id", "actor_login", "public", "repo_id", "repo_name", "payload_commits"]
    df_data_flatten = pd.DataFrame.from_dict(data_flatten)[selected_columns]

    logging.info("Converting Pandas DataFrame to pyarrow.")
    table_flatten = p.Table.from_pandas(df_data_flatten)

    dest_file = src_file.replace('.json.gz', '.parquet')
    logging.info("Writing Parquet file: " + dest_file)
    pq.write_table(table_flatten, dest_file)

def delete_local_files(filenames):
    for filename in filenames:
        if os.path.exists(filename):
            os.remove(filename)
        else:
            print("The file does not exist: "+filename)    


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
    # "start_date": datetime(2022, 3, 21, 20),
    "start_date": datetime.now()-timedelta(hours = 2),
    # "end_date": datetime.now(),
    "depends_on_past": False,
    "retries": 5,
    "retry_delay": timedelta(minutes=10)
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="data_ingestion_gcs_dag_GITHUB_DATA_34",
    schedule_interval='15 * * * *',
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSf {dataset_url} > {path_to_local_home}/{dataset_file}"
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{dataset_file}",
        },
    )

    delete_local_files_task = PythonOperator(
        task_id="delete_local_files_task",
        python_callable=delete_local_files,
        op_kwargs={
            "filenames": [f"{path_to_local_home}/{dataset_file}", f"{path_to_local_home}/{parquet_file}"],
        },
    )


    # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw_data/{parquet_file}",
            "local_file": f"{path_to_local_home}/{parquet_file}",
        },
    )


    bigquery_update_table_task = GoogleCloudStorageToBigQueryOperator(
        task_id = 'bigquery_update_table_task',
        bucket = BUCKET,
        source_objects = [f"raw_data/{parquet_file}"],
        destination_project_dataset_table = f'{PROJECT_ID}:{BIGQUERY_DATASET}.github_data',
        # schema_object = 'cities/us_cities_demo.json',
        write_disposition='WRITE_APPEND',
        source_format = 'PARQUET',
        skip_leading_rows = 0,
        max_bad_records=1000,
        ignore_unknown_values=True,
        autodetect = True
    )

    download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> bigquery_update_table_task >> delete_local_files_task