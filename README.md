# GitHub events metrics

This is a Data Engineering project which creates a data pipeline for monitoring GitHub events in realtime (updates every 1 hr)
Github updates the dataset to the last hour.
Details: https://www.gharchive.org/

Github receives ~100k events every hour. Events like `Push`, `PullRequest`, `Fork`, etc. To monitor the business is performing well we defined the following KPIs to be visible in realtime.

KPIs:
- Total GitHub events stored.
- Events per hour and by type.
- Percentage of each event type.
- Most frequently used words in commits.

These KPIs were also chosen as an example for designing a data pipeline with frequent updates.

GitHub releases a new Event archive every hour. Our system also pulls data every hour, 15min after Github release. Our visualization dashboard will be up to date.


### Steps:
 - [x] data analysis to research which data to monitor
 - [x] pipeline to load files from GitHub API and store them in Google storage
 - [x] pipeline to import all files from Google store to Google BigQuery DB
 - [x] pipeline to transform from data lake to a Data Warehouse using clean data
 - [x] pipeline to transform from Data Warehouse raw tables to aggregated data views
 - [x] pipeline to create realtime visualization dashboards
 - [x] pipeline to perform batch/stream processing
 - [x] optimize DB queries(partition & cluster)
 - [x] review and cleanup pipelines
 - [x] deploy to cloud


### System architecture

Everything is Dockerized, so it can be deployed to any platform easily.

![image](https://user-images.githubusercontent.com/3721810/160255769-12c40df2-0d3d-406f-a85e-88b0c783cb2b.png)


### Exploratory data analysis

See the analysis in jupyter notebook: [data analysis](https://github.com/razorcd/data-engineering-training/blob/main/final-project/data-analysis/data_analysis.ipynb)

- loaded data for 1 hour from GitHub API
- cleaned up the data and created a DataFrame
- decided to create dashboards for following data:
    - Count of event types
    - Distribution of commit count for PushEvents
    - Most common words in commits

These 3 dashboards will be displayed in the final visualizations once the data pipeline is complete.    


## Steps to reproduce:

### 1. Terraform

To setup Google Cloud Platform: BigQuery and GCS run following commands from `/terraform_gcp/terraform` folder:

```sh
# login to gcloud cli
gcloud auth application-default login   

terraform init

terraform apply

# to destroy
terraform destroy
```

### 2. BigQuery partitioning and clustering

`github_data_clean` table is partitioned on `created_at_timestamp` field using DBT. This field is used to group event count by hour.

Optionally it can be created manually:
```sql
CREATE TABLE de_final_data.github_data_clean
(
  id STRING,
  created_at_timestamp TIMESTAMP,
  type STRING,
  actor_id_string STRING,
  actor_login STRING,
  public BOOLEAN,
  repo_id_string STRING,
  repo_name STRING,
  payload_commits STRING
 )
PARTITION BY 
  TIMESTAMP_TRUNC(created_at_timestamp, HOUR)
CLUSTER BY
  id
```


`de_final_data` table was recreated manually. `word` is grouped to count it's frequency.

```sql
CREATE TABLE de_final_data.words_data
(
    github_event_foreign_key STRING,
    word STRING
 )
CLUSTER BY
  github_event_foreign_key,
  word
```


### 3. Airflow

Airflow has a DAG with multiple jobs for Github events: 
    - download github file every hour
    - convert from .json.gz to .parquet
    - upload data to Google Storage
    - transfer data from Google Storage to Google Bigquery
    - delete local temp files

And another DAG with one job:    
    - transfer GitHub commit Words data from Google Storage to Google Bigquery

Run locally:    
```sh
cd airflow

#review .env file. 
#Ensure your google credentials are correct. This file must exist: ~/.google/credentials/google_credentials.json.

docker-compose up

# once started, open in browser:  http://localhost:8080/home    and start both DAGs
```

### 4.DBT
DBT is used to create BigQuery views of aggregated data. 

DBT image has a starting script to run updates every periodically.

- BigQuery table: `github_data_clean` with better field types, partitioning and clustering.
- BigQuery views: `events_per_hour_by_type`, `events_per_minute`, `count`, `most_used_commit_words`.

Run locally:
```sh
docker build -t dbt_transform .

# requires correct google_credentials.json path
docker run --rm --name dbtTransform -it -v $PWD:/dbt -v ..../google_credentials.json:/dbt/google_credentials.json -v profiles.yml:/root/.dbt/profiles.yml dbt_transform
```

### 5.Spark

Spark is used to load Github event data from BigQuery, extract commit messages, break messages in words and list most common words. The result is sent back to GCS as parquet file. Airflow job will take it back to BQ as a separate table.

Spark image has a starting script to run updates every periodically.

Run locally:
```sh
# Add google_credentials.json and p12 to this folder.

docker build -f Dockerfile -t spark_3_1_datamech . 

# Dockerfile3 can also be used but main.py file has to be manually submitted. See Dockerfile3 for comments.

docker run --rm --name spark_3_1_datamech -it spark_3_1_datamech
```


## Visualizations:

Visualizations are done using Google Data Studio and they update in realtime (every hour once data is ingested from GitHub).

View visualization Live: https://datastudio.google.com/s/urfSamU8nfQ

![image](https://user-images.githubusercontent.com/3721810/160303922-e308bc6c-ce06-4d36-9b22-13b6d9fb01ed.png)



## Cloud

Deploayed to Cloud following the "Steps to reproduce" from above.

I used GCP Compute Engine as a virtual machine instance. All docker images running together.

![image](https://user-images.githubusercontent.com/3721810/160302367-dd8f1186-2678-4bd1-8304-1e5c3089cae3.png)


