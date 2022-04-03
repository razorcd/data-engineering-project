# Data Engineering Training

## Final Project:
https://github.com/razorcd/data-engineering-training/tree/main/final-project

---

## Start Postgres locally
```
docker run --rm -e POSTGRES_USER="root" -e POSTGRES_PASSWORD="root" -e POSTGRES_DB="ny_taxi" -v /home/cristiandugacicu/projects/personal/de-zoomcamp/course1b/postgres_data:/var/lib/postgresql/data -p 5432:5432 postgres:13
```

## Copy google credentials
```
cp secrets/razor-project-339321-3a382dd6a910.json  ~/.google/credentials/google_credentials.json
```

## Change repository owner of all folders
```
chown -R cristiandugacicu:cristiandugacicu dags
```

## Backfill Airflow dags
```
airflow dags backfill -s 2019-1-1 -e 2019-1-31 data_ingestion_gcs_dag3
```

#### ML in GCP using BigQuery

[BigQuery Machine Learning](https://youtu.be/B-WtpB0PuG4)  
[SQL for ML in BigQuery](big_query_ml.sql)

**Important links**
- [BigQuery ML Tutorials](https://cloud.google.com/bigquery-ml/docs/tutorials)
- [BigQuery ML Reference Parameter](https://cloud.google.com/bigquery-ml/docs/analytics-reference-patterns)
- [Hyper Parameter tuning](https://cloud.google.com/bigquery-ml/docs/reference/standard-sql/bigqueryml-syntax-create-glm)
- [Feature preprocessing](https://cloud.google.com/bigquery-ml/docs/reference/standard-sql/bigqueryml-syntax-preprocess-overview)

##### Deploying ML model
[BigQuery Machine Learning Deployment](https://youtu.be/BjARzEWaznU)  
[Steps to extract and deploy model with docker](extract_model.md)  


#### DBT

## run dbt
```
dbt run --profiles-dir .
```

## run one dbt model
```
dbt run -m model_name
or 
dbt build --select model_name
```

## run one dbt model and all it's dependencies
```
dbt build --select +model_name
```

## dbt variables
```
 -- dbt build --m <model.sql> --var 'is_test_run: false'
 {% if var('is_test_run', default=true) %}
```

## dbt seed overwrite table
```
dbt seed --full-refresh
```

#### Spark

## pyspark env vas
```
cd spark
export SPARK_HOME=`pwd`
export PYTHONPATH=$(ZIPS=("$SPARK_HOME"/python/lib/*.zip); IFS=:; echo "${ZIPS[*]}"):$PYTHONPATH
```
