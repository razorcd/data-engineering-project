# docker build -f Dockerfile -t spark_3_1_datamech . && docker run --rm --name spark_3_1_datamech  -it spark_3_1_datamech python main.py

from gcr.io/datamechanics/spark:platform-3.1-dm14
# from gcr.io/datamechanics/spark:platform-2.4-latest

ENV PYSPARK_MAJOR_PYTHON_VERSION=3
WORKDIR /opt/application/

COPY requirements.txt .
RUN pip3 install -r requirements.txt


COPY spark-bigquery-with-dependencies_2.12-0.24.0.jar /opt/spark/jars
# COPY gcs-connector-hadoop2-1.9.17.jar /opt/spark/jars
COPY gcs-connector-hadoop3-2.2.5-shaded.jar /opt/spark/jars
COPY conscrypt-openjdk-2.5.1-linux-x86_64.jar /opt/spark/jars
COPY flogger-system-backend-0.7.4.jar /opt/spark/jars

COPY razor-project-339321-f3a7294a27d0.p12 ./google_credentials.p12
COPY google_credentials.json .
ENV GOOGLE_APPLICATION_CREDENTIALS=/opt/bitnami/spark/google_credentials.json


COPY main.py .
COPY entry.sh .

# Run dbt
CMD ["bash", "-i", "entry.sh"]
