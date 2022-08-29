import os, datetime
# from databricks import koalas as ks
# from pyspark.sql.dataframe import DataFrame
# from pyspark.sql.types import *
# from pyspark.sql.functions import explode, split, col, sum, lit
# from pyspark.sql import SparkSession

# def apply_transforms(df: DataFrame) -> DataFrame:

#     # split _c0 column as it is a string and we want the population data from it
#     split_col = split(df['_c0'], '\t')

#     # add population column, group by country, sum population
#     return df \
#             .withColumn("population", split_col.getItem(2).cast('float')) \
#             .groupBy("country") \
#             .agg(col("country"), sum("population")).select(col("country"), col("sum(population)") \
#             .alias("population"))

if __name__ == "__main__":

    # # build spark session
    # spark = SparkSession.builder.appName("KoalasPostgresDemo").getOrCreate()

    # # Enable hadoop s3a settings
    # spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
    # spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    # spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", \
    # "com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    # spark._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A")

    # # read data from publc bucket into Spark DF
    # data_path = "s3a://dataforgood-fb-data/csv/" 
    # df = spark.read.csv(data_path)

    # # apply spark transformations
    # transformedDF = df.transform(apply_transforms)

    # # build Koalas DF from Spark DF, get median, convert back to Spark DataFrame, add column with current date
    # kDF = ks.DataFrame(transformedDF)
    # medianDF = kDF.median().withColumn
    # finalDF = medianDF.to_spark().withColumn("etl_time", lit(datetime.datetime.now()))

    # # SQL metadata
    # properties = {"user": os.environ['PG_USER'],"postgres": os.environ['PG_PASSWORD'],"driver": "org.postgresql.Driver"}
    # url = f"jdbc:postgresql://{os.environ['PG_HOST']}:{os.environ['PG_PORT']}/{os.environ['PG_DB_NAME']}"

    # # write to db
    # finalDF.write.jdbc(url=url, table=os.environ['TABLE_NAME'], mode="overwrite", properties=properties)

    #1 - Loading a Google BigQuery table into a DataFrame
    # Initializing SparkSession
    import logging
    from pyspark.sql import SparkSession
    import base64


    # import time
    # time.sleep(9999999)


    spark = SparkSession.builder.master('local[*]') \
        .appName('spark-read-from-bigquery') \
        .config('BigQueryProjectId','razor-project-339321') \
        .config('BigQueryDatasetLocation','de_final_data') \
        .config('parentProject','razor-project-339321') \
        .config("google.cloud.auth.service.account.enable", "true") \
        .config("credentialsFile", "google_credentials.json") \
        .config("GcpJsonKeyFile", "google_credentials.json") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.memory.offHeap.enabled",True) \
        .config("spark.memory.offHeap.size","5g") \
        .config('google.cloud.auth.service.account.json.keyfile', "google_credentials.json") \
        .config("fs.gs.project.id", "razor-project-339321") \
        .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .getOrCreate()
        # .config("spark.driver.memory", "12g") \
        # .config("spark.executor.memory", "5g") \
        # .config("spark.memory.offHeap.enabled",True) \
        # .config("spark.memory.offHeap.size","4g") \
        # .config('project','razor-project-339321') \
    # Creating DataFrames

    
    spark.conf.set("GcpJsonKeyFile","google_credentials.json")
    spark.conf.set("BigQueryProjectId","razor-project-339321")
    spark.conf.set("BigQueryDatasetLocation","de_final_data")
    spark.conf.set("google.cloud.auth.service.account.enable", "true")
    spark.conf.set('google.cloud.auth.service.account.json.keyfile', "google_credentials.json")
    spark.conf.set("fs.gs.project.id", "razor-project-339321")
    spark.conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    spark.conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    spark.conf.set('temporaryGcsBucket', "spark_temp_gh")



    spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
    # This is required if you are using service account and set true, 
    spark._jsc.hadoopConfiguration().set("fs.gs.project.id", "razor-project-339321")
    spark._jsc.hadoopConfiguration().set('fs.gs.auth.service.account.enable', 'true')
    spark._jsc.hadoopConfiguration().set('google.cloud.auth.service.account.json.keyfile', "google_credentials.json")
    # # Following are required if you are using oAuth
    # spark._jsc.hadoopConfiguration().set('fs.gs.auth.client.id', 'YOUR_OAUTH_CLIENT_ID')
    # spark._jsc.hadoopConfiguration().set('fs.gs.auth.client.secret', 'OAUTH_SECRET')


    # spark.sparkContext.setLogLevel("ERROR")

    # Use the Cloud Storage bucket for temporary BigQuery export data used
    # by the connector.
    

    logging.basicConfig(level=logging.INFO)

    logging.info("\nSpark configs: \n")
    logging.info(spark.sparkContext._conf.getAll())  # check the config

    logging.info("Spark URL: " + spark.sparkContext.uiWebUrl)


    sql = """
        SELECT *
        FROM github_data_clean
        LIMIT 1000000
        ORDER_BY created_at_timestamp DESC
    """

    df = spark.read \
        .format('bigquery') \
        .option("credentialsFile","google_credentials.json") \
        .option("project", "razor-project-339321") \
        .option("parentProject", "razor-project-339321") \
        .option('dataset','de_final_data') \
        .option('table','github_data_clean') \
        .load(sql)
    
    logging.info("\nImput table schema: ")
    df.printSchema()

    # df.show(1,False)


    df.createOrReplaceTempView("github_data_clean_temp")


    # from pyspark.sql.functions import hour
    # df = df \
    #     .limit(1) \
    #     .show(1, truncate=True)

    from pyspark.sql.functions import udf, array, explode, col
    from pyspark.sql.types import ArrayType
    from pyspark.sql.types import StringType
    from pyspark.sql.types import IntegerType
    import json
    import re

    def get_commit_words(json_commits):
        commits = json.loads(json_commits)
        words = []
        good_words = []
        # return commits[0]["message"].split(' ')
        
        for commit in commits:
            words = words + commit['message'].split(' ')
            # commit_words = filter(lambda w: (w!=None and type(w)==unicode), commit_words)
        
        for w in words:
            if ((w!=None)):
                w = re.sub(r"[~`!@#$%^&*()_+-={}\[\]\\|;:',\.<>/?\"]", "", w.lower()).strip()
                if (w!=''): good_words.append(w)
        #     filtered_words = filter(lambda w: (w!=''), clean_words)
            # words = commit_words

        return good_words    

    comits_count = udf(lambda x: len(json.loads(x)), IntegerType())
    comits_words = udf(lambda x: get_commit_words(x), ArrayType(StringType()))
    words_count = udf(lambda x: len(x), IntegerType())



    df1 = spark \
        .sql("""
            SELECT id, payload_commits
            FROM github_data_clean_temp
            WHERE type = 'PushEvent'
            """) \
        .repartition(4)
    df2 = df1.withColumn("commits_count", comits_count("payload_commits")) \
        # .show()

    df3 = df2 \
        .withColumn("commits_words", comits_words("payload_commits")) \
        .withColumn("words_count", words_count("commits_words"))
    df4 = df3.select(df3.id, explode(df3.commits_words).alias("word")) \
        .withColumnRenamed("id", "github_event_foreign_key") 
        # .show()

    # df4.limit(5).show()

    df4.write \
        .format("parquet") \
        .mode("overwrite") \
        .option("credentialsFile","google_credentials.json") \
        .option("project", "razor-project-339321") \
        .option("parentProject", "razor-project-339321") \
        .option("fs.gs.project.id", "razor-project-339321") \
        .option("table", "de_final_data.github_data_counter2") \
        .option("persistentGcsBucket", "spark_temp_gh1") \
        .option("persistentGcsPath", "spark_temp_gh1") \
        .save('gs://spark_github_words_razor-project-339321/files/')


        # .format("bigquery") \
        # .save()
        # .option("writeMethod", "direct") \
        # .mode("append") \
        # .save("razor-project-339321.de_final_data.github_data_counter")
        # .option("temporaryGcsBucket","spark_temp_gh") \
        # .coalesce(1) \

    # df. \
    #     write. \
    #     format("bigquery"). \
    #     option("table", "de_final_data.github_data_clean2"). \
    #     save()
        # mode("overwrite"). \





    # sc = spark.SparkContext()
    # lines = sc.textFile("111")
    # words = lines.flatMap(lambda line: line.split())
    # wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda count1, count2: count1 + count2)
    # wordCounts.saveAsTextFile(sys.argv[2])





# spark.conf.set("GcpJsonKeyFile",jsonKeyFile)
# spark.conf.set("BigQueryProjectId",projectId)
# spark.conf.set("BigQueryDatasetLocation",datasetLocation)
# spark.conf.set("google.cloud.auth.service.account.enable", "true")
# spark.conf.set("fs.gs.project.id", projectId)
# spark.conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
# spark.conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
# spark.conf.set("temporaryGcsBucket", tmp_bucket)
# sqltext = ""
# print("\nreading data from "+projectId+":"+inputTable)
# source_df = spark.read. \
#               format("bigquery"). \
#               option("credentialsFile",jsonKeyFile). \
#               option("project", projectId). \
#               option("parentProject", projectId). \
#               option("dataset", sourceDataset). \
#               option("table", sourceTable). \
#               load()

# # Save data to a BigQuery table
# print("\nsaving data to " + outputTable)
# summary_df. \
#     write. \
#     format("bigquery"). \
#     mode("overwrite"). \
#     option("table", fullyQualifiedoutputTableId). \
#     save()

# print("\nreading data from "+projectId+":"+outputTable)
# target_df = spark.read. \
#               format("bigquery"). \
#               option("credentialsFile",jsonKeyFile). \
#               option("project", projectId). \
#               option("parentProject", projectId). \
#               option("dataset", targetDataset). \
#               option("table", targetTable). \
#               load()
# target_df.show(1,False)














    # commits.write.format('bigquery') \
    #     .option('dataset','trips_data_all') \
    #     .option('table','dim_zones3') \
    #     .save()        
    #     # .mode(SaveMode.Append) \
    # .option('table', 'wordcount_dataset.wordcount_output') \

    # rdd2=commits.rdd.map(lambda x: (x["payload_commits"]))
    # rdd2.toDF("payload_commits").show()











    # 3 - Creating or replacing a local temporary view with this DataFrame
    # df.createOrReplaceTempView("counter")
    # # Perform select order_id
    # peopleCountDf = spark.sql("SELECT COUNT(*) from github_data_clean")
    # # Display the content of df
    # peopleCountDf.show()
    # df = df.select('id')
    # # The filters that are allowed will be automatically pushed down.
    # # Those that are not will be computed client side
    # df = df.where("id > 0 ")
    # df = df.limit("100")
    # # Further processing is done inside Spark
    # # df = df.groupBy('word').sum('word_count')
    # # df = df.orderBy(df['sum(word_count)'].desc()).cache()

    # print('The resulting schema is')
    # df.printSchema()

    # print('The top words in shakespeare are')
    # df.show()







# #!/usr/bin/env python
# # Copyright 2018 Google Inc. All Rights Reserved.
# #
# # Licensed under the Apache License, Version 2.0 (the "License");
# # you may not use this file except in compliance with the License.
# # You may obtain a copy of the License at
# #
# #       http://www.apache.org/licenses/LICENSE-2.0
# #
# # Unless required by applicable law or agreed to in writing, software
# # distributed under the License is distributed on an "AS IS" BASIS,
# # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# # See the License for the specific language governing permissions and
# # limitations under the License.

# from __future__ import print_function
# import tempfile
# from pyspark.sql import SparkSession

# spark = SparkSession.builder.appName('Shakespeare WordCount').getOrCreate()

# table = 'bigquery-public-data.samples.shakespeare'
# df = spark.read.format('bigquery').load(table)
# # Only these columns will be read
# df = df.select('word', 'word_count')
# # The filters that are allowed will be automatically pushed down.
# # Those that are not will be computed client side
# df = df.where("word_count > 0 AND word NOT LIKE '%\\'%'")
# # Further processing is done inside Spark
# df = df.groupBy('word').sum('word_count')
# df = df.orderBy(df['sum(word_count)'].desc()).cache()

# print('The resulting schema is')
# df.printSchema()

# print('The top words in shakespeare are')
# df.show()

# # Use tempfile just to get random directory name. Spark will create the
# # directory in the default file system anyways.
# path = tempfile.mkdtemp(prefix='spark-bigquery')
# print('Writing table out to {}'.format(path))
# df.write.csv(path)