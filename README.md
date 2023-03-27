# ebd2023-pyspark

Pre-requesite: Download a gcloud console: https://cloud.google.com/sdk/docs/install
-
Open the console and run below:

gcloud auth login

gcloud config set project ebd2023


## **Two working samples (for now)**:

### spark_gcloud_read_txt_demo.py - Read csv in Google Cloud Buckets and print it out. Suppose to run on Google Cloud Dataproc, not locally.

How to run:

1. gsutil cp "\your\absolute\path\to\spark_gcloud_read_txt_demo.py" gs://ebd2023_spark

2. Go to console -> Dataproc -> Jobs -> Submit Job -> select region "australia-southeast1" -> select cluster "ebd-project" -> select job type "PySpark" -> fill "main class" with "gs://ebd2023_spark/spark_gcloud_read_txt_demo.py" -> SUBMIT

3. You should see a table loaded in the logging part.

### spark_gcloud_sql_conn_demo.py - Read table from Google Cloud Postgres SQL and print it out. Suppose to run locally, not on cloud (hassle to setup on cloud).

How to run:

1. Download a cloud-sql-proxy first: https://cloud.google.com/sql/docs/mysql/sql-proxy

2. Install it and run: **cloud-sql-proxy -g --address 0.0.0.0 --port 1234 ebd2023:asia-southeast1:ebd2023**. -g means auth with your google account, ebd2023:asia-southeast1:ebd2023 is the instance full name shown on Google Cloud Postgres SQL

3. Find password in our google doc and substitue it in script. Run spark_gcloud_sql_conn_demo.py locally.

4. You should see the "assessment" table printed.
