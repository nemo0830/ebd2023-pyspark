# Define database connection properties
from pyspark.sql import SparkSession

jdbcHost = "0.0.0.0"  # Cloud SQL Proxy listening address
jdbcPort = "1234"
jdbcGoldDatabase = "golddb"
jdbcWorkDatabase = "workdb"
jdbcUsername = "postgres"
jdbcPassword = "mock_pwd"

# Set JDBC URL for the PostgreSQL database using the Cloud SQL Proxy
jdbcGoldUrl = f"jdbc:postgresql://{jdbcHost}:{jdbcPort}/{jdbcGoldDatabase}"
jdbcWorkUrl = f"jdbc:postgresql://{jdbcHost}:{jdbcPort}/{jdbcWorkDatabase}"

# Define properties for the database connection
connectionProperties = {
    "user": jdbcUsername,
    "password": jdbcPassword,
    "driver": "org.postgresql.Driver"
}

spark = SparkSession.builder \
    .appName("gcloud_sql") \
    .config("spark.jars", "../postgresql-42.6.0.jar") \
    .master("local").getOrCreate()