from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .appName("gcloud_sql")\
    .config("spark.jars", "./postgresql-42.6.0.jar")\
    .master("local").getOrCreate()
# Define database connection properties
jdbcHost = "0.0.0.0"  # Cloud SQL Proxy listening address
jdbcPort = "1234"
jdbcDatabase = "mockdb"
jdbcUsername = "postgres"
jdbcPassword = "<replace_actual_password_here>"

# Set JDBC URL for the PostgreSQL database using the Cloud SQL Proxy
jdbcUrl = f"jdbc:postgresql://{jdbcHost}:{jdbcPort}/{jdbcDatabase}"

# Define properties for the database connection
connectionProperties = {
    "user": jdbcUsername,
    "password": jdbcPassword,
    "driver": "org.postgresql.Driver"
}

# Read data from a PostgreSQL table
df = spark.read.jdbc(url=jdbcUrl, table="assessment", properties=connectionProperties)

# Display the data
df.show()