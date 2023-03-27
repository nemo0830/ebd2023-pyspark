from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

spark = SparkSession.builder\
    .appName("gcloud_sql")\
    .config("spark.jars", "../postgresql-42.6.0.jar")\
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
df_a = spark.read.jdbc(url=jdbcUrl, table="assessment", properties=connectionProperties)
df_a = df_a.withColumn('weight', df_a['weight']/100)
df_sa = spark.read.jdbc(url=jdbcUrl, table="student_assessment", properties=connectionProperties)

df_join = df_sa.join(df_a, on="id_assessment", how="left")

grouped_data = df_join.groupBy(["id_student", "code_presentation"]).agg(
    sum(col("score") * col("weight")).alias('weighted_score')
)

grouped_data.orderBy(["id_student", "code_presentation"]).where(col('id_student') == '100064').show()