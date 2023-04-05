from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, desc, collect_list
import matplotlib.pyplot as plt

def setup_local_conn_props():
    # Define database connection properties
    jdbcHost = "0.0.0.0"  # Cloud SQL Proxy listening address
    jdbcPort = "1234"
    jdbcDatabase = "mockdb"
    jdbcUsername = "postgres"
    jdbcPassword = "ebd2023"

    # Set JDBC URL for the PostgreSQL database using the Cloud SQL Proxy
    jdbcUrl = f"jdbc:postgresql://{jdbcHost}:{jdbcPort}/{jdbcDatabase}"

    # Define properties for the database connection
    connectionProperties = {
        "user": jdbcUsername,
        "password": jdbcPassword,
        "driver": "org.postgresql.Driver"
    }
    return jdbcUrl, connectionProperties

def read_table_and_transform(spark, jdbcUrl, connectionProperties):
    df_a = spark.read.jdbc(url=jdbcUrl, table="assessment", properties=connectionProperties)
    df_a = df_a.withColumn('weight', df_a['weight']/100)
    df_sa = spark.read.jdbc(url=jdbcUrl, table="student_assessment", properties=connectionProperties)

    df_join = df_sa.join(df_a, on="id_assessment", how="left")

    return df_join.groupBy(["id_student", "code_presentation"]).agg(
        sum(col("score") * col("weight")).alias('weighted_score')
    ).sort(desc("weighted_score")).dropna()

def transformed_data(processed_data, col_name, top_or_bottom, number):
    data = processed_data.select(collect_list(col_name)).first()[0]

    if top_or_bottom.casefold() == "top":
        data = data[:number]
    else:
        data = data[-number:]

    if col_name == "id_student":
        data = [str(sid) for sid in data]
    elif col_name == "weighted_score":
        data = list(map(int, data))

    return data

def plot(x, y, x_label, y_label, title):
    plt.bar(x, y)
    for i in range(len(x)):
        plt.text(i, y[i], y[i], ha='center', va='bottom')
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(title)
    plt.show()

def run(top_or_bottom, number):
    spark = SparkSession.builder\
        .appName("gcloud_sql")\
        .config("spark.jars", "../postgresql-42.6.0.jar")\
        .master("local").getOrCreate()

    jdbcUrl, connectionProperties = setup_local_conn_props()
    processed_data = read_table_and_transform(spark, jdbcUrl, connectionProperties)

    student_ids = transformed_data(processed_data, "id_student", top_or_bottom, number)
    scores = transformed_data(processed_data, "weighted_score", top_or_bottom, number)

    plot(student_ids, scores, "Student ID", "Score", top_or_bottom.upper() + " " + str(number) + " Scores")

if __name__ == "__main__":
    top_or_bottom = input("You want to see 'top' or 'bottom' ?: ")
    if top_or_bottom.upper() not in ["TOP", "BOTTOM"]:
        print("Invalid input, must be 'top' or 'bottom'")
        exit()
    number = input(top_or_bottom.upper() + " how many?: ")
    number = int(number)
    run(top_or_bottom, number)