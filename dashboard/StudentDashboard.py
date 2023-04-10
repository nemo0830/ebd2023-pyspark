from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, desc
import dash
from dash import html
from dash import dcc
import dash_bootstrap_components as dbc
import plotly.express as px

def setup_local_conn_props():
    # Define database connection properties
    jdbcHost = "0.0.0.0"  # Cloud SQL Proxy listening address
    jdbcPort = "1234"
    jdbcDatabase = "mockdb"
    jdbcUsername = "postgres"
    jdbcPassword = "<replace_your_password_here>"

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
    df_a = spark.read.jdbc(url=jdbcUrl, table="assessments", properties=connectionProperties)
    df_a = df_a.withColumn('weight', df_a['weight']/100)
    df_sa = spark.read.jdbc(url=jdbcUrl, table="student_assessment", properties=connectionProperties)

    df_join = df_sa.join(df_a, on="id_assessment", how="left")

    return df_join.groupBy(["id_student", "code_presentation"])\
        .agg(sum(col("score") * col("weight")).alias('weighted_score'))\
        .dropna()\
        .select(col("id_student"), col("weighted_score")) \
        .sort(desc("weighted_score"))\
        .limit(3)\
        .toPandas()

def launch_dashboard_with_plots(df):
    app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
    title_font_size = 15
    fig = px.bar(df, x="id_student", y="weighted_score",
                 hover_data=['id_student', 'weighted_score'], color='weighted_score',
                 labels={'pop': ''}, height=450,
                 title="Top X students and their scores")\
        .update_layout(title_font_size=title_font_size)
    app.layout = html.Div(
        [
            html.H1("Dashboard on Student Engagement in an E-Learning System"),
            html.Ul(
                [
                    dcc.Graph(id='score-1st-attempt', figure=fig)

                ]
            ),
        ]
    )
    return app

def run():
    spark = SparkSession.builder\
        .appName("gcloud_sql")\
        .config("spark.jars", "../postgresql-42.6.0.jar")\
        .master("local").getOrCreate()

    jdbcUrl, connectionProperties = setup_local_conn_props()
    processed_data = read_table_and_transform(spark, jdbcUrl, connectionProperties)
    return launch_dashboard_with_plots(processed_data)

if __name__ == "__main__":
    ## only have 1 query now, will try to enrich
    configed_app = run()
    configed_app.run_server(debug=True)