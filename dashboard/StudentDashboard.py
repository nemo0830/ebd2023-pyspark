from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, desc
import dash
from dash import html
from dash import dcc
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State
import plotly.express as px

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
    df_a = spark.read.jdbc(url=jdbcUrl, table="assessments", properties=connectionProperties)
    df_a = df_a.withColumn('weight', df_a['weight']/100)
    df_sa = spark.read.jdbc(url=jdbcUrl, table="student_assessment", properties=connectionProperties)

    df_join = df_sa.join(df_a, on="id_assessment", how="left")

    return df_join.groupBy(["id_student", "code_presentation"]) \
        .agg(sum(col("score") * col("weight")).alias('weighted_score')) \
        .dropna() \
        .select(col("id_student"), col("weighted_score")) \
        .sort(desc("weighted_score")) \
        .toPandas()



## Init Spark
spark = SparkSession.builder \
    .appName("gcloud_sql") \
    .config("spark.jars", "../postgresql-42.6.0.jar") \
    .master("local").getOrCreate()

jdbcUrl, connectionProperties = setup_local_conn_props()
processed_data = read_table_and_transform(spark, jdbcUrl, connectionProperties)
processed_data = processed_data[processed_data.weighted_score != 0.0]

## Init app
top_or_bottom_options = [
    {'label': 'top', 'value': 'top'},
    {'label': 'bottom', 'value': 'bottom'}
]

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
title_font_size = 15


@app.callback(
    Output('output-plot', 'figure'),
    Input('top_or_bottom', 'value'),
    Input('submit-button', 'n_clicks'),
    State('input-param', 'value'),
)
def update_output(top_or_bottom, n_clicks, input_param, df=processed_data):
    print(top_or_bottom)
    print(input_param)
    if top_or_bottom == "top":
        df = df.head(input_param)
    else:
        df = df.tail(input_param)
    fig = px.bar(df, x="id_student", y="weighted_score",
                 hover_data=['id_student', 'weighted_score'], color='weighted_score',
                 labels={'pop': ''}, height=450,
                 title=top_or_bottom + " " + str(input_param) + " students and their scores") \
        .update_layout(title_font_size=title_font_size)
    return fig

app.layout = html.Div(
    [
        html.H1("Dashboard on Student Engagement in an E-Learning System"),
        html.H2("Total Students: " + str(processed_data.shape[0])),
        html.Div([
            html.Label('Top Or Bottom'),
            dcc.Dropdown(
                id='top_or_bottom',
                options=top_or_bottom_options,
                value='top',
                style={'width': '50%'}
            ),
        ]),
        html.Div([
            html.Label('Query num students:'),
            dcc.Input(id='input-param', type='number', value=0),
            html.Button('Submit', id='submit-button', n_clicks=0)
        ]),
        html.Div([
            dcc.Graph(id='output-plot')
        ])
    ]
)


if __name__ == "__main__":
    app.run_server(debug=True)