from pyspark.sql import SparkSession
from api import Callbacks
from config.AppProperties import *
from config.SparkConn import jdbcUrl, connectionProperties
from transformer import StudentScoreTransformer
import dash
from dash import html
from dash import dcc
import dash_bootstrap_components as dbc


## Init Spark
spark = SparkSession.builder \
    .appName("gcloud_sql") \
    .config("spark.jars", "../postgresql-42.6.0.jar") \
    .master("local").getOrCreate()

## Init Data
student_score_data = StudentScoreTransformer.StudentScoreTransformer().read_table_and_transform(spark, jdbcUrl, connectionProperties)

## Init App
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
Callbacks.register_callbacks(app, student_score_data)
app.layout = html.Div(
    [
        html.H1("Dashboard on Student Engagement in an E-Learning System"),
        html.H2("Total Students: " + str(student_score_data.shape[0])),
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
