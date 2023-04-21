from pyspark.sql import SparkSession
from api import Callbacks
from config.AppProperties import *
from config.SparkConn import jdbcUrl, connectionProperties
from transformer import StudentScoreTransformer, StudentInfoMLTransformer
import dash
from dash import html
from dash import dcc
import dash_bootstrap_components as dbc
import os

os.environ["PYSPARK_PYTHON"] = "C:Users\siyuan\AppData\Local\Programs\Python\Python38\python.exe"

# Init Spark
spark = SparkSession.builder \
    .appName("gcloud_sql") \
    .config("spark.jars", "../postgresql-42.6.0.jar") \
    .master("local").getOrCreate()

# Init Data
student_score_data = StudentScoreTransformer.StudentScoreTransformer().read_table_and_transform(spark, jdbcUrl, connectionProperties)
training_data = StudentInfoMLTransformer.StudentInfoMLTransformer().read_table_and_transform(spark, jdbcUrl, connectionProperties)

# Init Model Training
indexer_str = "_idx"
indexing_feature_cols = ["gender", "highest_education", "imd_band", "age_band", "disability"]
non_indexing_feature_cols = ["total_click"]

model_dic = {}
for code in code_modules:
    model = (training_data, code, indexing_feature_cols, non_indexing_feature_cols, indexer_str)
    model_dic[code] = model
    print("Loaded ML model for " + code + " : " + model.__str__())

# Init App
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
Callbacks.register_callbacks(app, student_score_data)
app.layout = html.Div(
    [
        html.H1("Dashboard on Student Engagement in an E-Learning System"),
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
            html.Label('Semester'),
            dcc.Dropdown(
                id='semester',
                options=semester_options,
                value='2013J',
                style={'width': '50%'}
            ),
        ]),
        html.Div([
            html.Label('Query num students:'),
            dcc.Input(id='input-param', type='number', value=0),
            html.Button('Submit', id='submit-button', n_clicks=0)
        ]),
        html.Div([
            dcc.Graph(id='student-score-plot')
        ])
    ]
)

if __name__ == "__main__":
    app.run_server(debug=True)
