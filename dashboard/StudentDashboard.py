from pyspark.sql import SparkSession
from api import Callbacks
from config.AppProperties import *
from config.SparkConn import jdbcGoldUrl, connectionProperties
from model.DecisionTreeTrainer import train_data
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
student_score_data = StudentScoreTransformer.StudentScoreTransformer().read_table_and_transform(spark, jdbcGoldUrl, connectionProperties).toPandas()
student_info_data = StudentInfoMLTransformer.StudentInfoMLTransformer().read_table_and_transform(spark, jdbcGoldUrl, connectionProperties)

# Init Model Training
model_dic = {}
for code in code_modules:
    model = train_data(student_info_data, code, indexing_feature_cols, non_indexing_feature_cols, indexer_str)
    model_dic[code] = model
    print("Loaded ML model for " + code + " : " + model.__str__())

# Init App
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
Callbacks.register_callbacks(app, student_score_data, model_dic, final_result_options, result_style, student_info_data)
app.layout = html.Div(
    [
        html.H1("Dashboard on Student Engagement in an E-Learning System", style={'marginBottom': '20px'}),
        html.H2("Student Score Browser"),
        html.Div([
            html.Label('Top Or Bottom'),
            dcc.Dropdown(id='top_or_bottom', options=top_or_bottom_options, value='top', style={'width': '40%'}),
            html.Label('Semester'),
            dcc.Dropdown(id='semester', options=semester_options, value='2013J', style={'width': '40%'}),
        ], style={'display': 'flex', 'flexDirection': 'row'}),
        html.Div([
            html.Label('Query num students:'),
            dcc.Input(id='input-param', type='number', value=0),
            html.Button('Submit', id='submit-button', n_clicks=0)
        ]),
        html.Div([
            dcc.Graph(id='student-score-plot')
        ]),
        html.H2("Student Profile Quick View"),
        html.Div([
            dcc.Dropdown(id='semester-piechart', options=semester_options, value='2013J', style={'width': '50%'}),
            html.Div([
                dcc.Graph(id='gender_piechart'),
                dcc.Graph(id='age_band_piechart'),
            ], style={'display': 'flex', 'flex-direction': 'row'}),
            html.Div([
                dcc.Graph(id='imd_band_piechart'),
                dcc.Graph(id='highest_education_piechart')
            ], style={'display': 'flex', 'flex-direction': 'row'}),
            html.Div([
                dcc.Graph(id='disability_piechart')
            ])
        ]),
        html.H2("Student Exam Result Predictor"),
        html.Div([
            html.Label('Course to Predict'),
            dcc.Dropdown(id='course_to_predict', options=course_options, value='AAA', style={'width': '50%'}),
            html.Label('Gender'),
            dcc.Dropdown(id='gender', options=gender_options, style={'width': '50%'}),
            html.Label('Highest Education'),
            dcc.Dropdown(id='highest_education', options=highest_education_options, style={'width': '50%'}),
            html.Label('IMD Band'),
            dcc.Dropdown(id='imd_band', options=imd_band_options, style={'width': '50%'}),
            html.Label('Age Band'),
            dcc.Dropdown(id='age_band',  options=age_band_options, style={'width': '50%'}),
            html.Label('Disability'),
            dcc.Dropdown(id='disability', options=disability_options, style={'width': '50%'}),
            html.Label('Number of clicks:'),
            dcc.Input(id='total_clicks', type='number', value=30)
        ]),
        html.Div([
            html.Button('Submit', id='submit-button-prediction', n_clicks=0),
            html.Div(id='predicted_result', style={'color': 'green', 'fontSize': '30px'})
        ])
    ]
)

if __name__ == "__main__":
    app.run_server(debug=True)
