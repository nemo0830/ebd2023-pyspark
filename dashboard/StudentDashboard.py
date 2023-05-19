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
import pandas as pd
from legacy.LegacyQuery import *
from legacy.LegacyConn import *
from legacy import LegacyCallbacks

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

#Loading legacy componet
print("Loading legacy plot...")

dfPie = pd.read_sql(pieQuery, dbConnection)

dfRegionLine = pd.read_sql(regionLineQuery, dbConnection)
dfAgeBandLine = pd.read_sql(ageBandLineQuery, dbConnection)
dfHighestEduLine = pd.read_sql(HighestEducationLineQuery, dbConnection)
dfFinalResultLine = pd.read_sql(finalResultLineQuery, dbConnection)
dfGenderLine = pd.read_sql(genderLineQuery, dbConnection)

dfGenderBar = pd.read_sql(GenderBarQuery, dbConnection)
dfRegionBar = pd.read_sql(RegionBarQuery, dbConnection)
dfAgeBandBar = pd.read_sql(AgeBandBarQuery, dbConnection)
dfFinalResultBar = pd.read_sql(FinalResultBarQuery, dbConnection)
dfHighestEduBar = pd.read_sql(HighestEduBarQuery, dbConnection)

dfGenderScatter = pd.read_sql(GenderScatterQuery, dbConnection)
dfRegionScatter = pd.read_sql(RegionScatterQuery, dbConnection)
dfAgeBandScatter = pd.read_sql(AgeBandScatterQuery, dbConnection)
dfFinalResultScatter = pd.read_sql(FinalResultScatterQuery, dbConnection)
dfHighestEduScatter = pd.read_sql(HighestEduScatterQuery, dbConnection)

# Init App
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
Callbacks.register_callbacks(app, student_score_data, model_dic, final_result_options, result_style)
LegacyCallbacks.register_callbacks(app, dfPie, dfRegionLine, dfAgeBandLine, dfHighestEduLine, dfFinalResultLine, dfGenderLine,
                                   dfRegionBar, dfAgeBandBar, dfHighestEduBar, dfFinalResultBar, dfGenderBar,
                                   dfRegionScatter, dfAgeBandScatter, dfHighestEduScatter, dfFinalResultScatter, dfGenderScatter)
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
        html.H2("Student Info Plots"),
        dbc.Row([
            dbc.Col(dcc.Dropdown(
                id='my_dropdown',
                options=[
                    {'label': 'Gender', 'value': 'gender'},
                    {'label': 'Region', 'value': 'region'},
                    {'label': 'Highest Education', 'value': 'highest_education'},
                    {'label': 'Age Band', 'value': 'age_band'},
                    {'label': 'Final Result', 'value': 'final_result'}
                ],
                value='region',
                multi=False,
                clearable=False,
            ), width=3),
            dbc.Col(dcc.Dropdown(
                dfRegionLine['code_presentation_description'].unique(),
                dfRegionLine['code_presentation_description'].iloc[0],
                id='my_dropdown2',
                multi=False,
                clearable=False,
            ), width=3,)
        ]),
        dbc.Row([
            dbc.Col(html.Label(['Filter By Student Demographics :']), width=3),
            dbc.Col(html.Label(['Filter By Course Semester :']), width=3)
        ], style={'position':'sticky','top':0}),
        dbc.Row([
            dbc.Col(dcc.Graph(id='sunBurst')),
            dbc.Col(dcc.Graph(id='pieChart'))
        ]),
        dbc.Row([
            dbc.Col(dcc.Graph(id='sunBurst2')),
            dbc.Col(dcc.Graph(id='pieChart2'))
        ]),
        dbc.Row(html.Div([
            dcc.Graph(id='histogram')
        ])),
        dbc.Row(html.Div([
            dcc.Graph(id='barChart')
        ])),
        dbc.Row(html.Div([
            dcc.Graph(id='scatterChartOverView')
        ])),
        dbc.Row(html.Div([
            dcc.Graph(id='scatterChart')
        ])),
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
