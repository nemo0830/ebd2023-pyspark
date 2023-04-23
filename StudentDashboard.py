from pyspark.sql import SparkSession
from api import Callbacks
from config.AppProperties import *
from config.SparkConn import *
from model.DecisionTreeTrainer import train_data
from transformer import StudentScoreTransformer, StudentInfoMLTransformer
from google.cloud.sql.connector import Connector
import dash
from dash import html
from dash import dcc
import dash_bootstrap_components as dbc
import os
from sqlalchemy import create_engine, text
import pandas as pd
from dash.dependencies import Input, Output
import plotly.express as px

# Init Spark
spark = SparkSession.builder \
    .appName("gcloud_sql") \
    .config("spark.jars", "../postgresql-42.6.0.jar") \
    .master("local").getOrCreate()

# initialize Connector object
connector = Connector()

# function to return the database connection object
def getconn():
    conn = connector.connect(
        INSTANCE_CONNECTION_NAME,
        "pg8000",
        user=jdbcUsername,
        password=db_pass,
        db=jdbcDatabase
    )
    return conn

# create connection pool with 'creator' argument to our connection object function
alchemyEngine = create_engine(
    "postgresql+pg8000://",
    creator=getconn,
)
dbConnection = alchemyEngine.connect()

# Init DB Data
pieChartQuery = text("""SELECT si.gender,si.region,si.highest_education,si.age_band, si.final_result
    FROM studentassessment sa join studentvle sv
    on sa.id_student = sv.id_student
    join studentinfo si
     on si.id_student = sa.id_student
    where (sa.id_student, sa.date_submitted) 
    in (select id_student, max(date_submitted)
    from studentassessment group by id_student)
    and sa.date_submitted = sv.date""")

regionLineQuery = text("""SELECT si.gender,si.region, count(*) as count
    FROM studentassessment sa join studentvle sv
    on sa.id_student = sv.id_student
    join studentinfo si
    on si.id_student = sa.id_student
    where (sa.id_student, sa.date_submitted) 
    in (select id_student, max(date_submitted)
    from studentassessment group by id_student)
    and sa.date_submitted = sv.date
    group by si.gender,si.region""")

ageBandLineQuery = text("""SELECT si.gender,si.age_band,count(*) as count
    FROM studentassessment sa join studentvle sv
    on sa.id_student = sv.id_student
    join studentinfo si
    on si.id_student = sa.id_student
    where (sa.id_student, sa.date_submitted) 
    in (select id_student, max(date_submitted)
    from studentassessment group by id_student)
    and sa.date_submitted = sv.date
    group by si.gender,si.age_band""")

HighestEducationLineQuery = text("""SELECT si.gender,si.highest_education,count(*) as count
    FROM studentassessment sa join studentvle sv
    on sa.id_student = sv.id_student
    join studentinfo si
    on si.id_student = sa.id_student
    where (sa.id_student, sa.date_submitted) 
    in (select id_student, max(date_submitted)
    from studentassessment group by id_student)
    and sa.date_submitted = sv.date
    group by si.gender,si.highest_education""")

finalResultLineQuery = text("""SELECT si.gender,si.final_result,count(*) as count
    FROM studentassessment sa join studentvle sv
    on sa.id_student = sv.id_student
    join studentinfo si
    on si.id_student = sa.id_student
    where (sa.id_student, sa.date_submitted) 
    in (select id_student, max(date_submitted)
    from studentassessment group by id_student)
    and sa.date_submitted = sv.date
    group by si.gender,si.final_result""")

genderLineQuery = text("""SELECT si.gender,count(*) as count
    FROM studentassessment sa join studentvle sv
    on sa.id_student = sv.id_student
    join studentinfo si
    on si.id_student = sa.id_student
    where (sa.id_student, sa.date_submitted) 
    in (select id_student, max(date_submitted)
    from studentassessment group by id_student)
    and sa.date_submitted = sv.date
    group by si.gender""")

GenderBarQuery = text("""SELECT count(v.activity_type) as login_count, v.activity_type, si.gender
    FROM studentassessment sa 
    join studentvle sv
    on sa.id_student = sv.id_student
    join vle v
    on v.id_site = sv.id_site
    join studentinfo si
    on si.id_student = sa.id_student
    where (sa.id_student, sa.date_submitted) 
    in (select id_student, max(date_submitted)
    from studentassessment group by id_student)
    and sa.date_submitted = sv.date
    group by v.activity_type,si.gender""")

RegionBarQuery = text("""SELECT count(v.activity_type) as login_count, v.activity_type, si.region
    FROM studentassessment sa 
    join studentvle sv
    on sa.id_student = sv.id_student
    join vle v
    on v.id_site = sv.id_site
    join studentinfo si
    on si.id_student = sa.id_student
    where (sa.id_student, sa.date_submitted) 
    in (select id_student, max(date_submitted)
    from studentassessment group by id_student)
    and sa.date_submitted = sv.date
    group by v.activity_type,si.region""")

AgeBandBarQuery = text("""SELECT count(v.activity_type) as login_count, v.activity_type, si.age_band
    FROM studentassessment sa 
    join studentvle sv
    on sa.id_student = sv.id_student
    join vle v
    on v.id_site = sv.id_site
    join studentinfo si
    on si.id_student = sa.id_student
    where (sa.id_student, sa.date_submitted) 
    in (select id_student, max(date_submitted)
    from studentassessment group by id_student)
    and sa.date_submitted = sv.date
    group by v.activity_type,si.age_band""")

FinalResultBarQuery = text("""SELECT count(v.activity_type) as login_count, v.activity_type, si.final_result
    FROM studentassessment sa 
    join studentvle sv
    on sa.id_student = sv.id_student
    join vle v
    on v.id_site = sv.id_site
    join studentinfo si
    on si.id_student = sa.id_student
    where (sa.id_student, sa.date_submitted) 
    in (select id_student, max(date_submitted)
    from studentassessment group by id_student)
    and sa.date_submitted = sv.date
    group by v.activity_type,si.final_result""")

HighestEduBarQuery = text("""SELECT count(v.activity_type) as login_count, v.activity_type, si.highest_education
    FROM studentassessment sa 
    join studentvle sv
    on sa.id_student = sv.id_student
    join vle v
    on v.id_site = sv.id_site
    join studentinfo si
    on si.id_student = sa.id_student
    where (sa.id_student, sa.date_submitted) 
    in (select id_student, max(date_submitted)
    from studentassessment group by id_student)
    and sa.date_submitted = sv.date
    group by v.activity_type,si.highest_education""")

GenderScatterQuery = text("""SELECT sa.id_student, sa.score,sum(sv.sum_click) as totalClick,si.gender
    FROM studentassessment sa 
    join studentvle sv
    on sa.id_student = sv.id_student
    join studentinfo si
    on si.id_student = sa.id_student
    where (sa.id_student, sa.date_submitted) 
    in (select id_student, max(date_submitted)
    from studentassessment group by id_student)
    and sa.date_submitted = sv.date
    group by sa.id_student, sa.score,si.gender""")

RegionScatterQuery = text("""SELECT sa.id_student, sa.score,sum(sv.sum_click) as totalClick,si.region
    FROM studentassessment sa 
    join studentvle sv
    on sa.id_student = sv.id_student
    join studentinfo si
    on si.id_student = sa.id_student
    where (sa.id_student, sa.date_submitted) 
    in (select id_student, max(date_submitted)
    from studentassessment group by id_student)
    and sa.date_submitted = sv.date
    group by sa.id_student, sa.score,si.region""")

AgeBandScatterQuery = text("""SELECT sa.id_student, sa.score,sum(sv.sum_click) as totalClick,si.age_band
    FROM studentassessment sa 
    join studentvle sv
    on sa.id_student = sv.id_student
    join studentinfo si
    on si.id_student = sa.id_student
    where (sa.id_student, sa.date_submitted) 
    in (select id_student, max(date_submitted)
    from studentassessment group by id_student)
    and sa.date_submitted = sv.date
    group by sa.id_student, sa.score,si.age_band""")

FinalResultScatterQuery = text("""SELECT sa.id_student, sa.score,sum(sv.sum_click) as totalClick,si.final_result
    FROM studentassessment sa 
    join studentvle sv
    on sa.id_student = sv.id_student
    join studentinfo si
    on si.id_student = sa.id_student
    where (sa.id_student, sa.date_submitted) 
    in (select id_student, max(date_submitted)
    from studentassessment group by id_student)
    and sa.date_submitted = sv.date
    group by sa.id_student, sa.score,si.final_result""")

HighestEduScatterQuery = text("""SELECT sa.id_student, sa.score,sum(sv.sum_click) as totalClick,si.highest_education
    FROM studentassessment sa 
    join studentvle sv
    on sa.id_student = sv.id_student
    join studentinfo si
    on si.id_student = sa.id_student
    where (sa.id_student, sa.date_submitted) 
    in (select id_student, max(date_submitted)
    from studentassessment group by id_student)
    and sa.date_submitted = sv.date
    group by sa.id_student, sa.score,si.highest_education""")

dfPieChart = pd.read_sql(pieChartQuery, dbConnection)
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

# Init Data
student_score_data = StudentScoreTransformer.StudentScoreTransformer().read_table_and_transform(spark, jdbcUrl, connectionProperties)
training_data = StudentInfoMLTransformer.StudentInfoMLTransformer().read_table_and_transform(spark, jdbcUrl, connectionProperties)

# Init Model Training
indexer_str = "_idx"
indexing_feature_cols = ["gender", "highest_education", "imd_band", "age_band", "disability"]
non_indexing_feature_cols = ["total_click"]

model_dic = {}
for code in code_modules:
    model = train_data(training_data, code, indexing_feature_cols, non_indexing_feature_cols, indexer_str)
    model_dic[code] = model
    print("Loaded ML model for " + code + " : " + model.__str__())

# Init App
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
Callbacks.register_callbacks(app, student_score_data, model_dic, final_result_options, result_style)
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
        ]),
        html.H2("Filter Demographics of Students in 1st Attempt By :"),
        html.Div([
            dcc.Dropdown(
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
                style={"width": "50%"}
            ),
        ]),
        html.Div([
            dcc.Graph(id='pieChart' )
        ]),
        html.Div([
            dcc.Graph(id='histogram')
        ]),
        html.Div([
            dcc.Graph(id='barChart')
        ]),
        html.Div([
            dcc.Graph(id='scatterChart')
        ])
    ]
)

#---------------------------------------------------------------
@app.callback(
    Output(component_id='pieChart', component_property='figure'),
    [Input(component_id='my_dropdown', component_property='value')]
)

def update_pie(my_dropdown):

    fig_piechart = px.pie(
            data_frame=dfPieChart,
            names=my_dropdown,
            hole=.3,
            labels={
                "gender": "Gender",
                "age_band": "Age Band",
                "highest_education": "Highest Education",
                "final_result": "Final Result",
                "region": "Region"
            },
            ).update_traces(textposition='inside', textinfo='percent+label')
    return (fig_piechart)

@app.callback(Output('histogram', 'figure'),
              Input('my_dropdown', 'value'))

def update_line_category(my_dropdown):

    if my_dropdown == "region":
        df = dfRegionLine
        title =  "Gender of Students By Region"
        label = "Region"
        category_list= "[{'label': 'Gender', 'value': 'gender'},{'label': 'Region', 'value': 'region'},{'label': 'Highest Education', 'value': 'highest_education'},{'label': 'Age Band', 'value': 'age_band'},{'label': 'Final Result', 'value': 'final_result'}]"

    elif my_dropdown == "age_band":
        df = dfAgeBandLine
        title = "Gender of Students By Age Band (Years)"
        label = "Age Band"

    elif my_dropdown == "highest_education":
        df = dfHighestEduLine
        title = "Gender of Students By Highest Education"
        label = "Highest Education"

    elif my_dropdown == "final_result":
        df = dfFinalResultLine
        title = "Gender of Students By Final Result"
        label = "Final Result"
    else:
        df = dfGenderLine
        title = "No of Students By Gender"
        label = "Gender"

    fig_line= px.line(df,
        x=my_dropdown,
        y="count",
        color='gender',
        height=600,
        title=title,
        labels={
            "count": "Total Count",
            "gender": "Gender",
            "age_band": "Age Band",
            "highest_education": "Highest Education",
            "final_result": "Final Result",
            "region": "Region"
         },
        markers = True,
        text = "count"
        ).update_layout(title_font_size=title_font_size)\
        .update_traces(textposition="top right")
    return fig_line

@app.callback(
    Output(component_id='barChart', component_property='figure'),
    Input(component_id='my_dropdown', component_property='value'))

def update_bar(my_dropdown):

    if my_dropdown == "region":
        df = dfRegionBar
        title = "No of Students By Region"
        label = "Region"

    elif my_dropdown == "age_band":
        df = dfAgeBandBar
        title = "No of Students By Age Band (Years)"
        label = "Age Band"

    elif my_dropdown == "highest_education":
        df = dfHighestEduBar
        title = "No of Students By Highest Education"
        label = "Highest Education"

    elif my_dropdown == "final_result":
        df = dfFinalResultBar
        title = "No of Students By Final Result"
        label = "Final Result"

    else:
        df = dfGenderBar
        title = "No of Students By Gender"
        label = "gender"

    fig_bar = px.bar(df,
    x='activity_type',
    y='login_count',
    hover_data=['login_count'],
    text_auto=True,
    height=600,
    labels={
        "activity_type": "Activity Type",
        "login_count": "No of Student Logins",
        "gender": "Gender",
        "age_band": "Age Band",
        "highest_education": "Highest Education",
        "final_result": "Final Result",
        "region": "Region"
    },
    color=my_dropdown,
    title="Relation of Login Count to Activity Type Performed in 1st Assessment Attempt by " + label
    ).update_layout(title_font_size=title_font_size)

    return fig_bar

@app.callback(
    Output(component_id='scatterChart', component_property='figure'),
    Input(component_id='my_dropdown', component_property='value'))

def update_scatter(my_dropdown):

    if my_dropdown == "region":
        df = dfRegionScatter
        title = "No of Students By Region"
        label = "Region"

    elif my_dropdown == "age_band":
        df = dfAgeBandScatter
        title = "No of Students By Age Band (Years)"
        label = "Age Band"

    elif my_dropdown == "highest_education":
        df = dfHighestEduScatter
        title = "No of Students By Highest Education"
        label = "Highest Education"

    elif my_dropdown == "final_result":
        df = dfFinalResultScatter
        title = "No of Students By Final Result"
        label = "Final Result"

    else:
        df = dfGenderScatter
        title = "No of Students By Gender"
        label = "Gender"

    fig_scatter = px.scatter(df,
        x="totalclick",
        y="score",
        size="totalclick",
        color=my_dropdown,
        hover_name=my_dropdown,
        labels={
            "totalclick": "Total No. of Clicks",
            "score": "Total Score",
            "gender": "Gender",
            "age_band": "Age Band",
            "highest_education": "Highest Education",
            "final_result": "Final Result",
            "region": "Region"
        },
        log_x=True,
        size_max=60,
        title="Relation of Total Click to Student Score obtained in 1st Assessment Attempt By " + label
        ).update_layout(title_font_size=title_font_size)

    return fig_scatter

if __name__ == "__main__":
    app.run_server(debug=True)
