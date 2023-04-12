from pyspark.sql import SparkSession
from config.AppProperties import *
from config.SparkConn import jdbcUrl, connectionProperties
from transformer import StudentScoreTransformer
import dash
from dash import html
from dash import dcc
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State
import plotly.express as px

## Init Spark
spark = SparkSession.builder \
    .appName("gcloud_sql") \
    .config("spark.jars", "../postgresql-42.6.0.jar") \
    .master("local").getOrCreate()

## Init Data
student_score_data = StudentScoreTransformer.StudentScoreTransformer().read_table_and_transform(spark, jdbcUrl, connectionProperties)

## Init App
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

@app.callback(
    Output('output-plot', 'figure'),
    Input('top_or_bottom', 'value'),
    Input('submit-button', 'n_clicks'),
    State('input-param', 'value'),
)
def update_top_x_score(top_or_bottom, n_clicks, input_param, df=student_score_data):
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
