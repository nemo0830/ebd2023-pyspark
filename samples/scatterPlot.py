from google.cloud.sql.connector import Connector
from sqlalchemy import create_engine, text
import pandas as pd
import dash
from dash import html
from dash import dcc
import dash_bootstrap_components as dbc
import plotly.express as px

db_user = "postgres"
db_pass = "<replace_actual_password_here>"
db_name = "mockdb"
project_id = "ebd2023"
region = "asia-southeast1"
instance_name = "ebd2023"

#gcloud auth login

# initialize parameters
INSTANCE_CONNECTION_NAME = f"{project_id}:{region}:{instance_name}" # i.e demo-project:us-central1:demo-instance
print(f"Your instance connection name is: {INSTANCE_CONNECTION_NAME}")

# initialize Connector object
connector = Connector()

# function to return the database connection object
def getconn():
   conn = connector.connect(
       INSTANCE_CONNECTION_NAME,
       "pg8000",
       user=db_user,
       password=db_pass,
       db=db_name
   )
   return conn

# create connection pool with 'creator' argument to our connection object function
engine = create_engine(
   "postgresql+pg8000://",
   creator=getconn,
)

with engine.begin() as conn:

    query = text("""SELECT sa.id_student, sa.score,sum(sv.sum_click::INTEGER) as totalClick
    FROM student_assessment sa join student_vle sv
    on sa.id_student = sv.id_student
    where (sa.id_student, sa.date_submitted) in
    (select id_student, max(date_submitted)
    from student_assessment group by id_student)
    and sa.date_submitted = sv.date
	group by sa.id_student, sa.score""")

    query2 = text("""SELECT sa.id_student, count(v.activity_type) as login_count, v.activity_type
    FROM student_assessment sa 
	join student_vle sv
    on sa.id_student = sv.id_student
	join vle v
	on v.id_site = sv.id_site
    where (sa.id_student, sa.date_submitted) in
    (select id_student, max(date_submitted)
    from student_assessment group by id_student)
    and sa.date_submitted = sv.date
	group by sa.id_student, v.activity_type""")

    df = pd.read_sql_query(query,conn)
    df2 = pd.read_sql_query(query2, conn)

    # line creates the dashboard.
    app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
    title_font_size = 15
    fig = px.scatter(df, x="score", y="totalclick",
                     size="totalclick", color="totalclick", hover_name="totalclick",
                     log_x=True, size_max=60,
                     title="Relation of Total Clicks to Scores obtained by Students in 1st Assessment Attempt").update_layout(
        title_font_size=title_font_size)

    fig2 = px.bar(df2, x='activity_type', y='login_count',
                  hover_data=['id_student', 'login_count'], color='activity_type',
                  labels={'pop': ''}, height=450,
                  title="Relation of Login Counts to Activity Types in 1st Assessment Attempt").update_layout(
        title_font_size=title_font_size)
    # line defines the dashboard components and its layout with the help of HTML tags.
    # This is where you will add the components of the dashboard.
    app.layout = html.Div(
        [
            html.H1("Dashboard on Student Engagement in an e-Learning System"),
            html.H1("& Their Impact on Student Course Assessment Scores"),
            html.Ul(
                [
                    dcc.Graph(id='url-1st-attempt', figure=fig2),
                    dcc.Graph(id='score-1st-attempt', figure=fig)

                ]
            ),
        ]
    )

    if __name__ == "__main__":
        app.run_server(debug=True)