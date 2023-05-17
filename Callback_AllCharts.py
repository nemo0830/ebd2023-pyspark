import pandas as pd
from sqlalchemy import create_engine, text
from google.cloud.sql.connector import Connector
import dash_bootstrap_components as dbc
import plotly.express as px
from dash import dcc, html, dash
from dash.dependencies import Input, Output

db_user = "postgres"
db_pass = "ebd2023"
db_name = "golddb"

# initialize Connector object
connector = Connector()

# function to return the database connection object
def getconn():
	conn = connector.connect(
		'gifted-loader-384715:asia-southeast1:ebd2023',
		"pg8000",
		user=db_user,
		password=db_pass,
		db=db_name
	)
	return conn

# create connection pool with 'creator' argument to our connection object function
alchemyEngine = create_engine(
    "postgresql+pg8000://",
    creator=getconn,
)

#local host connection
#alchemyEngine = create_engine("postgresql+psycopg2://postgres:Ruiyun@localhost:5432/postgres")

dbConnection = alchemyEngine.connect()

pieQuery = text("""SELECT a.assessment_type, a.weight, si.code_module,count(si.id_student) as count,
si.region,si.gender,si.final_result, si.age_band,si.code_presentation,si.highest_education,
        CASE 
           WHEN si.code_presentation LIKE '%J' THEN Concat('Feb ',left(si.code_presentation,4))
           WHEN si.code_presentation LIKE '%B' THEN Concat('Oct ',left(si.code_presentation,4))
        END code_presentation_description,
        CASE a.assessment_type
           WHEN 'TMA' THEN 'Tutor Marked Assessment (TMA)'
           WHEN 'CMA' THEN 'Computer Marked Assessment (CMA)'
        END assessment_type_description
        FROM studentassessment sa join studentvle sv
        on sa.id_student = sv.id_student
	    join studentinfo si
	    on si.id_student = sa.id_student
	    join assessments a
		on sa.id_assessment = a.id_assessment
        where (sa.id_student, sa.date_submitted)
        in (select id_student, max(date_submitted)
        from studentassessment group by id_student)
        and sa.date_submitted = sv.date
		group by si.code_module, si.code_presentation,a.assessment_type, a.weight,si.region,
		si.age_band,si.highest_education,si.final_result,si.gender""")

regionLineQuery = text("""SELECT si.code_module,si.code_presentation, si.region,count(*) as count,
		CASE 
           WHEN si.code_presentation LIKE '%J' THEN Concat('Feb ',left(si.code_presentation,4))
           WHEN si.code_presentation LIKE '%B' THEN Concat('Oct ',left(si.code_presentation,4))
        END code_presentation_description
    FROM studentassessment sa join studentvle sv
    on sa.id_student = sv.id_student
	join studentinfo si
	on si.id_student = sa.id_student
	join assessments a
	on sa.id_assessment = a.id_assessment
    where (sa.id_student, sa.date_submitted) 
    in (select id_student, max(date_submitted)
    from studentassessment group by id_student)
    and sa.date_submitted = sv.date
	group by si.region,si.code_module,si.code_presentation""")

ageBandLineQuery = text("""SELECT si.code_presentation,si.code_module,count(*) as count,si.age_band,
		CASE 
           WHEN si.code_presentation LIKE '%J' THEN Concat('Feb ',left(si.code_presentation,4))
           WHEN si.code_presentation LIKE '%B' THEN Concat('Oct ',left(si.code_presentation,4))
        END code_presentation_description
    FROM studentassessment sa join studentvle sv
    on sa.id_student = sv.id_student
	join studentinfo si
	on si.id_student = sa.id_student
	join assessments a
	on sa.id_assessment = a.id_assessment
    where (sa.id_student, sa.date_submitted) 
    in (select id_student, max(date_submitted)
    from studentassessment group by id_student)
    and sa.date_submitted = sv.date
	group by si.age_band,si.code_module, si.code_presentation""")

HighestEducationLineQuery = text("""SELECT si.code_presentation,si.highest_education,count(*) as count,si.code_module, 
		CASE 
           WHEN si.code_presentation LIKE '%J' THEN Concat('Feb ',left(si.code_presentation,4))
           WHEN si.code_presentation LIKE '%B' THEN Concat('Oct ',left(si.code_presentation,4))
        END code_presentation_description
FROM studentassessment sa join studentvle sv
on sa.id_student = sv.id_student
join studentinfo si
on si.id_student = sa.id_student
join assessments a
on sa.id_assessment = a.id_assessment
where (sa.id_student, sa.date_submitted) 
in (select id_student, max(date_submitted)
from studentassessment group by id_student)
and sa.date_submitted = sv.date
group by si.highest_education,si.code_module,si.code_presentation""")

finalResultLineQuery = text("""SELECT si.code_presentation,si.final_result,count(*) as count,si.code_module, 
		CASE 
           WHEN si.code_presentation LIKE '%J' THEN Concat('Feb ',left(si.code_presentation,4))
           WHEN si.code_presentation LIKE '%B' THEN Concat('Oct ',left(si.code_presentation,4))
        END code_presentation_description
FROM studentassessment sa join studentvle sv
on sa.id_student = sv.id_student
join studentinfo si
on si.id_student = sa.id_student
join assessments a
on sa.id_assessment = a.id_assessment
where (sa.id_student, sa.date_submitted) 
in (select id_student, max(date_submitted)
from studentassessment group by id_student)
and sa.date_submitted = sv.date
group by si.final_result,si.code_module, si.code_presentation""")

genderLineQuery = text("""SELECT si.code_presentation,si.gender,count(*) as count,si.code_module, 
		CASE 
           WHEN si.code_presentation LIKE '%J' THEN Concat('Feb ',left(si.code_presentation,4))
           WHEN si.code_presentation LIKE '%B' THEN Concat('Oct ',left(si.code_presentation,4))
        END code_presentation_description
FROM studentassessment sa join studentvle sv
on sa.id_student = sv.id_student
join studentinfo si
on si.id_student = sa.id_student
join assessments a
on sa.id_assessment = a.id_assessment
where (sa.id_student, sa.date_submitted) 
in (select id_student, max(date_submitted)
from studentassessment group by id_student)
and sa.date_submitted = sv.date
group by si.gender,si.code_module, si.code_presentation""")

GenderBarQuery = text("""SELECT si.code_presentation, count(v.activity_type) as login_count, v.activity_type, si.gender,
	CASE 
        WHEN si.code_presentation LIKE '%J' THEN Concat('Feb ',left(si.code_presentation,4))
        WHEN si.code_presentation LIKE '%B' THEN Concat('Oct ',left(si.code_presentation,4))
    END code_presentation_description
    FROM studentassessment sa 
	join studentvle sv
    on sa.id_student = sv.id_student
	join vle v
	on v.id_site = sv.id_site
	join studentinfo si
	on si.id_student = sa.id_student
	join assessments a
	on sa.id_assessment = a.id_assessment
    where (sa.id_student, sa.date_submitted) 
    in (select id_student, max(date_submitted)
    from studentassessment group by id_student)
    and sa.date_submitted = sv.date
	group by si.code_presentation,v.activity_type,si.gender""")

RegionBarQuery = text("""SELECT si.code_presentation, count(v.activity_type) as login_count, v.activity_type, si.region,
	CASE 
        WHEN si.code_presentation LIKE '%J' THEN Concat('Feb ',left(si.code_presentation,4))
        WHEN si.code_presentation LIKE '%B' THEN Concat('Oct ',left(si.code_presentation,4))
    END code_presentation_description
    FROM studentassessment sa 
	join studentvle sv
    on sa.id_student = sv.id_student
	join vle v
	on v.id_site = sv.id_site
	join studentinfo si
	on si.id_student = sa.id_student
	join assessments a
	on sa.id_assessment = a.id_assessment
    where (sa.id_student, sa.date_submitted) 
    in (select id_student, max(date_submitted)
    from studentassessment group by id_student)
    and sa.date_submitted = sv.date
	group by si.code_presentation,v.activity_type,si.region""")

AgeBandBarQuery = text("""SELECT si.code_presentation,count(v.activity_type) as login_count, v.activity_type, si.age_band,
	CASE 
        WHEN si.code_presentation LIKE '%J' THEN Concat('Feb ',left(si.code_presentation,4))
        WHEN si.code_presentation LIKE '%B' THEN Concat('Oct ',left(si.code_presentation,4))
    END code_presentation_description
    FROM studentassessment sa 
	join studentvle sv
    on sa.id_student = sv.id_student
	join vle v
	on v.id_site = sv.id_site
	join studentinfo si
	on si.id_student = sa.id_student
	join assessments a
	on sa.id_assessment = a.id_assessment
    where (sa.id_student, sa.date_submitted) 
    in (select id_student, max(date_submitted)
    from studentassessment group by id_student)
    and sa.date_submitted = sv.date
	group by si.code_presentation,v.activity_type,si.age_band""")

FinalResultBarQuery = text("""SELECT si.code_presentation,count(v.activity_type) as login_count, v.activity_type, si.final_result,
	CASE 
        WHEN si.code_presentation LIKE '%J' THEN Concat('Feb ',left(si.code_presentation,4))
        WHEN si.code_presentation LIKE '%B' THEN Concat('Oct ',left(si.code_presentation,4))
    END code_presentation_description
    FROM studentassessment sa 
	join studentvle sv
    on sa.id_student = sv.id_student
	join vle v
	on v.id_site = sv.id_site
	join studentinfo si
	on si.id_student = sa.id_student
	join assessments a
	on sa.id_assessment = a.id_assessment
    where (sa.id_student, sa.date_submitted) 
    in (select id_student, max(date_submitted)
    from studentassessment group by id_student)
    and sa.date_submitted = sv.date
	group by si.code_presentation,v.activity_type,si.final_result""")

HighestEduBarQuery = text("""SELECT si.code_presentation,count(v.activity_type) as login_count, v.activity_type, si.highest_education,
	CASE 
        WHEN si.code_presentation LIKE '%J' THEN Concat('Feb ',left(si.code_presentation,4))
        WHEN si.code_presentation LIKE '%B' THEN Concat('Oct ',left(si.code_presentation,4))
    END code_presentation_description
    FROM studentassessment sa 
	join studentvle sv
    on sa.id_student = sv.id_student
	join vle v
	on v.id_site = sv.id_site
	join assessments a
	on sa.id_assessment = a.id_assessment
	join studentinfo si
	on si.id_student = sa.id_student
    where (sa.id_student, sa.date_submitted) 
    in (select id_student, max(date_submitted)
    from studentassessment group by id_student)
    and sa.date_submitted = sv.date
	group by si.code_presentation,v.activity_type,si.highest_education""")

GenderScatterQuery = text("""SELECT si.code_presentation, count(*), sa.score,sum(sv.sum_click) as totalClick,si.gender,
	CASE 
        WHEN si.code_presentation LIKE '%J' THEN Concat('Feb ',left(si.code_presentation,4))
        WHEN si.code_presentation LIKE '%B' THEN Concat('Oct ',left(si.code_presentation,4))
    END code_presentation_description
    FROM studentassessment sa 
	join studentvle sv
    on sa.id_student = sv.id_student
    join assessments a
	on sa.id_assessment = a.id_assessment
	join studentinfo si
	on si.id_student = sa.id_student
    where (sa.id_student, sa.date_submitted) 
    in (select id_student, max(date_submitted)
    from studentassessment group by id_student)
    and sa.date_submitted = sv.date
	group by si.code_presentation, sa.score,si.gender""")

RegionScatterQuery = text("""SELECT si.code_presentation, count(*), sa.score,sum(sv.sum_click) as totalClick,si.region,
	CASE 
        WHEN si.code_presentation LIKE '%J' THEN Concat('Feb ',left(si.code_presentation,4))
        WHEN si.code_presentation LIKE '%B' THEN Concat('Oct ',left(si.code_presentation,4))
    END code_presentation_description
    FROM studentassessment sa 
	join studentvle sv
    on sa.id_student = sv.id_student
    join assessments a
	on sa.id_assessment = a.id_assessment
	join studentinfo si
	on si.id_student = sa.id_student
    where (sa.id_student, sa.date_submitted) 
    in (select id_student, max(date_submitted)
    from studentassessment group by id_student)
    and sa.date_submitted = sv.date
	group by si.code_presentation, sa.score,si.region""")

AgeBandScatterQuery = text("""SELECT si.code_presentation, count(*), sa.score,sum(sv.sum_click) as totalClick,si.age_band,
	CASE 
        WHEN si.code_presentation LIKE '%J' THEN Concat('Feb ',left(si.code_presentation,4))
        WHEN si.code_presentation LIKE '%B' THEN Concat('Oct ',left(si.code_presentation,4))
    END code_presentation_description
    FROM studentassessment sa 
	join studentvle sv
    on sa.id_student = sv.id_student
    join assessments a
	on sa.id_assessment = a.id_assessment
	join studentinfo si
	on si.id_student = sa.id_student
    where (sa.id_student, sa.date_submitted) 
    in (select id_student, max(date_submitted)
    from studentassessment group by id_student)
    and sa.date_submitted = sv.date
	group by si.code_presentation, sa.score,si.age_band""")

FinalResultScatterQuery = text("""SELECT si.code_presentation, count(*), sa.score,sum(sv.sum_click) as totalClick,si.final_result,
	CASE 
        WHEN si.code_presentation LIKE '%J' THEN Concat('Feb ',left(si.code_presentation,4))
        WHEN si.code_presentation LIKE '%B' THEN Concat('Oct ',left(si.code_presentation,4))
    END code_presentation_description
    FROM studentassessment sa 
	join studentvle sv
    on sa.id_student = sv.id_student
    join assessments a
	on sa.id_assessment = a.id_assessment
	join studentinfo si
	on si.id_student = sa.id_student
    where (sa.id_student, sa.date_submitted) 
    in (select id_student, max(date_submitted)
    from studentassessment group by id_student)
    and sa.date_submitted = sv.date
	group by si.code_presentation, sa.score,si.final_result""")

HighestEduScatterQuery = text("""SELECT si.code_presentation, count(*), sa.score,sum(sv.sum_click) as totalClick,si.highest_education,
	CASE 
        WHEN si.code_presentation LIKE '%J' THEN Concat('Feb ',left(si.code_presentation,4))
        WHEN si.code_presentation LIKE '%B' THEN Concat('Oct ',left(si.code_presentation,4))
    END code_presentation_description
    FROM studentassessment sa 
	join studentvle sv
    on sa.id_student = sv.id_student
	join studentinfo si
	on si.id_student = sa.id_student
	join assessments a
	on sa.id_assessment = a.id_assessment
    where (sa.id_student, sa.date_submitted) 
    in (select id_student, max(date_submitted)
    from studentassessment group by id_student)
    and sa.date_submitted = sv.date
	group by si.code_presentation, sa.score,si.highest_education""")

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

# you need to include __name__ in your Dash constructor if
# you plan to use a custom CSS or JavaScript in your Dash apps
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

title_font_size = 15

# ---------------------------------------------------------------
app.layout = dbc.Container([
	dbc.Row([
		dbc.Col(html.Label(['Filter By Student Demographics :']), width=3),
		dbc.Col(html.Label(['Filter By Course Semester :']), width=3)
	], style={'position':'sticky','top':0}),
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
	]))
])

# ---------------------------------------------------------------
@app.callback(
	Output(component_id='pieChart', component_property='figure'),
	[Input(component_id='my_dropdown', component_property='value')],
	[Input(component_id='my_dropdown2', component_property='value')],
)
def update_pie(my_dropdown, my_dropdown2):
	df = dfPie.loc[dfPie['code_presentation_description'] == my_dropdown2]

	fig_piechart = px.pie(
		data_frame=df,
		names=my_dropdown,
		hole=.3,
		labels={
			"gender": "Gender",
			"age_band": "Age Band",
			"highest_education": "Highest Education",
			"final_result": "Final Result",
			"region": "Region"
		},
		height=550,
		width=550,
		title="Students Composition By <b>" + my_dropdown + "</b><br>for Course Semester <b>" + my_dropdown2 + "</b>"
	).update_traces(textposition='inside', textinfo='percent+label')
	return (fig_piechart)

# ---------------------------------------------------------------
@app.callback(
	Output(component_id='sunBurst', component_property='figure'),
	[Input(component_id='my_dropdown', component_property='value')]
)

def update_sunBurst(my_dropdown):

	fig_update_sunBurst = px.sunburst(
		data_frame=dfPie,
		names=my_dropdown,
		labels={
			"gender": "Gender",
			"age_band": "Age Band",
			"highest_education": "Highest Education",
			"final_result": "Final Result",
			"region": "Region"
		},
		height=500,
		width=500,
		path=['code_presentation_description', my_dropdown],
		values='count',
		title="Students Composition Overview<br>By <b>" + my_dropdown + "</b>"
	)
	return (fig_update_sunBurst)

@app.callback(
	Output(component_id='pieChart2', component_property='figure'),
	[Input(component_id='my_dropdown', component_property='value')],
	[Input(component_id='my_dropdown2', component_property='value')],
)
def update_pie2(my_dropdown, my_dropdown2):
	df = dfPie.loc[dfPie['code_presentation_description'] == my_dropdown2].dropna()

	fig_piechart2 = px.sunburst(
		data_frame=df,
		names=my_dropdown,
		labels={
			"gender": "Gender",
			"age_band": "Age Band",
			"highest_education": "Highest Education",
			"final_result": "Final Result",
			"region": "Region"
		},
		height=400,
		width=400,
		path=['code_module', 'code_presentation_description', 'assessment_type','weight'],
		values='count',
		title="Course Module Composition for Course Semester <b>" + my_dropdown2 + "</b>",
		color='code_presentation_description')

	return (fig_piechart2)

# ---------------------------------------------------------------
@app.callback(
	Output(component_id='sunBurst2', component_property='figure'),
	[Input(component_id='my_dropdown', component_property='value')]
)

def update_sunBurst2(my_dropdown):
	df = dfPie.dropna()
	fig_update_sunBurst2 = px.sunburst(
		data_frame=df,
		names=my_dropdown,
		height=650,
		width=650,
		path=['code_module','code_presentation_description','assessment_type_description','weight'],
		values='count',
		title="Course Module Composition Overview",
		color = 'code_presentation_description'
	)
	return (fig_update_sunBurst2)

@app.callback(Output('histogram', 'figure'),
              Input('my_dropdown', 'value'),
              Input('my_dropdown2', 'value'))

def update_line_category(my_dropdown, my_dropdown2):
	if my_dropdown == "region":
		df = dfRegionLine.loc[dfRegionLine['code_presentation_description'] == my_dropdown2]
		title = "No of Students By <b>Region</b> for Course Semester <b>" + my_dropdown2 + "</b>"
	elif my_dropdown == "age_band":
		df = dfAgeBandLine.loc[dfAgeBandLine['code_presentation_description'] == my_dropdown2]
		title = "No of Students By <b>Age Band (Years)</b> for Course Semester <b>" + my_dropdown2 + "</b>"
	elif my_dropdown == "highest_education":
		df = dfHighestEduLine.loc[dfHighestEduLine['code_presentation_description'] == my_dropdown2]
		title = "No of Students By <b>Highest Education</b> for Course Semester <b>" + my_dropdown2 + "</b>"
	elif my_dropdown == "final_result":
		df = dfFinalResultLine.loc[dfFinalResultLine['code_presentation_description'] == my_dropdown2]
		title = "No of Students By <b>Final Result</b> for Course Semester <b>" + my_dropdown2 + "</b>"
	else:
		df = dfGenderLine.loc[dfGenderLine['code_presentation_description'] == my_dropdown2]
		title = "No of Students By <b>Gender</b> for Course Semester <b>" + my_dropdown2 + "</b>"

	fig_line = px.line(df,
	    y='count',
	    x=my_dropdown,
		color='code_module',
	    height=600,
	    title=title,
	    labels={
		    "count": "Total Count",
		    "gender": "Gender",
		    "age_band": "Age Band",
		    "highest_education": "Highest Education",
		    "final_result": "Final Result",
		    "region": "Region",
		    "code_presentation": "Course Semester",
		    "assessment_type_description": "Assessment Type",
	    },
		markers = 'count',
	    text = 'count',
	    ).update_layout(title_font_size=title_font_size) \
		.update_traces(textposition="top right")
	return fig_line

@app.callback(
	Output(component_id='barChart', component_property='figure'),
	Input(component_id='my_dropdown', component_property='value'),
	Input(component_id='my_dropdown2', component_property='value'))

def update_bar(my_dropdown, my_dropdown2):
	if my_dropdown == "region":
		df = dfRegionBar.loc[dfRegionBar['code_presentation_description'] == my_dropdown2]
		label = "Region"
	elif my_dropdown == "age_band":
		df = dfAgeBandBar.loc[dfAgeBandBar['code_presentation_description'] == my_dropdown2]
		label = "Age Band"
	elif my_dropdown == "highest_education":
		df = dfHighestEduBar.loc[dfHighestEduBar['code_presentation_description'] == my_dropdown2]
		label = "Highest Education"
	elif my_dropdown == "final_result":
		df = dfFinalResultBar.loc[dfFinalResultBar['code_presentation_description'] == my_dropdown2]
		label = "Final Result"
	else:
		df = dfGenderBar.loc[dfGenderBar['code_presentation_description'] == my_dropdown2]
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
	    title="Relation of Login Counts to <b>" + label + "</b> in 1st Assessment Attempt <br>for Course Semester <b>" + my_dropdown2 + "</b>"
	    ).update_layout(title_font_size=title_font_size).update_layout(barmode='stack', xaxis={'categoryorder':'total descending'})
	return fig_bar

@app.callback(
	Output(component_id='scatterChart', component_property='figure'),
	Input(component_id='my_dropdown', component_property='value'),
	Input(component_id='my_dropdown2', component_property='value'))

def update_scatter(my_dropdown, my_dropdown2):
	if my_dropdown == "region":
		df = dfRegionScatter.loc[dfRegionScatter['code_presentation_description'] == my_dropdown2]
		label = "Region"
	elif my_dropdown == "age_band":
		df = dfAgeBandScatter.loc[dfAgeBandScatter['code_presentation_description'] == my_dropdown2]
		label = "Age Band"
	elif my_dropdown == "highest_education":
		df = dfHighestEduScatter.loc[dfHighestEduScatter['code_presentation_description'] == my_dropdown2]
		label = "Highest Education"
	elif my_dropdown == "final_result":
		df = dfFinalResultScatter.loc[dfFinalResultScatter['code_presentation_description'] == my_dropdown2]
		label = "Final Result"
	else:
		df = dfGenderScatter.loc[dfGenderScatter['code_presentation_description'] == my_dropdown2]
		label = "Gender"

	fig_scatter = px.scatter(df,
	    x="totalclick",
	    y="score",
	    size="count",
	    color=my_dropdown,
	    hover_name=my_dropdown,
	    labels={
		"totalclick": "Clicks",
		"score": "Total Score",
		"gender": "Gender",
		"age_band": "Age Band",
		"highest_education": "Highest Education",
		"final_result": "Final Result",
		"region": "Region"
		},
	    facet_col=my_dropdown,
	    size_max=40,
	    title="Relation of Total Clicks to Student Scores obtained in 1st Assessment Attempt By <b>" + label + "</b><br>for Course Semester <b>" + my_dropdown2 + "</b>"
	    )\
		.update_layout(title_font_size=title_font_size).update_layout(showlegend=False)

	for anno in fig_scatter['layout']['annotations']:
		anno['text'] = ''

	return fig_scatter

@app.callback(
	Output(component_id='scatterChartOverView', component_property='figure'),
	Input(component_id='my_dropdown', component_property='value'),
	Input(component_id='my_dropdown2', component_property='value'))

def update_scatterOverView(my_dropdown, my_dropdown2):
	if my_dropdown == "region":
		df = dfRegionScatter.loc[dfRegionScatter['code_presentation_description'] == my_dropdown2]
		label = "Region"
	elif my_dropdown == "age_band":
		df = dfAgeBandScatter.loc[dfAgeBandScatter['code_presentation_description'] == my_dropdown2]
		label = "Age Band"
	elif my_dropdown == "highest_education":
		df = dfHighestEduScatter.loc[dfHighestEduScatter['code_presentation_description'] == my_dropdown2]
		label = "Highest Education"
	elif my_dropdown == "final_result":
		df = dfFinalResultScatter.loc[dfFinalResultScatter['code_presentation_description'] == my_dropdown2]
		label = "Final Result"
	else:
		df = dfGenderScatter.loc[dfGenderScatter['code_presentation_description'] == my_dropdown2]
		label = "Gender"

	fig_scatter = px.scatter(df,
	    x="totalclick",
	    y="score",
	    size="count",
	    color=my_dropdown,
	    hover_name=my_dropdown,
	    labels={
		"totalclick": "Total No. of Clicks",
		"score": "Total Score",
		"gender": "Gender",
		"age_band": "Age Band",
		"highest_education": "Highest Education",
		"final_result": "Final Result",
		"region": "Region",
		"code_presentation_description": "Course Semester"
		},
	    title="Overview Relation of Total Clicks to Student Scores obtained in 1st Assessment Attempt By <b>" + label + "</b><br>for Course Semester <b>" + my_dropdown2 + "</b>"
	    )\
		.update_layout(title_font_size=title_font_size)

	return fig_scatter

if __name__ == '__main__':
	app.run_server(debug=True)
