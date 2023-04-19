from dash.dependencies import Input, Output, State
from config.AppProperties import *
import plotly.express as px

def register_callbacks(app, student_score_data):

    @app.callback(
        Output('student-score-plot', 'figure'),
        Input('top_or_bottom', 'value'),
        Input('semester', 'value'),
        Input('submit-button', 'n_clicks'),
        State('input-param', 'value'),
    )
    def update_top_x_score(top_or_bottom, semester, n_clicks, input_param, df=student_score_data):
        df = df[df['code_presentation'] == semester]
        if top_or_bottom == "top":
            df = df.head(input_param)
        else:
            df = df.tail(input_param)
        fig = px.bar(df, x="id_student", y="weighted_score",
                     hover_data=['id_student', 'weighted_score'], color='weighted_score',
                     labels={'pop': ''}, height=450,
                     title=top_or_bottom + " " + str(input_param) + " students and their scores in semester " + semester) \
            .update_layout(title_font_size=title_font_size)
        return fig