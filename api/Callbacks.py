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


    @app.callback(Output('predicted_result', 'children'),
                  Input('submit-button-prediction', 'n_clicks'),
                  Input('course_to_predict', 'value'),
                  Input('gender', 'value'),
                  Input('highest_education', 'value'),
                  Input('imd_band', 'value'),
                  Input('age_band', 'value'),
                  Input('disability', 'value'),)
    def update_prediction(n_clicks, course_to_predict, gender, highest_education, imd_band, age_band, disability):
        return str(course_to_predict) + str(gender) + str(highest_education) + str(imd_band) + str(age_band) + str(disability)