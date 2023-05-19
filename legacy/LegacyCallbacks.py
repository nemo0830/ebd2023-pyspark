from dash.dependencies import Input, Output
import plotly.express as px

def register_callbacks(app, dfPie):

    @app.callback(
        Output(component_id='pieChart', component_property='figure'),
        [Input(component_id='my_dropdown', component_property='value')],
        [Input(component_id='my_dropdown2', component_property='value')],
    )
    def update_pie(my_dropdown, my_dropdown2, dfPie=dfPie):
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
        return fig_piechart

    @app.callback(
        Output(component_id='sunBurst', component_property='figure'),
        [Input(component_id='my_dropdown', component_property='value')]
    )
    def update_sunBurst(my_dropdown, dfPie=dfPie):
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
        return fig_update_sunBurst