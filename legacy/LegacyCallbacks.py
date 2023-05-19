from dash.dependencies import Input, Output
from config.AppProperties import title_font_size
import plotly.express as px

def register_callbacks(app, dfPie,
                       dfRegionLine, dfAgeBandLine, dfHighestEduLine, dfFinalResultLine, dfGenderLine,
                       dfRegionBar, dfAgeBandBar, dfHighestEduBar, dfFinalResultBar, dfGenderBar,
                       dfRegionScatter, dfAgeBandScatter, dfHighestEduScatter, dfFinalResultScatter, dfGenderScatter):

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

    @app.callback(
        Output(component_id='pieChart2', component_property='figure'),
        [Input(component_id='my_dropdown', component_property='value')],
        [Input(component_id='my_dropdown2', component_property='value')],
    )
    def update_pie2(my_dropdown, my_dropdown2, dfPie=dfPie):
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

        return fig_piechart2

    @app.callback(
        Output(component_id='sunBurst2', component_property='figure'),
        [Input(component_id='my_dropdown', component_property='value')]
    )
    def update_sunBurst2(my_dropdown, dfPie=dfPie):
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
        return fig_update_sunBurst2

    @app.callback(Output('histogram', 'figure'),
                  Input('my_dropdown', 'value'),
                  Input('my_dropdown2', 'value'))
    def update_line_category(my_dropdown, my_dropdown2,
                             dfRegionLine=dfRegionLine,
                             dfAgeBandLine=dfAgeBandLine,
                             dfHighestEduLine=dfHighestEduLine,
                             dfFinalResultLine=dfFinalResultLine,
                             dfGenderLine=dfGenderLine):
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

    def update_bar(my_dropdown, my_dropdown2,
                   dfRegionBar=dfRegionBar,
                   dfAgeBandBar=dfAgeBandBar,
                   dfHighestEduBar=dfHighestEduBar,
                   dfFinalResultBar=dfFinalResultBar,
                   dfGenderBar=dfGenderBar):
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
        Output(component_id='scatterChartOverView', component_property='figure'),
        Input(component_id='my_dropdown', component_property='value'),
        Input(component_id='my_dropdown2', component_property='value'))
    def update_scatterOverView(my_dropdown, my_dropdown2,
                               dfRegionScatter=dfRegionScatter,
                               dfAgeBandScatter=dfAgeBandScatter,
                               dfHighestEduScatter=dfHighestEduScatter,
                               dfFinalResultScatter=dfFinalResultScatter,
                               dfGenderScatter=dfGenderScatter):
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
                                 ) \
            .update_layout(title_font_size=title_font_size)

        return fig_scatter

    @app.callback(
        Output(component_id='scatterChart', component_property='figure'),
        Input(component_id='my_dropdown', component_property='value'),
        Input(component_id='my_dropdown2', component_property='value'))

    def update_scatter(my_dropdown, my_dropdown2,
                       dfRegionScatter=dfRegionScatter,
                       dfAgeBandScatter=dfAgeBandScatter,
                       dfHighestEduScatter=dfHighestEduScatter,
                       dfFinalResultScatter=dfFinalResultScatter,
                       dfGenderScatter=dfGenderScatter):
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
                                 ) \
            .update_layout(title_font_size=title_font_size).update_layout(showlegend=False)

        for anno in fig_scatter['layout']['annotations']:
            anno['text'] = ''

        return fig_scatter