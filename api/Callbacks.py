from dash.dependencies import Input, Output, State
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import col
import plotly.express as px
import os

def register_callbacks(app, student_score_data, model_dic, final_result_options, result_style):

    @app.callback(
        Output(component_id='student-score-plot', component_property='figure'),
        State(component_id='top_or_bottom', component_property='value'),
        State(component_id='semester', component_property='value'),
        Input(component_id='submit-button', component_property='n_clicks'),
        State(component_id='input-param', component_property='value'),
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
            .update_layout(title_font_size=30)
        return fig


    @app.callback(Output(component_id='predicted_result', component_property='children'),
                  Output(component_id='predicted_result', component_property='style'),
                  Input(component_id='submit-button-prediction', component_property='n_clicks'),
                  State(component_id='course_to_predict', component_property='value'),
                  State(component_id='gender', component_property='value'),
                  State(component_id='highest_education', component_property='value'),
                  State(component_id='imd_band', component_property='value'),
                  State(component_id='age_band', component_property='value'),
                  State(component_id='disability', component_property='value'),
                  State(component_id='total_clicks', component_property='value')
  )
    def update_prediction(n_clicks, course_to_predict, gender, highest_education, imd_band, age_band, disability, total_clicks, ml_map=model_dic, result_map=final_result_options, result_style_map=result_style):
        os.environ["PYSPARK_PYTHON"] = "C:Users\siyuan\AppData\Local\Programs\Python\Python38\python.exe"
        if n_clicks > 0:
            model = ml_map[course_to_predict]
            feature_cols = ["gender_idx", "highest_education_idx", "imd_band_idx", "age_band_idx", "disability_idx", "total_click"]
            spark_local = SparkSession.builder.appName("example").getOrCreate()
            new_data = spark_local.createDataFrame([(gender, highest_education, imd_band, age_band, disability, total_clicks)], feature_cols)
            assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
            new_data = assembler.transform(new_data)

            # Make predictions with the model
            predictions = model.transform(new_data)
            result = result_map[int(predictions.select(col("prediction")).collect()[0]["prediction"])]
            return result, result_style_map[result]
        return 'Unknown', {'color': 'grey', 'fontSize': '30px'}