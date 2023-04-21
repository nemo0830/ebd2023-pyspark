from dash.dependencies import Input, Output, State
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import col
import plotly.express as px
import os

def register_callbacks(app, student_score_data, model_dic, final_result_options, result_style):

    @app.callback(
        Output('student-score-plot', 'figure'),
        State('top_or_bottom', 'value'),
        State('semester', 'value'),
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
            .update_layout(title_font_size=30)
        return fig


    @app.callback(Output('predicted_result', 'children'),
                  Output('predicted_result', 'style'),
                  Input('submit-button-prediction', 'n_clicks'),
                  State('course_to_predict', 'value'),
                  State('gender', 'value'),
                  State('highest_education', 'value'),
                  State('imd_band', 'value'),
                  State('age_band', 'value'),
                  State('disability', 'value'),
                  State('total_clicks', 'value')
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