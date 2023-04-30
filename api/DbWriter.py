from config.AppProperties import code_modules, indexing_feature_cols, indexer_str
from config.SparkConn import *
from transformer import StudentScoreTransformer, StudentInfoMLTransformer
from config.SparkConn import spark as project_spark

student_score_data = StudentScoreTransformer.StudentScoreTransformer().read_table_and_transform(project_spark, jdbcGoldUrl, connectionProperties)
student_score_data.write \
    .format("jdbc") \
    .option("dbtable", "studentscoretransformed") \
    .option("url", jdbcWorkUrl) \
    .options(**connectionProperties) \
    .mode("overwrite") \
    .save()

student_info_data = StudentInfoMLTransformer.StudentInfoMLTransformer().read_table_and_transform(project_spark, jdbcGoldUrl, connectionProperties)
for module_code in code_modules:
    print("start loading " + module_code)
    tmp_data = student_info_data.filter("code_module = '%s'" % module_code)
    tmp_data = StudentInfoMLTransformer.StudentInfoMLTransformer().one_hot_encoding(tmp_data, indexing_feature_cols, indexer_str)
    tmp_data.write \
        .format("jdbc") \
        .option("dbtable", "studentinfotransformed") \
        .option("url", jdbcWorkUrl) \
        .options(**connectionProperties) \
        .mode("append") \
        .save()