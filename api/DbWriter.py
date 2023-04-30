from pyspark.sql import SparkSession
from config.SparkConn import *
from transformer import StudentScoreTransformer, StudentInfoMLTransformer

spark = SparkSession.builder \
    .appName("gcloud_sql") \
    .config("spark.jars", "../postgresql-42.6.0.jar") \
    .master("local").getOrCreate()

student_score_data = StudentScoreTransformer.StudentScoreTransformer().read_table_and_transform(spark, jdbcGoldUrl, connectionProperties)
student_score_data.write \
    .format("jdbc") \
    .option("dbtable", "studentscoretransformed") \
    .option("url", jdbcWorkUrl) \
    .options(**connectionProperties) \
    .mode("overwrite") \
    .save()

student_info_data = StudentInfoMLTransformer.StudentInfoMLTransformer().read_table_and_transform(spark, jdbcGoldUrl, connectionProperties)
student_info_data.write \
    .format("jdbc") \
    .option("dbtable", "studentinfotransformed") \
    .option("url", jdbcWorkUrl) \
    .options(**connectionProperties) \
    .mode("overwrite") \
    .save()