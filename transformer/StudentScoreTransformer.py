from pyspark.sql.functions import col, sum, desc


class StudentScoreTransformer:

    def __init__(self):
        self.id = 1

    def read_table_and_transform(self, spark, jdbcUrl, connectionProperties):
        df_a = spark.read.jdbc(url=jdbcUrl, table="assessments", properties=connectionProperties)
        df_a = df_a.withColumn('weight', df_a['weight'] / 100)
        df_sa = spark.read.jdbc(url=jdbcUrl, table="studentassessment", properties=connectionProperties)

        df_join = df_sa.join(df_a, on="id_assessment", how="left")

        processed_data = df_join\
            .groupBy(["id_student", "code_presentation"]) \
            .agg(sum(col("score") * col("weight")).alias('weighted_score')) \
            .dropna() \
            .select(col("id_student").cast("varchar(20)"), col("weighted_score"), col("code_presentation")) \
            .sort(desc("weighted_score")) \
            .filter("weighted_score != %s" % 0.0)

        return processed_data
