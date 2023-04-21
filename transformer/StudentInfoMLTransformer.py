from pyspark.sql.functions import col, sum

class StudentInfoMLTransformer:

    def __init__(self):
        self.id = 2

    def read_table_and_transform(self, spark, jdbcUrl, connectionProperties):
        df_s = spark.read.jdbc(url=jdbcUrl, table="studentinfo", properties=connectionProperties)
        df_sv = spark.read.jdbc(url=jdbcUrl, table="studentvle", properties=connectionProperties)
        df_sv = df_sv.groupBy(["id_student", "code_presentation", "code_module"]) \
            .agg(sum(col("sum_click")).alias('total_click'))

        return df_s.join(df_sv, ["code_module", "code_presentation", "id_student"])\
                .select(col("id_student").cast("varchar(20)")
                        , col("code_presentation")
                        , col("code_module")
                        , col("gender")
                        , col("highest_education")
                        , col("imd_band")
                        , col("age_band")
                        , col("disability")
                        , col("total_click")
                        , col("final_result"))