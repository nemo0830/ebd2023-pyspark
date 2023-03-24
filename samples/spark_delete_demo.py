from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").enableHiveSupport().getOrCreate()
spark.sql("DROP TABLE random_numbers")