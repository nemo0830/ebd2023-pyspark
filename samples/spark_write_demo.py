from pyspark.sql import SparkSession
import os

spark = SparkSession.builder.master("local").enableHiveSupport().getOrCreate()
print("Storing random number")
spark.range(100).write.saveAsTable("random_numbers")
print("Created")