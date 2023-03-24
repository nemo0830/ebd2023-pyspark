from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("gcloud_downloader").master("local").getOrCreate()
df = spark.read.text("gs://mock_raw_ebd_2023/20230101/studentAssessment.csv")
df.show()