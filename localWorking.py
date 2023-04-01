from pyspark import SparkContext, SparkConf
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import matplotlib.pyplot as plot

def main():
    cnfg = SparkConf().setAppName("CustomerApplication").setMaster("local[2]")
    sc = SparkContext(conf=cnfg)
    spark = SparkSession(sc)

    inputFilePathSVle = "C:\Program Files\Python311\Workspace\studentVle2.csv"
    inputFilePathA = "C:\Program Files\Python311\Workspace\Assessments.csv"
    inputFilePathSI = "C:\Program Files\Python311\Workspace\studentInfo.csv"
    inputFilePathSR = "C:\Program Files\Python311\Workspace\studentRegistration.csv"
    inputFilePathVle = "C:\Program Files\Python311\Workspace\Vle.csv"
    inputFilePathC = "C:\Program Files\Python311\Workspace\courses.csv"
    inputFilePathSA = "C:\Program Files\Python311\Workspace\studentAssessment.csv"

    dfSVle = (spark.read.option("header", "true").option("inferSchema", "true").csv(inputFilePathSVle))
    dfA = (spark.read.option("header", "true").option("inferSchema", "true").csv(inputFilePathA))
    dfSI = (spark.read.option("header", "true").option("inferSchema", "true").csv(inputFilePathSI))
    dfA = (spark.read.option("header", "true").option("inferSchema", "true").csv(inputFilePathA))
    dfSR = (spark.read.option("header", "true").option("inferSchema", "true").csv(inputFilePathSR))
    dfVle = (spark.read.option("header", "true").option("inferSchema", "true").csv(inputFilePathVle))
    dfC = (spark.read.option("header", "true").option("inferSchema", "true").csv(inputFilePathC))
    dfSA = (spark.read.option("header", "true").option("inferSchema", "true").csv(inputFilePathSA))

    #joinDF = dfVle.join(dfC, "code_module")\
    #    .select("module_presentation_length")\
    #    .where("code_module='AAA'") \
    #    .show(10, False)

    joinDF = dfSVle.join(dfSA, "id_student")\
        .select("sum_click", "score")\
        .show(100, False)

if __name__ == '__main__':
    main()