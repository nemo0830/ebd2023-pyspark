from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler

from config.AppProperties import *
from config.SparkConn import jdbcGoldUrl, connectionProperties
from config.SparkConn import spark as project_spark
from model.DecisionTreeTrainer import train_data
from transformer import StudentInfoMLTransformer

# Train data
module_code = 'AAA'
student_info_data = StudentInfoMLTransformer.StudentInfoMLTransformer().read_table_and_transform(project_spark, jdbcGoldUrl, connectionProperties)
training_df, testing_df = student_info_data.filter("code_module = '%s'" % module_code)\
    .randomSplit([0.7, 0.3], seed=666)
model = train_data(training_df, module_code, indexing_feature_cols, non_indexing_feature_cols, indexer_str)

print(model.toDebugString)

# Fit model
testing_df = StudentInfoMLTransformer.StudentInfoMLTransformer().one_hot_encoding(testing_df, indexing_feature_cols, indexer_str)
feature_cols = [feature + indexer_str for feature in indexing_feature_cols] + non_indexing_feature_cols
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
testing_df = assembler.transform(testing_df)
predictions = model.transform(testing_df)

# Start evaluating
evaluator = MulticlassClassificationEvaluator(predictionCol="prediction", labelCol="final_result" + indexer_str, metricName="f1")

# Compute the F1-score
f1_score = evaluator.evaluate(predictions)

# Compute the accuracy
accuracy = predictions.filter(predictions.final_result_idx == predictions.prediction).count() / float(predictions.count())

# Compute the recall
true_positive = predictions.filter((predictions.final_result_idx == 1) & (predictions.prediction == 1)).count()
false_negative = predictions.filter((predictions.final_result_idx == 1) & (predictions.prediction == 0)).count()
recall = true_positive / float(true_positive + false_negative)

# Compute the precision
true_positive = predictions.filter((predictions.final_result_idx == 1) & (predictions.prediction == 1)).count()
false_positive = predictions.filter((predictions.final_result_idx == 0) & (predictions.prediction == 1)).count()
precision = true_positive / float(true_positive + false_positive)

# Print the metrics
print("F1-score = {:.4f}".format(f1_score))
print("Accuracy = {:.4f}".format(accuracy))
print("Recall = {:.4f}".format(recall))
print("Precision = {:.4f}".format(precision))