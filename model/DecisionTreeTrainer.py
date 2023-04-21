from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import DecisionTreeClassifier


def train_data(full_data, module_code, indexing_feature_cols, non_indexing_feature_cols, indexer_str):
    module_data = full_data.filter("code_module == " + module_code)

    for feature in indexing_feature_cols:
        module_data = StringIndexer(inputCol=feature, outputCol=feature + indexer_str) \
            .setHandleInvalid("skip") \
            .fit(module_data) \
            .transform(module_data)

    module_data = StringIndexer(inputCol="final_result", outputCol="final_result" + indexer_str) \
        .setHandleInvalid("skip") \
        .fit(module_data) \
        .transform(module_data)

    # Data Dictionary after indexing
    # gender: {0: 'M', 1: 'F'}
    # highest_education: {0: 'A Level or Equivalent', 1: 'HE Qualification', 2: 'Lower Than A Level', 3: 'Post Graduate Qualification'}
    # imd_band: {0: '90-100%', 1: '70-80%', 2: '80-90%', 3: '40-50%', 4: '30-40%', 5: '50-60%', 6: '60-70%', 7: '20-30%', 8: '10-20', 9: '0-10%'}
    # age_band: {0: '0-35', 1: '35-55', 2: '55<='}
    # disability: {0: 'N', 1: 'Y'}
    # final_result: {0: 'Pass', 1: 'Withdrawn', 2: 'Fail', 3: 'Distinction'}

    feature_cols = indexing_feature_cols + non_indexing_feature_cols
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    module_data = assembler.transform(module_data)
    label_col = "final_result" + indexer_str

    # Train a decision tree classifier on the training data
    dt = DecisionTreeClassifier(labelCol=label_col)
    return dt.fit(module_data)

