from pyspark.sql.functions import col, when


def transform_transactions(df):

    transformed = df.withColumn(
        "risk_score",
        when(col("amount") > 100000, 95)
        .when(col("amount") > 50000, 70)
        .when(col("amount") > 20000, 40)
        .otherwise(10)
    )

    transformed = transformed.withColumn(
        "is_fraud",
        when(col("amount") > 50000, True)
        .otherwise(False)
    )

    return transformed
