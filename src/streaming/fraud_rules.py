from pyspark.sql.functions import col, when

def add_fraud_features(df):
    return df.withColumn(
        "risk_score",
        when(col("amount") > 8000, 95)
        .when(col("amount") > 5000, 70)
        .when(col("amount") > 3000, 40)
        .otherwise(10)
    ).withColumn(
        "is_fraud",
        when(col("amount") > 8000, True).otherwise(False)
    )
