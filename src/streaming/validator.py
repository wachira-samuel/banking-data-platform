from pyspark.sql.functions import col


def validate_transactions(df):

    # valid records
    valid_df = df.filter(
        col("transaction_id").isNotNull() &
        col("amount").isNotNull() &
        (col("amount") > 0) &
        (col("amount") < 1000000) &
        col("merchant").isNotNull()
    )

    # invalid records
    invalid_df = df.filter(
        col("transaction_id").isNull() |
        col("amount").isNull() |
        (col("amount") <= 0) |
        (col("amount") >= 1000000) |
        col("merchant").isNull()
    )

    return valid_df, invalid_df
