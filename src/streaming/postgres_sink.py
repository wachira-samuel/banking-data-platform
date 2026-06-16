
def write_to_postgres(batch_df, batch_id):

    batch_df.select(
        "transaction_id",
        "account_id",
        "amount",
        "risk_score",
        "is_fraud",
        "timestamp"
    ).write \
     .format("jdbc") \
     .option(
        "url",
        "jdbc:postgresql://localhost:5432/banking_db"
     ) \
     .option("dbtable", "transaction_analytics") \
     .option("user", "postgres") \
     .option("password", "12345") \
     .option("driver", "org.postgresql.Driver") \
     .mode("append") \
     .save()
