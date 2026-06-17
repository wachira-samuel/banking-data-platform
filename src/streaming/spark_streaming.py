from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp

from streaming.validator import validate_transactions
from streaming.schema import transaction_schema
from streaming.transformations import transform_transactions
from streaming.fraud_rules import add_fraud_features
from streaming.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    POSTGRES_URL,
    POSTGRES_USER,
    POSTGRES_PASSWORD
)

# Spark Session
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("BankingStreaming") \
    .config("spark.driver.memory", "2g") \
    .config(
        "spark.jars.packages",
        ",".join([
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
            "org.postgresql:postgresql:42.7.3"
        ])
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# ---------------------------------------------------
# 1. Read Kafka Stream
# ---------------------------------------------------
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()


# ---------------------------------------------------
# 2. Convert Kafka bytes → JSON
# ---------------------------------------------------
json_df = kafka_df.selectExpr(
    "CAST(value AS STRING) as json_str"
)


# ---------------------------------------------------
# 3. Parse JSON Schema
# ---------------------------------------------------
transactions_df = json_df.select(
    from_json(
        col("json_str"),
        transaction_schema
    ).alias("data")
).select("data.*")


# ---------------------------------------------------
# 4. Convert timestamp
# ---------------------------------------------------
transactions_df = transactions_df.withColumn(
    "event_time",
    to_timestamp(col("timestamp"))
).drop("timestamp")


# ---------------------------------------------------
# 5. Data Validation Layer
# ---------------------------------------------------
valid_df, invalid_df = validate_transactions(
    transactions_df
)


# ---------------------------------------------------
# 6. Invalid records → Console (quarantine)
# ---------------------------------------------------
invalid_query = invalid_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .option("truncate", False) \
    .start()


# ---------------------------------------------------
# 7. Transform valid transactions
# ---------------------------------------------------
transformed_df = transform_transactions(
    valid_df
)


# ---------------------------------------------------
# 8. Fraud Detection Rules
# ---------------------------------------------------
final_df = add_fraud_features(
    transformed_df
)


# ---------------------------------------------------
# 9. PostgreSQL Sink
# ---------------------------------------------------
def write_to_postgres(batch_df, batch_id):

    print(f"Writing batch {batch_id}")

    batch_df.write \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option(
            "dbtable",
            "public.transaction_analytics"
        ) \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option(
            "driver",
            "org.postgresql.Driver"
        ) \
        .mode("append") \
        .save()

    print(
        f"Batch {batch_id} written successfully"
    )


# ---------------------------------------------------
# 10. Write Valid Records to PostgreSQL
# ---------------------------------------------------
postgres_query = final_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .option(
        "checkpointLocation",
        "/tmp/spark_checkpoint_banking"
    ) \
    .start()


# Keep stream running
postgres_query.awaitTermination()
invalid_query.awaitTermination()
