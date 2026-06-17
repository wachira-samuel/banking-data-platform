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
    POSTGRES_PASSWORD,
    POSTGRES_DRIVER
)

# ---------------------------------------------------
# 1. Spark Session
# ---------------------------------------------------
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
# 2. Read Kafka Stream
# ---------------------------------------------------
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# ---------------------------------------------------
# 3. Parse JSON
# ---------------------------------------------------
json_df = kafka_df.selectExpr(
    "CAST(value AS STRING) as json_str"
)

transactions_df = json_df.select(
    from_json(
        col("json_str"),
        transaction_schema
    ).alias("data")
).select("data.*")

# ---------------------------------------------------
# 4. Convert Timestamp
# ---------------------------------------------------
transactions_df = transactions_df.withColumn(
    "event_time",
    to_timestamp(col("timestamp"))
).drop("timestamp")

# ---------------------------------------------------
# 5. Raw Data Lake
# ---------------------------------------------------
raw_query = transactions_df.writeStream \
    .format("parquet") \
    .option("path", "datalake/raw") \
    .option("checkpointLocation", "/tmp/raw_checkpoint") \
    .outputMode("append") \
    .start()

# ---------------------------------------------------
# 6. Validation Layer
# ---------------------------------------------------
valid_df, invalid_df = validate_transactions(
    transactions_df
)

# ---------------------------------------------------
# 7. Invalid Records Quarantine
# ---------------------------------------------------
invalid_query = invalid_df.writeStream \
    .format("parquet") \
    .option("path", "datalake/invalid") \
    .option("checkpointLocation", "/tmp/invalid_checkpoint") \
    .outputMode("append") \
    .start()

# ---------------------------------------------------
# 8. Transform Valid Records
# ---------------------------------------------------
transformed_df = transform_transactions(
    valid_df
)

# ---------------------------------------------------
# 9. Fraud Detection Rules
# ---------------------------------------------------
final_df = add_fraud_features(
    transformed_df
)

# ---------------------------------------------------
# 10. Processed Data Lake
# ---------------------------------------------------
processed_query = final_df.writeStream \
    .format("parquet") \
    .option("path", "datalake/processed") \
    .option("checkpointLocation", "/tmp/processed_checkpoint") \
    .outputMode("append") \
    .start()

# ---------------------------------------------------
# 11. PostgreSQL Sink
# ---------------------------------------------------
def write_to_postgres(batch_df, batch_id):

    print(f"Writing batch {batch_id}")

    batch_df.write \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", "public.transaction_analytics") \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", POSTGRES_DRIVER) \
        .mode("append") \
        .save()

    print(f"Batch {batch_id} written successfully")


postgres_query = final_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .option(
        "checkpointLocation",
        "/tmp/postgres_checkpoint"
    ) \
    .start()

# ---------------------------------------------------
# 12. Keep Streams Running
# ---------------------------------------------------
spark.streams.awaitAnyTermination()
