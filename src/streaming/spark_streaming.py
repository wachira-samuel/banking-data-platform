from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, date_format

from streaming.schema import transaction_schema
from streaming.transformations import transform_transactions
from streaming.fraud_rules import add_fraud_features
from streaming.validator import validate_transactions

from streaming.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    POSTGRES_URL,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    POSTGRES_DRIVER
)

from cloud.bigquery_loader import write_to_bigquery as bq_writer


# =========================
# 1. SPARK SESSION
# =========================
spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("BankingStreaming")
    .config("spark.driver.memory", "2g")
    .config(
        "spark.jars.packages",
        ",".join([
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
            "org.postgresql:postgresql:42.7.3"
        ])
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# =========================
# 2. KAFKA SOURCE
# =========================
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")

parsed_df = (
    json_df.select(from_json(col("json_str"), transaction_schema).alias("data"))
    .select("data.*")
)


# =========================
# 3. TIMESTAMP CLEANING
# =========================
parsed_df = parsed_df.withColumn(
    "event_time",
    date_format(to_timestamp(col("timestamp")), "yyyy-MM-dd HH:mm:ss")
).drop("timestamp")


# =========================
# 4. VALIDATION + TRANSFORM
# =========================
valid_df, invalid_df = validate_transactions(parsed_df)

transformed_df = transform_transactions(valid_df)
final_df = add_fraud_features(transformed_df)


# =========================
# 5. POSTGRES SINK
# =========================
def write_to_postgres(batch_df, batch_id):
    print(f"[Postgres] Batch {batch_id}")

    if batch_df.rdd.isEmpty():
        return

    batch_df.write \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", "public.transaction_analytics") \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", POSTGRES_DRIVER) \
        .mode("append") \
        .save()


# =========================
# 6. SINGLE STREAM SINK

def sink_all(batch_df, batch_id):

    row_count = batch_df.count()

    print("\n" + "=" * 60)
    print(f"[PIPELINE] Processing batch={batch_id}")
    print(f"[PIPELINE] Rows={row_count}")

    batch_df.show(100, truncate=False)

    if row_count == 0:
        print("[PIPELINE] Empty batch skipped")
        return

    # --------------------------------
    # POSTGRES DATAFRAME
    # account_id -> integer
    # event_time -> text (matches postgres)
    # --------------------------------
    postgres_df = (
        batch_df
        .withColumn("account_id", col("account_id").cast("int"))
        .withColumn("event_time", col("event_time").cast("string"))
    )

    # --------------------------------
    # BIGQUERY DATAFRAME
    # account_id -> string
    # event_time -> timestamp
    # --------------------------------
    bq_df = (
        batch_df
        .withColumn("account_id", col("account_id").cast("string"))
        .withColumn("event_time", to_timestamp(col("event_time")))
    )

    print("[PIPELINE] Writing to Postgres...")
    write_to_postgres(postgres_df, batch_id)

    print("[PIPELINE] Writing to BigQuery...")
    bq_writer(bq_df, batch_id)

    print(f"[PIPELINE] Batch {batch_id} completed")




# 7. MAIN STREAM QUERY
# =========================
main_query = (
    final_df.writeStream
    .foreachBatch(sink_all)
    .option("checkpointLocation", "/tmp/checkpoint_main")
    .outputMode("append")
    .start()
)
# 8. INVALID DATA STREAM
# =========================
invalid_query = (
    invalid_df.writeStream
    .format("parquet")
    .option("path", "/tmp/banking/invalid")
    .option("checkpointLocation", "/tmp/checkpoints/invalid")
    .outputMode("append")
    .start()
)


# =========================
# 9. PROCESSED ARCHIVE
# =========================
processed_query = (
    final_df.writeStream
    .format("parquet")
    .option("path", "/tmp/banking/processed")
    .option("checkpointLocation", "/tmp/checkpoints/processed")
    .outputMode("append")
    .start()
)


# =========================
# 10. RAW STREAM
# =========================
raw_query = (
    parsed_df.writeStream
    .format("parquet")
    .option("path", "/tmp/banking/raw")
    .option("checkpointLocation", "/tmp/checkpoints/raw")
    .outputMode("append")
    .start()
)


# =========================
# 11 DEBUG STREAM
debug_query = (
    final_df.writeStream
    .format("console")
    .option("numRows", 100)
    .option("truncate", False)
    .outputMode("append")
    .start()
)


# =========================
# 12. KEEP ALIVE
# =========================
print("ACTIVE STREAMS:")

for q in spark.streams.active:
    print("---------------")
    print("ID:", q.id)
    print("Active:", q.isActive)

spark.streams.awaitAnyTermination()
