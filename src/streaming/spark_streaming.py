
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *

from transformations import transform_transactions


schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("merchant", StringType(), True),
    StructField("timestamp", StringType(), True)
])


spark = SparkSession.builder \
    .appName("BankingTransactionProcessor") \
    .getOrCreate()


# Read Kafka stream
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "bank_transactions") \
    .load()


# Deserialize JSON
json_df = df.selectExpr("CAST(value AS STRING)")


parsed_df = json_df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")


# Transform
processed_df = transform_transactions(parsed_df)


# Output to console first
query = processed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()


query.awaitTermination()
