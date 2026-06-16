from pyspark.sql.types import *

transaction_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("account_id", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("channel", StringType(), True),
    StructField("merchant", StringType(), True),
    StructField("location", StringType(), True),
    StructField("timestamp", StringType(), True)
])
