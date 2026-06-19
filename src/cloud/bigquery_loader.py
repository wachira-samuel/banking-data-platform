from google.cloud import bigquery
import pandas as pd
import os

BASE_DIR = "/mnt/e/banking-data-platform"

KEY_PATH = os.path.join(BASE_DIR, "credentials", "spark-gcs-key.json")

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = KEY_PATH

client = bigquery.Client()


def write_to_bigquery(batch_df, batch_id):

    table_id = "banking-data-platform.banking_analytics.transaction_analytics"

    print("\n" + "=" * 70)
    print("BIGQUERY FUNCTION CALLED")
    print(f"BATCH ID: {batch_id}")

    row_count = batch_df.count()
    print(f"ROWS RECEIVED: {row_count}")

    if row_count == 0:
        print("EMPTY BATCH")
        return

    try:
        pdf = batch_df.toPandas()

        # ALIGN COLUMNS TO BIGQUERY
        # rename Spark column to BigQuery column
        if "channel" in pdf.columns:
            pdf.rename(columns={"channel": "transaction_type"}, inplace=True)

        # remove columns not in BigQuery
        if "location" in pdf.columns:
            pdf.drop(columns=["location"], inplace=True)

        # FORCE DATA TYPES
        pdf["transaction_id"] = pdf["transaction_id"].astype(str)
        pdf["account_id"] = pdf["account_id"].astype(str)
        pdf["amount"] = pdf["amount"].astype(float)
        pdf["merchant"] = pdf["merchant"].astype(str)
        pdf["transaction_type"] = pdf["transaction_type"].astype(str)
        pdf["risk_score"] = pdf["risk_score"].astype(int)
        pdf["is_fraud"] = pdf["is_fraud"].astype(bool)

        # timestamp conversion
        pdf["event_time"] = pd.to_datetime(pdf["event_time"])

        print("FINAL BIGQUERY DTYPES")
        print(pdf.dtypes)

        # LOAD JOB
        job = client.load_table_from_dataframe(
            pdf,
            table_id,
            job_config=bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND"
            )
        )

        job.result()

        print(f"[BigQuery SUCCESS] inserted={len(pdf)} rows")

    except Exception as e:
        print("[BigQuery FAILED]")
        print(repr(e))
        raise
