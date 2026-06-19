from google.cloud import bigquery
import os

BASE_DIR = "/mnt/e/banking-data-platform"
KEY_PATH = os.path.join(BASE_DIR, "credentials", "spark-gcs-key.json")

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = KEY_PATH

client = bigquery.Client()

print("Authenticated project:", client.project)
