from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

from ingestion.producer import main as producer_main


default_args = {
    "owner": "sam"
}


with DAG(
    dag_id="banking_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 6, 20),
    schedule=None,
    catchup=False
) as dag:

    start_producer = PythonOperator(
        task_id="start_producer",
        python_callable=producer_main
    )

    start_streaming = SparkSubmitOperator(
        task_id="start_streaming",
        application="/opt/project/src/streaming/spark_streaming.py",
        conn_id="spark_default",
        name="banking-streaming-job",
        conf={
            "spark.master": "spark://spark-master:7077",
            "spark.app.name": "BankingStreaming",
            "spark.executor.memory": "1g",
            "spark.driver.memory": "1g"
        }
    )

    start_producer >> start_streaming
