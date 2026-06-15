import json
import time
import logging
from kafka import KafkaProducer
from transaction_generator import generate_transaction



logging.basicConfig(
    filename="../logs/pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

TOPIC_NAME = "bank_transactions"


def create_producer():

    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    return producer


def send_transactions():

    producer = create_producer()

    while True:

        transaction = generate_transaction()

        try:
            producer.send(TOPIC_NAME, transaction)
            producer.flush()

            logging.info(
                f"Transaction sent: {transaction['transaction_id']}"
            )

            print("Sent:", transaction)

            time.sleep(2)

        except Exception as e:

            logging.error(f"Producer error: {e}")
            print("Error:", e)


if __name__ == "__main__":
    send_transactions()
