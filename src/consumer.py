import json
import logging
from kafka import KafkaConsumer
from storage.db import connect_db


logging.basicConfig(
    filename="logs/pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(message)s"
)

TOPIC_NAME = "bank_transactions"


def create_consumer():

    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers="localhost:9092",

        value_deserializer=lambda x: json.loads(
            x.decode("utf-8")
        ),

        auto_offset_reset="earliest",
        enable_auto_commit=True
    )

    return consumer


def save_transaction(transaction):
    """
    Insert transaction into PostgreSQL
    """

    conn = connect_db()

    if conn is None:
        print("Database unavailable")
        return

    cursor = conn.cursor()
    cursor.execute("SET search_path TO public;")
    



    query = """
    INSERT INTO public.transactions (
        transaction_id,
        account_id,
        amount,
        channel,
	location,
        merchant,
        timestamp
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s)
    """

    values = (
        transaction["transaction_id"],
        transaction["account_id"],
        transaction["amount"],
        transaction["channel"],
	transaction["location"],
        transaction["merchant"],
        transaction["timestamp"]
    )

    try:
        cursor.execute(query, values)
        conn.commit()

        print(
            f"Saved transaction {transaction['transaction_id']}"
        )

    except Exception as e:
        print(f"Insert failed: {e}")

    finally:
        cursor.close()
        conn.close()


def consume_transactions():

    consumer = create_consumer()

    for message in consumer:

        transaction = message.value

        logging.info(
            f"Received transaction: {transaction['transaction_id']}"
        )

        print("Received:", transaction)

        # Save into PostgreSQL
        save_transaction(transaction)


if __name__ == "__main__":
    consume_transactions()
