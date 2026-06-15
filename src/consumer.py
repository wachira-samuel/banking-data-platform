import json
import logging
from kafka import KafkaConsumer

logging.basicConfig(
	filename="logs/pipleine.log",
	level=logging.INFO,
	format= "%(asctime)s - %(message)s"
	)

TOPIC_NAME= "bank_transactions"

def create_consumer():
	consumer = KafkaConsumer(
		TOPIC_NAME,
		bootstrap_servers="localhost:9092",
		value_deserializer= lambda x:json.loads(x.decode("utf-8")
		),
		auto_offset_reset="earliest",
		enable_auto_commit=True
	)
	return consumer
def consume_transactions():
	consumer = create_consumer()
	for message in  consumer:
		transaction = message.value
		logging.info(
			f"Received transaction : {transaction['transaction_id']}"
		)
		print("received:", transaction)

if __name__ == "__main__":
	consume_transactions()


