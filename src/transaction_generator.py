import uuid
import random
from faker import Faker
from datetime import datetime

faker = Faker()

MERCHANTS = [
	"Naivas",
	"Carrefour",
	"Quickmart",
	"Jumiapay",
	"Safaricom",
	"KCB Bank"
]

CHANNELS = [
	"ATM",
	"POS",
	"Mobile  Banking",
	"Internet Banking"
]

#generate Synthetic banking transaction
def generate_transaction() -> dict:
	transaction = {
	"transaction_id" : str(uuid.uuid4()),
	"account_id" : random.randint(1000,99999),
	"amount": round(random.uniform(100, 10000), 2),
	"channel": random.choice(CHANNELS),
	"merchant" :random.choice(MERCHANTS),
	"location": faker.city(),
	"timestamp" : datetime.utcnow().isoformat()
	}
	return transaction
if __name__ =="__main__":
	print(generate_transaction())

