import json

from faker import Faker
from confluent_kafka import SerializingProducer
from datetime import datetime
import time
import random

# Create a Faker instance
fake = Faker()

def generate_sales_transactions():
    user = fake.simple_profile()

    return {
        "transactionId": fake.uuid4(),
        "productId": random.choice(['product1', 'product2', 'product3', 'product4', 'product5', 'product6']),
        "productName": random.choice(['laptop', 'mobile', 'tablet', 'smartwatch', 'headphone', 'speaker']),
        "productPrice": round(random.uniform(0, 100), 2),
        "productQuantity": random.randint(1, 10),
        "productBrand": random.choice(['apple', 'samsung', 'xiaomi', 'huawei', 'sony', 'nokia']),
        "currency": random.choice(['USD', 'VND']),
        "customerId": user['username'],
        "transactionDate": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f%z'),
        "payMethod": random.choice(['credit_card', 'debit_card', 'online_transfer'])
    }
def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for User record {msg.key()}: {err}")
        return
    print(f"User record {msg.key()} successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def main():
    topic = 'financial_transactions'
    producer = SerializingProducer({
        "bootstrap.servers": "localhost:9092"
    })

    cur_time = datetime.now()

    while (datetime.now() - cur_time).seconds < 10000:
        try:
            transaction = generate_sales_transactions()
            transaction['totalAmount'] = round(transaction['productPrice'] * transaction['productQuantity'],2)
            # producer.produce(topic=topic, value=generate_sales_transactions())
            print(transaction)

            producer.produce(topic=topic,
                             key=transaction['transactionId'],
                             value=json.dumps(transaction),
                             on_delivery=delivery_report)

            producer.poll(0)

            time.sleep(5)

        except BufferError:
            print(f"Local producer queue is full ({len(producer)} messages awaiting delivery): try again")
            time.sleep(1)
        except Exception as e:
            print(f"Error: {e}")

if __name__ == '__main__':
    main()