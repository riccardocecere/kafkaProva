# generator/app.py
import os
import json
from cgi import log
from time import sleep
from kafka import KafkaProducer
from transactions import create_random_transaction

KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL')
OUTPUT_TOPIC = os.environ.get('OUTPUT_TOPIC')
TRANSACTIONS_PER_SECOND = float(os.environ.get('TRANSACTIONS_PER_SECOND'))
SLEEP_TIME = 1 / TRANSACTIONS_PER_SECOND



if __name__ == '__main__':
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER_URL,
        # Encode all values as JSON
        value_serializer=lambda value: json.dumps(value).encode()
    )
    #while True:
    for i in range(0,30):
        future = producer.send(OUTPUT_TOPIC, value=str(i) + " - MESSAGGIO")
        result = future.get(timeout=60)
        #print(transaction)  # DEBUG
        print("Sent message n# "+str(i))
        sleep(SLEEP_TIME)