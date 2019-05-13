# destination/app.py
import os
import json
from kafka import KafkaConsumer

KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL')
INPUT_TOPIC = os.environ.get('INPUT_TOPIC')


if __name__ == '__main__':
    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BROKER_URL,
        value_deserializer=lambda value: json.loads(value),
        enable_auto_commit=False,
        auto_offset_reset='earliest'
    )
    working = True
    while working:
        dict = consumer.poll(timeout_ms = 10, max_records = 5)
        #if (dict is not None) and (str(dict) is not '{}') and (dict.items() is not []) and dict.items():
        if(dict != {}):
            for topic, messages in dict.items():
                for message in messages:
                    print(message.value)
            #print(dict)
        print('passo')