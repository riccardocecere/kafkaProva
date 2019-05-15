# detector/app.py
import os
import json
from time import sleep
from kafka import KafkaConsumer, KafkaProducer
import classifier

KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL')
INPUT_TOPIC = os.environ.get('INPUT_TOPIC')
OUTPUT_TOPIC = os.environ.get('OUTPUT_TOPIC')
MODEL = os.environ.get('MODEL')
TRAINING_SET = os.environ.get('TRAINING_SET')
TRAINING_PARQUET = os.environ.get('TRAINING_PARQUET')


if __name__ == '__main__':
    model = classifier.load_classifier(MODEL, TRAINING_PARQUET, TRAINING_SET)
    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BROKER_URL,
        value_deserializer=lambda value: json.loads(value),
        enable_auto_commit=False,
        auto_offset_reset='earliest'
    )
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER_URL,
        # Encode all values as JSON
        value_serializer=lambda value: json.dumps(value).encode(),
    )
    working = True
    while working:
        dict = consumer.poll(timeout_ms = 10, max_records = 5)
        #if (dict is not None) and (str(dict) is not '{}') and (dict.items() is not []) and dict.items():
        if(dict != {}):
            print(dict)
            for topic, messages in dict.items():
                print(topic)
                print(messages)
                for message in messages:
                    print(message.value)
                    if classifier.predict(model, message.value) == 1:
                        future = producer.send(OUTPUT_TOPIC, value=message.value)
                        result = future.get(timeout=60)
                        print("Sent message n# " + str(message.value))
                    else:
                        print('Predicted negative value ##########')
        print('passo')
