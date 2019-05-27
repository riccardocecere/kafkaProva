# detector/app.py
import os
import json
from time import sleep
from kafka import KafkaConsumer, KafkaProducer
import foxlink_crawler

KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL')
INPUT_TOPIC = os.environ.get('TOPIC_INPUT')
OUTPUT_TOPIC = os.environ.get('TOPIC_OUTPUT')
depth_limit = os.environ.get('DEPTH_LIMIT')
download_delay = os.environ.get('DOWNLOAD_DELAY')
closespider_pagecount = os.environ.get('CLOSESPIDER_PAGECOUNT')
autothrottle_enable = os.environ.get('AUTOTHROTTLE_ENABLE')
autothrottle_target_concurrency = os.environ.get('AUTOTHROTTLE_TARGET_CONCURRENCY')


if __name__ == '__main__':
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
        if(dict != {}):
            #print(dict)
            for topic, messages in dict.items():
                #print(topic)
                #print(messages)
                urls = []
                for message in messages:
                    print('Received message: '+str(message.value))
                    urls.append(message.value)
                try:
                    foxlink_crawler.intrasite_crawling_iterative(urls,
                                                                 depth_limit,
                                                                 download_delay,
                                                                 closespider_pagecount,
                                                                 autothrottle_enable,
                                                                 autothrottle_target_concurrency,
                                                                 producer, OUTPUT_TOPIC)
                except Exception as ex:
                    print(ex)
        #print('passo')
