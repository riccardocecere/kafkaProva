# generator/app.py
import os
import json
from cgi import log
from time import sleep
from kafka import KafkaProducer

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
    urls = ['https://neo4j.com','https://www.couchsurfing.com',
                                'https://www.rightmove.co.uk','https://www.realtor.com/',
                                'https://www.homes.com/','https://www.investopedia.com',
                                'http://www.nasdaq.com','https://www.invesco.com',
                                'https://www.reddit.com','https://www.nhl.com',
                                'https://www.nfl.com','https://9gag.com',
                                'https://www.skype.com','https://www.coursera.org',
                                'https://web.mit.edu','https://www.stanford.edu',
                                'https://www.edx.org','https://iversity.org',
                                'https://www.rottentomatoes.com','https://www.imdb.com',
                                'https://www.youtube.com','https://uk.ign.com/',
                                'https://www.goodreads.com/','http://www.readprint.com/',
                                'https://www.bookfinder.com/','https://www.indiebound.org/indie-bookstore-finder',
                                'https://www.cbr.com/','http://www.addall.com/',
                                'http://onlinebooks.library.upenn.edu/search.html','https://www.powells.com/',
                                'https://manybooks.net/','http://www.online-literature.com/author_index.php',
                                'http://freecomputerbooks.com/','https://librivox.org/',
                                'http://www.authorama.com/','http://www.gutenberg.org/wiki/Main_Page',
                                'http://en.childrenslibrary.org/','https://archive.org/details/texts',
                                'https://www.questia.com/library/free-books','https://en.wikisource.org/wiki/Main_Page',
                                'https://en.wikibooks.org/wiki/Main_Page','https://www.bibliomania.ws/shop/bibliomania/index.html?id=32m2gmtX',
                                'https://openlibrary.org/','http://www.sacred-texts.com/',
                                'https://www.slideshare.net/','https://www.free-ebooks.net/',
                                'http://digital.library.upenn.edu/books/','http://worldpubliclibrary.org/']
    for i in urls:
        future = producer.send(OUTPUT_TOPIC, value=str(i) + " - MESSAGGIO")
        result = future.get(timeout=60)
        #print(transaction)  # DEBUG
        print("Sent message n# "+str(i))
        sleep(SLEEP_TIME)