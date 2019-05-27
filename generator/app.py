# generator/app.py
import os
import json
from cgi import log
from time import sleep
from kafka import KafkaProducer
import text_parser

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
    # urls = ['https://www.ibs.it',
    #         'https://www.goodreads.com/',
    #         'http://www.readprint.com/',
    #         'https://www.questia.com/library/free-books',
    #         'https://en.wikisource.org/wiki/Main_Page',
    #         'https://en.wikibooks.org/wiki/Main_Page',
    #         'https://www.slideshare.net/',
    #         'https://www.free-ebooks.net/',
    #         'http://digital.library.upenn.edu/books/',
    #         'http://worldpubliclibrary.org/']

    urls = ['https://www.ge.infn.it/~dameri/Cataloghi/Libri/LibriAnna/page5.html',
'http://internationalgrainsummit.com/book/2gzamir/il-nome-della-rosa-narratori-italiani-italian-edition.pdf',
'https://www.fnac.com/livre-numerique/a11767199/Umberto-Eco-Il-nome-della-rosa',
'https://www.apnews.com/3b12f5abea604f19a5ad36d700d090b1',
'http://goondigital.com.br/index.php/books/il-nome-della-rosa-nuova-edizione-narratori-italiani',
'https://www.newtoncompton.com/collana/i-mammut/60',
'https://news.yahoo.com/vice-president-pence-tells-west-231935764.html',
'https://variety.com/2019/film/festivals/all-about-yves-review-1203225399/',
'https://www.miglioriofferteweb.com/il-nome-della-rosa-ebook/',
'https://www.bookrepublic.it/book/9788854125742-il-capitale/',
'https://libri.cx/il-nome-della-rosa-umberto-eco-mobi/',
'https://www.kobo.com/be/en/ebook/il-capitale-1',
'http://booktele.com/file/umberto-eco-libri-il-nome-della-rosa-pdf',
'https://books.google.it/books?id=4KagDQAAQBAJ&hl=it',
'https://www.bol.com/nl/p/il-nome-della-rosa/9200000034723609/',
'https://www.lafeltrinelli.it/ebook/umberto-eco/nome-rosa/9788858706152',
'http://www.forumitalotunisino.com/component/k2/tag/Politiche%20UE%20%20Africa%20Mediterranea,%20Medio%20Oriente.html?start=8',
'http://time.com/5596445/north-korea-john-bolton-war-monger/',
'https://play.google.com/store/books/details/Il_capitale?id=iZm8S_EMrbIC',
'https://www.eurolibro.it/libro/isbn/9788845297458.html',
'https://mashable.com/article/amd-ryzen-7/',
'https://crooksandliars.com/2019/05/facebook-wont-ban-actual-nazis-or-doctored',
'https://www.kobo.com/us/en/ebook/il-capitale-1',
'http://downwithtyranny.blogspot.com/2019/05/trumps-pardoning-of-men-found-guilty-of.html',
'https://www.rankwise.net/www.lafeltrinelli.it',
'https://variety.com/2019/tv/news/scott-pelley-cbs-evening-news-work-environment-1203226229/',
'https://www.goodreads.com/work/editions/3138328-il-nome-della-rosa?page=5',
'https://www.mondadoristore.it/nome-rosa-Nuova-edizione-Umberto-Eco/eai978885870615/',
'https://www.ibs.it/nome-della-rosa-ebook-umberto-eco/e/9788858706152',
'https://www.unilibro.it/ebook/umberto-eco/nome-rosa-nuova-edizione-e-book-epub/29942334',
'https://deadline.com/2019/05/bart-starr-dead-green-bay-packers-quarterback-1202622484/',
'https://www.barnesandnoble.com/w/il-capitale-karl-marx/1119168605',
'https://www.mondadoristore.it/Il-capitale-Karl-Marx/eai978885412574/',
'https://www.libreriauniversitaria.it/ebook/9788854125742/autore-karl-marx/il-capitale-e-book.htm',
'https://books.google.com/books/about/Il_nome_della_rosa.html?id=4KagDQAAQBAJ',
'https://libri.cx/il-nome-della-rosa-umberto-eco-epub/',
'https://catalogo.fondazionelia.org/content/il-nome-della-rosa-9788858706152',
'https://sites.google.com/site/scaricarewwi/-libri-xig-scaricare-marx-il-capitale-libri-pdf-gratis-0813',
'https://www.libraccio.it/ebook/9788854125742/1.html',
'https://www.bookrepublic.it/book/9788858706152-il-nome-della-rosa/',
'https://www.cnet.com/news/game-of-thrones-the-last-watch-and-kit-haringtons-initial-reaction-to-the-season-8-finale-twist/',
'https://store.streetlib.com/isbn/9788854125742',
'https://www.moviemeter.nl/film/1922/buy/',
'https://books.google.com/books/about/Il_capitale.html?id=iZm8S_EMrbIC',
'https://www.buchpreis24.de/isbn/9788858706152',
'https://www.kobo.com/us/en/ebook/il-nome-della-rosa-1',
'https://www.newtoncompton.com/libro/il-capitale/edizione/ebook/9788854125742',
'http://www.espn.com/tennis/story/_/id/26825441/federer-wins-first-french-open-match-15',
'https://www.kobo.com/it/en/ebook/il-capitale-1',
'https://www.libraccio.it/ebook/9788858706152/1.html',
'https://www.mlolplus.it/ebook/karl-marx/il-capitale/144270',
'https://archive.org/details/EcoIlNomeDellaRosa97888587061521',
'https://thebiglead.com/2019/05/26/kawhi-leonard-sister-video-leaving-toronto/',
'https://news.yahoo.com/pakistan-pm-khan-speaks-indias-modi-congratulate-him-120740751.html',
'https://play.google.com/store/books/details/Il_nome_della_rosa_Nuova_edizione?id=QMfSLZ-4iC4C',
'https://www.kobo.com/it/en/ebook/il-nome-della-rosa-1',
'https://archive.org/details/@francescope&tab=uploads',
'https://www.unilibro.it/ebook/karl-marx/capitale-ediz-integrale-e-book-epub/81198996',
'https://www.ilounge.com/news/macbook-pro-new-keyboard-material-implemented-in-2019-models',
'https://www.ge.infn.it/~dameri/Cataloghi/Libri/LibriMauro/Library/page6.html',
'https://www.slashgear.com/nvidia-studio-brings-rtx-laptops-to-designers-and-creators-27578039/',
'https://www.dailymail.co.uk/news/article-7072821/Lindsey-Graham-warns-Speaker-Nancy-Pelosi-lose-job-goes-Trump.html?ns_mchannel=rss&ns_campaign=1490&ito=1490',
'https://deadline.com/2019/05/indianapolis-500-team-penskes-simon-pagenaud-wins-the-103rd-race-1202622747/',
'https://www.find-more-books.com/book/isbn/9781724656575.html',
'https://www.dailymail.co.uk/news/article-7073669/AOC-slams-New-York-Times-article-framing-Hope-Hickss-existential-question.html?ns_mchannel=rss&ns_campaign=1490&ito=1490',
'https://www.eurolibro.it/libro/isbn/9788854125766.html',
'https://www.powerlineblog.com/archives/2019/05/sunday-morning-coming-down-80.php',
'http://grandvalleystate.org/books/8553c8/il_nome_della_rosa_narratori_italiani_italian_edition.pdf',
'https://www.wsfa.com/2019/05/26/auburn-university-officials-ask-prayers-after-rod-bramblett-wife-killed-crash/']

    # urls = ['https://www.ge.infn.it/',
    #         'http://internationalgrainsummit.com/',
    #         'https://www.fnac.com/',
    #         'https://www.apnews.com/',
    #         'http://goondigital.com.br/',
    #         'https://www.newtoncompton.com/',
    #         'https://news.yahoo.com/vice-president-pence-tells-west-231935764.html',
    #         'https://variety.com/2019/',
    #         'https://www.miglioriofferteweb.com/',
    #         'https://www.bookrepublic.it/',
    #         'https://libri.cx/',
    #         'https://www.kobo.com/',
    #         'http://booktele.com/',
    #         'https://books.google.it/',
    #         'https://www.bol.com/',
    #         'https://www.lafeltrinelli.it/',
    #         'http://www.forumitalotunisino.com/',
    #         'http://time.com/',
    #         'https://play.google.com/',
    #         'https://www.eurolibro.it/',
    #         'https://mashable.com/',
    #         'https://crooksandliars.com/',
    #         'https://www.kobo.com/',
    #         'http://downwithtyranny.blogspot.com/',
    #         'https://www.rankwise.net/',
    #         'https://variety.com/',
    #         'https://www.goodreads.com/',
    #         'https://www.mondadoristore.it/',
    #         'https://www.ibs.it/',
    #         'https://www.unilibro.it/',
    #         'https://deadline.com/',
    #         'https://www.barnesandnoble.com/',
    #         'https://www.mondadoristore.it/',
    #         'https://www.libreriauniversitaria.it/',
    #         'https://libri.cx/',
    #         'https://catalogo.fondazionelia.org/',
    #         'https://sites.google.com/',
    #         'https://www.libraccio.it/',
    #         'https://www.bookrepublic.it/',
    #         'https://www.cnet.com/',
    #         'https://store.streetlib.com/',
    #         'https://www.moviemeter.nl/',
    #         'https://books.google.com/',
    #         'https://www.buchpreis24.de/',
    #         'https://www.kobo.com/',
    #         'https://www.newtoncompton.com/',
    #         'http://www.espn.com/',
    #         'https://www.kobo.com/',
    #         'https://www.libraccio.it/',
    #         'https://www.mlolplus.it/',
    #         'https://archive.org/',
    #         'https://thebiglead.com/',
    #         'https://news.yahoo.com/',
    #         'https://play.google.com/',
    #         'https://www.kobo.com/',
    #         'https://archive.org/',
    #         'https://www.unilibro.it/',
    #         'https://www.ilounge.com/',
    #         'https://www.ge.infn.it/',
    #         'https://www.slashgear.com/',
    #         'https://www.dailymail.co.uk/',
    #         'https://deadline.com/',
    #         'https://www.find-more-books.com/',
    #         'https://www.dailymail.co.uk/',
    #         'https://www.eurolibro.it/',
    #         'https://www.powerlineblog.com/',
    #         'http://grandvalleystate.org/',
    #         'https://www.wsfa.com/']

    for url in urls:
        print('url: '+str(url))
        try:
            domain_url = text_parser.extract_domain_from_url(url)
            future = producer.send(OUTPUT_TOPIC, value=str(domain_url))
            result = future.get(timeout=60)
            print("Sent message: "+str(domain_url))
            sleep(SLEEP_TIME)
        except Exception as ex:
            print('Problem to Send message')