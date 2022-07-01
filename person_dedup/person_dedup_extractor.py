import logging

from general.producer import Producer
from person_consumer import PersonConsumer


log = logging.getLogger(__name__)


class PersonDedupExtractor:
    def __init__(self):
        self.bafin_issuer_producer = Producer(Bafin_Issuer, TOPIC_BAFIN)
        

    def extract(self):
        msg = PersonConsumer().consume()
        exit(0)
