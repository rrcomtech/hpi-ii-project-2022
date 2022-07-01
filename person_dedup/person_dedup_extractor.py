import logging

from general.producer import Producer
from person_consumer import PersonConsumer

from build.gen.bakdata.union.v1.dup_person_pb2 import Dup_Person
from build.gen.bakdata.union.v1.dedup_person_pb2 import DeDup_Person

from constant import TOPIC_DUPPERSON, TOPIC_DEDUPPERSON

log = logging.getLogger(__name__)


class PersonDedupExtractor:
    def __init__(self):
        self.dup_producer = Producer(Dup_Person, TOPIC_DUPPERSON)
        self.dedup_producer = Producer(DeDup_Person, TOPIC_DEDUPPERSON)
        

    def extract(self):
        msg = PersonConsumer().consume()
        log.info(msg)
        exit(0)
