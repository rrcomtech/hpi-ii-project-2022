import logging

from pathlib import Path
from build.gen.bakdata.union.v1.union_pb2 import Union
from build.gen.bakdata.union.v1.union_pb2 import Union_Bafin_detail as Bafin_detail

from constant import TOPIC

from general.producer import Producer

log = logging.getLogger(__name__)


class UnionExtractor:
    def __init__(self, csv_path: Path):
        self.csv_path = csv_path
        self.producer = Producer(Union, TOPIC)
        

    def extract(self):
        log.info("Union Extractor started")
        # Consume events from kafka, join corporate and bafin schema to union schema and produce it to kafka again
        union = Union()
        union.corporateName     = "Test"
        union.rb_id             = "Test"
        union.bafin_id          = "Test"
        union.bafin_domicile    = "Test"
        union.bafin_country     = "Test"
        union.rb_reference_id   = "Test"
        union.rb_event_data     = "Test"
        union.rb_event_type     = "Test"
        union.rb_status         = 1
        union.rb_information    = "Test"
        union.bafin_detail.append(Bafin_detail())
        self.producer.produce_to_topic(union, union.rb_id)

