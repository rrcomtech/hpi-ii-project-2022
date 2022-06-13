import logging

from pathlib import Path
from build.gen.bakdata.union.v1.union_pb2 import Union, UnionStatus
from build.gen.bakdata.union.v1.union_pb2 import Union_Bafin_detail as Bafin_detail

from constant import TOPIC_UNION
from union_consumer import UnionConsumer

from general.producer import Producer

log = logging.getLogger(__name__)


class UnionExtractor:
    def __init__(self, csv_path: Path):
        self.csv_path = csv_path
        self.producer = Producer(Union, TOPIC_UNION)
        

    def extract(self):
        log.info("Union Extractor started")
        # Consume events from kafka, join corporate and bafin schema to union schema and produce it to kafka again
        """
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
        """

        cons = UnionConsumer()
        msgs = cons.consume()

        bafin_events = msgs["bafin-events"]
        bafin_persons = msgs["bafin-persons"]
        bafin_corporates = msgs["bafin-corporates"]
        rb_corporates = msgs["rb-corporates"]
        rb_persons = msgs["rb-persons"]

        union = {}

        for bafin_event in bafin_events:
            union[bafin_event.issuer] = Union()
            union[bafin_event.issuer].corporateName = bafin_event.issuer
            union[bafin_event.issuer].bafin_id = str(bafin_event.issuer_id)
            union[bafin_event.issuer].bafin_domicile = bafin_event.domicile
            union[bafin_event.issuer].bafin_country = bafin_event.country

        for bafin_person in bafin_persons:
            if bafin_person.issuer in union:
                detail = Bafin_detail()
                detail.reportable = bafin_person.title + " " + bafin_person.lastname + ", " + bafin_person.firstname
                detail.reportable_id = bafin_person.reportable_id
                detail.reportable_domicile = "unknown"
                detail.reportable_country = "unknown"
                detail.rights_33_34 = bafin_person.rights_33_34
                detail.rights_38 = bafin_person.rights_38
                detail.rights_39 = bafin_person.rights_39
                detail.publishing_date = bafin_person.publishing_date

                # It needs to be done like this for repeated fields for some reason ...
                union[bafin_person.issuer].bafin_detail.append(detail)

        for bafin_corporate in bafin_corporates:
            if bafin_corporate.issuer in union:
                detail = Bafin_detail()
                detail.reportable = bafin_corporate.reportable
                detail.reportable_id = bafin_corporate.reportable_id
                detail.reportable_domicile = bafin_corporate.reportable_domicile
                detail.reportable_country = bafin_corporate.reportable_country
                detail.rights_33_34 = bafin_corporate.rights_33_34
                detail.rights_38 = bafin_corporate.rights_38
                detail.rights_39 = bafin_corporate.rights_39
                detail.publishing_date = bafin_corporate.publishing_date

                # It needs to be done like this for repeated fields for some reason ...
                union[bafin_corporate.issuer].bafin_detail.append(detail)

        for rb_corp in rb_corporates:
            if rb_corp.company_name not in union:
                union[rb_corp.company_name] = Union()

            union[rb_corp.company_name].rb_id = str(rb_corp.rb_id)
            union[rb_corp.company_name].rb_reference_id = str(rb_corp.reference_id)
            union[rb_corp.company_name].rb_event_data = rb_corp.event_date
            union[rb_corp.company_name].rb_event_type = rb_corp.event_type
            union[rb_corp.company_name].rb_status = rb_corp.status
            union[rb_corp.company_name].rb_information = rb_corp.information

        print("Producing to Kafka ...")
        producer = Producer(Union, TOPIC_UNION)
        for union_obj in union:
            key = ""
            if union[union_obj].bafin_id:
                key = union[union_obj].bafin_id
            else:
                key = union[union_obj].rb_id
            producer.produce_to_topic(union[union_obj], key)
