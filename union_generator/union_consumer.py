import logging

from rb_crawler.constant import TOPIC_RB_CORPORATE, TOPIC_RB_PERSON
from rights_bafin_crawler.constant import TOPIC_BAFIN_CORPORATE, TOPIC_BAFIN_PERSON, TOPIC_BAFIN

from build.gen.bakdata.bafin.v1.bafin_pb2 import Bafin_Issuer
from build.gen.bakdata.bafin.v1.bafin_corporate_pb2 import Bafin_Reportable_Corp
from build.gen.bakdata.bafin.v1.bafin_person_pb2 import Bafin_Reportable_Person

from build.gen.bakdata.rb.v1.rb_corporate_pb2 import RB_Corporate
from build.gen.bakdata.rb.v1.rb_person_pb2 import RB_Person

from general.consumer import consume_topic

log = logging.getLogger(__name__)

class UnionConsumer:

    def consume(self):
        msg_bafin_events = consume_topic(TOPIC_BAFIN, Bafin_Issuer)
        log.info("Bafin-Events consumed.")
        msg_bafin_persons = consume_topic(TOPIC_BAFIN_PERSON, Bafin_Reportable_Person)
        log.info("Bafin-Persons consumed.")
        msg_bafin_corporates = consume_topic(TOPIC_BAFIN_CORPORATE, Bafin_Reportable_Corp)
        log.info("Bafin-Coporates consumed.")
        msg_rb_corporates = consume_topic(TOPIC_RB_CORPORATE, RB_Corporate)
        log.info("HRB-Corporates consumed.")
        msg_rb_persons = consume_topic(TOPIC_RB_PERSON, RB_Person)
        log.info("HRB-Persons consumed.")

        # Example for using the values:
        # print(str(msg_bafin_events[0].issuer))

        return {
            "bafin-events": msg_bafin_events,
            "bafin-persons": msg_bafin_persons,
            "bafin-corporates": msg_bafin_corporates,
            "rb-corporates": msg_rb_corporates,
            "rb-persons": msg_rb_persons
        }