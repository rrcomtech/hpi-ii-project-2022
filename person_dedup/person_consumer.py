import logging

from rb_crawler.constant import TOPIC_RB_PERSON
from rights_bafin_crawler.constant import TOPIC_BAFIN_PERSON

from build.gen.bakdata.bafin.v1.bafin_person_pb2 import Bafin_Reportable_Person
from build.gen.bakdata.rb.v1.rb_person_pb2 import RB_Person

from general.consumer import consume_topic

from person_dedup.constant import GROUP_ID

log = logging.getLogger(__name__)

class PersonConsumer:

    def consume(self):
        msg_bafin_persons = consume_topic(TOPIC_BAFIN_PERSON, Bafin_Reportable_Person, GROUP_ID)
        log.info("Bafin-Persons consumed.")
        msg_rb_persons = consume_topic(TOPIC_RB_PERSON, RB_Person, GROUP_ID)
        log.info("HRB-Persons consumed.")

        # Example for using the values:
        # print(str(msg_bafin_events[0].issuer))

        return {
            "bafin-persons": msg_bafin_persons,
            "rb-persons": msg_rb_persons
        }

