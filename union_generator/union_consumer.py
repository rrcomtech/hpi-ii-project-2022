import json
import logging
import sys

from confluent_kafka import DeserializingConsumer, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer
from confluent_kafka.serialization import StringDeserializer

from general.producer import Producer

from union_generator.constant import BOOTSTRAP_SERVER, GROUP_ID, SCHEMA_REGISTRY_URL, TOPIC_UNION
from rb_crawler.constant import TOPIC_RB_CORPORATE, TOPIC_RB_PERSON
from rights_bafin_crawler.constant import TOPIC_BAFIN_CORPORATE, TOPIC_BAFIN_PERSON, TOPIC_BAFIN

from build.gen.bakdata.bafin.v1.bafin_pb2 import Bafin_Issuer
from build.gen.bakdata.bafin.v1.bafin_corporate_pb2 import Bafin_Reportable_Corp
from build.gen.bakdata.bafin.v1.bafin_person_pb2 import Bafin_Reportable_Person

from build.gen.bakdata.rb.v1.rb_corporate_pb2 import RB_Corporate, RB_Status
from build.gen.bakdata.rb.v1.rb_person_pb2 import RB_Person
from build.gen.bakdata.union.v1.union_pb2 import Union, Union_Bafin_detail


log = logging.getLogger(__name__)

class UnionConsumer:

    def consume(self):

        msg_integrated = {}

        msg_bafin_events = self.consume_topic(TOPIC_BAFIN, Bafin_Issuer)
        print("Bafin-Events consumed.")
        msg_bafin_persons = self.consume_topic(TOPIC_BAFIN_PERSON, Bafin_Reportable_Person)
        print("Bafin-Persons consumed.")
        msg_bafin_corporate = self.consume_topic(TOPIC_BAFIN_CORPORATE, Bafin_Reportable_Corp)
        print("Bafin-Coporates consumed.")
        msg_rb_corporate = self.consume_topic(TOPIC_RB_CORPORATE, RB_Corporate)
        print("HRB-Corporates consumed.")
        msg_rb_persons = self.consume_topic(TOPIC_RB_PERSON, RB_Person)
        print("HRB-Persons consumed.")

        for bafin_event in msg_bafin_events:
            msg_integrated[bafin_event.issuer] = Union()
            msg_integrated[bafin_event.issuer].corporateName = bafin_event.issuer
            msg_integrated[bafin_event.issuer].bafin_id = str(bafin_event.issuer_id)
            msg_integrated[bafin_event.issuer].bain_domicile = bafin_event.domicile
            msg_integrated[bafin_event.issuer].bafin_country = bafin_event.country

        for bafin_person in msg_bafin_persons:
            msg_integrated[bafin_person.issuer].bafin_detail = None

        print("Producing to Kafka ...")
        producer = Producer(Union, TOPIC_UNION)
        for union in msg_integrated:
            producer.produce_to_topic(union, union.bafin_id)

        # Example for using the values:
        # print(str(msg_bafin_events[0].issuer))

    def consume_topic(self, topic, schema):
        schema_registry_conf = {"url": SCHEMA_REGISTRY_URL}
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)

        deserializer = ProtobufDeserializer(
            schema, {"use.deprecated.format": True}
        )

        string_deserializer = StringDeserializer("utf_8")
        config = {
            'bootstrap.servers': BOOTSTRAP_SERVER,
            'key.deserializer': string_deserializer,
            'value.deserializer': deserializer,
            'group.id': GROUP_ID,
            'session.timeout.ms': 6000,
            'auto.offset.reset': 'earliest'
        }

        c = DeserializingConsumer(conf=config)
        c.subscribe([topic])

        messages = []
        try:
            while True:
                msg = c.poll(timeout=10.0)
                if msg is None:
                    break
                if msg.error():
                    print("Error encountered ...")
                else:
                    messages.append(msg.value())
        except KeyboardInterrupt:
            sys.stderr.write('%% Aborted by user\n')

        finally:
            c.close()

        return messages
