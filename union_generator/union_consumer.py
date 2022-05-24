from confluent_kafka import DeserializingConsumer
from union_generator.constant import SCHEMA_REGISTRY_URL, BOOTSTRAP_SERVER, GROUP_ID
from rb_crawler.constant import TOPIC as RB_TOPIC
from rights_bafin_crawler.constant import TOPIC as BAFIN_TOPIC

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.serialization import StringSerializer

from build.gen.bakdata.union.v1 import union_pb2
from build.gen.bakdata.union.v1.union_pb2 import Union

from build.gen.bakdata.bafin.v1 import bafin_pb2
from build.gen.bakdata.corporate.v1 import corporate_pb2


class UnionConsumer:

    def __init__(self) -> None:
        self.rb_consumer = RbConsumer()
        self.bafin_consumer = BafinConsumer()

    def consume(self):
        return {
            "rb_message": self.rb_consumer.consume(),
            "bafin_message": self.bafin_consumer.consume()
        }

#
# See code of DeserializingConsumer:
# https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/_modules/confluent_kafka/deserializing_consumer.html#DeserializingConsumer
#

class BafinConsumer:
    
    def __init__(self) -> None:
        schema_registry_conf = {"url": SCHEMA_REGISTRY_URL}
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)

        protobuf_serializer = ProtobufSerializer(
            bafin_pb2.Bafin_detail, schema_registry_client, {"use.deprecated.format": True}
        )

        producer_conf = {
            "bootstrap.servers": BOOTSTRAP_SERVER,
#            "key.serializer": StringSerializer("utf_8"),
#            "value.serializer": protobuf_serializer,
            "group.id": GROUP_ID
        }

        self.consumer = DeserializingConsumer(producer_conf)
    
    def consume(self) -> bafin_pb2.Bafin_general:
        return self.consumer.poll()


class RbConsumer:

    def __init__(self) -> None:
        schema_registry_conf = {"url": SCHEMA_REGISTRY_URL}
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)

        protobuf_serializer = ProtobufSerializer(
            corporate_pb2.Corporate, schema_registry_client, {"use.deprecated.format": True}
        )

        producer_conf = {
            "bootstrap.servers": BOOTSTRAP_SERVER,
#            "key.serializer": StringSerializer("utf_8"),
#            "value.serializer": protobuf_serializer,
            "group.id": GROUP_ID
        }

        self.consumer = DeserializingConsumer(producer_conf)

    def consume(self) -> corporate_pb2.Corporate:
        return self.consumer.poll()