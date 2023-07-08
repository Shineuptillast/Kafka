from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer
from confluent_kafka.schema_registry.avro import AvroDeserializer
import os
from dotenv import load_dotenv

load_dotenv()


def schema_config():
    return {
        'url': os.getenv("ENDPOINT_URL"),
        'basic.auth.user.info': f"{os.getenv('SCHEMA_REGISTRY_API_KEY')}:{os.getenv('SCHEMA_REGISTRY_API_SECRET_KEY')}"
    }


def sasl_config():
    return {
        "bootstrap.servers": os.getenv("BOOTSTRAP_SERVER"),
        "security.protocol": os.getenv("SECURITY_PROTOCOL"),
        "sasl.mechanisms": os.getenv("SSL_MECHANISM"),
        "sasl.username": os.getenv("API_KEY"),
        "sasl.password": os.getenv("API_SECRET_KEY"),
        "group.id": os.getenv("GROUP_ID"),
        "auto.offset.reset": os.getenv("AUTO_OFFSET_RESET")
    }


class Stock:
    def __init__(self, record):
        for k, v in record.items():
            setattr(self, k, v)
        self.record = record

    @staticmethod
    def dict_to_car(record, ctx):
        return Stock(record)

    def __str__(self):
        print(f"{self.record}")


def main():
    myTopic = 'topic_1'
    consumer = Consumer(sasl_config())
    schema_registry_client = SchemaRegistryClient(schema_config())
    avro_schema = schema_registry_client.get_latest_version(myTopic + "-" + MessageField.VALUE)
    schema = avro_schema.schema
    print(schema)
    avrodeserializer = AvroDeserializer(schema_registry_client, schema, from_dict=Stock.dict_to_car)

    consumer.subscribe([myTopic])
    while True:
        try:
            msg = consumer.poll(1.0)

            if msg is not None:
                value = avrodeserializer(msg.value(), SerializationContext(myTopic, MessageField.VALUE))
                if value is not None:
                    print(value)
            elif msg is None:
                continue
        except Exception as e:
            print("Error: ", e)

    consumer.close()

main()