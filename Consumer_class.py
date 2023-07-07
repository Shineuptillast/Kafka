from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
import os
from dotenv import load_dotenv

load_dotenv()



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
def schema_config():
    return {
        'url': os.getenv("ENDPOINT_URL"),
        'basic.auth.user.info': f"{os.getenv('SCHEMA_REGISTRY_API_KEY')}:{os.getenv('SCHEMA_REGISTRY_API_SECRET_KEY')}"
    }

def main():
    mytopic='topic_1'
    schema_registry = SchemaRegistryClient(schema_config())
    schema_version = schema_registry.get_latest_version(mytopic+"-"+MessageField.VALUE)
    schema=schema_version.schema
    avrodeserial= AvroDeserializer(schema_registry, schema)
    consumer = Consumer(sasl_config())
    consumer.subscribe([mytopic])
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        else:
            value = avrodeserial(msg.value(), SerializationContext(mytopic, MessageField.VALUE))
            print(value, msg.partition(), msg.offset())
    consumer.close()

main()

