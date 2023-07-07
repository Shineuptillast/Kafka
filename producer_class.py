from confluent_kafka import Producer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import os
from dotenv import load_dotenv
from uuid import uuid4

load_dotenv()

def schema_config():
    return {
        "url": os.getenv("ENDPOINT_URL"),
        "basic.auth.user.info": f"{os.getenv('SCHEMA_REGISTRY_API_KEY')}:{os.getenv('SCHEMA_REGISTRY_API_SECRET_KEY')}"
    }

def sasl_config():
    return {'bootstrap.servers': os.getenv('BOOTSTRAP_SERVER'),
            'security.protocol': os.getenv('SECURITY_PROTOCOL'),
            'sasl.mechanisms': os.getenv('SSL_MECHANISM'),
            'sasl.username': os.getenv('API_KEY'),
            'sasl.password': os.getenv('API_SECRET_KEY')
            }


def main():
    my_topic="topic_1"
    schema_str = """
    {
  "type": "record",
  "namespace": "com.mycorp.mynamespace",
  "name": "Avro_Serializer_Testing",
  "doc": "Sample schema to help you get started.",
  "fields": [
    {
      "name": "data",
      "type": "int",
      "doc": "The int type is a 32-bit signed integer."
    },
    {
      "name": "name",
      "type": "string",
      "doc": "The double type is a double precision (64-bit) IEEE 754 floating-point number."
    },
    {
      "name": "Skills",
      "type": "string",
      "doc": "The string is a unicode character sequence."
    }
  ]
}
    """
    schema_registry_client = SchemaRegistryClient(schema_config())
    avro_serializer = AvroSerializer(schema_registry_client, schema_str, to_dict=None)
    str_serializer= StringSerializer('utf-8')
    producer = Producer(sasl_config())
    for i in range(1, 5000):
        data = {
            "data": i,
            "name": f"Naman{i}",
            "Skills": "Hadoop"
        }
        producer.produce(my_topic,key=str_serializer(str(uuid4()),SerializationContext( my_topic, MessageField.KEY) ),  value=avro_serializer(data, SerializationContext( my_topic, MessageField.VALUE)))
        producer.poll(0)
    producer.flush()

main()