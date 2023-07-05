# loading the necessary libraries
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import StringSerializer, IntegerSerializer,SerializationContext, MessageField
from json import dumps, loads
import os

from dotenv import load_dotenv

load_dotenv()

def schema_config():
    return { "url": os.getenv("ENDPOINT_URL"),
        "basic.auth.user.info": f"{os.getenv('SCHEMA_REGISTRY_API_KEY')}:{os.getenv('SCHEMA_REGISTRY_API_SECRET_KEY')}"
    }

def sasl_config():
    return {'bootstrap.servers': os.getenv('BOOTSTRAP_SERVER'),
            'security.protocol': os.getenv('SECURITY_PROTOCOL'),
            'sasl.mechanisms': os.getenv('SSL_MECHANISM'),
            'sasl.username': os.getenv('API_KEY'),
            'sasl.password': os.getenv('API_SECRET_KEY')
            }
schema_str= """
    {
  "$schema": "http://json-schema.org/draft-07/schema#",
  "$id": "http://example.com/myURI.schema.json",
  "title": "Details",
  "description": "Sample schema to help you get started.",
  "type": "object",
  "properties": {
    
    "data": {
      "type": "number",
      "description": "The string type is used for strings of text."
    },
    "name": {
      "type": "string",
      "description": "The string type is used for strings of text."
    }  
  }
}
    """

def main():
    schema_registry_client = SchemaRegistryClient(schema_config())
    json_serializer = JSONSerializer(schema_str, schema_registry_client, to_dict=None)
    my_topic="topic_0"
    producer = Producer(sasl_config())
    for i in range(1000,5000):
        data = {"data": i,
                "name":f"Naman:{i}"}
        print(data)
        producer.produce(topic=my_topic, value=json_serializer(data,SerializationContext(my_topic, MessageField.VALUE)))
    producer.flush()
main()


