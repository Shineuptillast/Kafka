from confluent_kafka import Consumer
from confluent_kafka.serialization import IntegerDeserializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer

from dotenv import load_dotenv
import os

load_dotenv()
schema_str = """
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
def schema_config():
    return { "url": os.getenv("ENDPOINT_URL"),
        "basic.auth.user.info": f"{os.getenv('SCHEMA_REGISTRY_API_KEY')}:{os.getenv('SCHEMA_REGISTRY_API_SECRET_KEY')}"}
def sasl_config():
    return {
        'sasl.mechanisms': os.getenv('SSL_MECHANISM'),
        'security.protocol': os.getenv('SECURITY_PROTOCOL'),
        'sasl.username': os.getenv('API_KEY'),
        'sasl.password': os.getenv('API_SECRET_KEY'),
        'bootstrap.servers': os.getenv('BOOTSTRAP_SERVER'),
        'group.id': os.getenv("GROUP_ID"),
        'auto.offset.reset': os.getenv("AUTO_OFFSET_RESET")

    }

def main():
    my_topic = 'topic_0'
    schema_registry_de = SchemaRegistryClient(schema_config())
    schema_iuy = schema_registry_de.get_latest_version("topic_0-value")
    json_des = JSONDeserializer(schema_iuy.schema)
    consumer2 = Consumer(sasl_config())
    consumer2.subscribe([my_topic])
    while True:
        try:
            msg = consumer2.poll(1.0)

            if (msg is not None):
                value = json_des(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
                print(value, msg.offset(), msg.partition())
            else:
                continue

        except Exception as e:
            print("ERROR: ", e)
    consumer2.close()


main()
