from confluent_kafka import Consumer
from confluent_kafka.serialization import IntegerDeserializer, StringDeserializer
import os
from dotenv import load_dotenv
from json import loads

load_dotenv()


def sasl_config():
    return {
        'sasl.mechanisms': os.getenv("SSL_MECHANISM"),
        'bootstrap.servers': os.getenv('BOOTSTRAP_SERVER'),
        'security.protocol': os.getenv("SECURITY_PROTOCOL"),
        'sasl.username': os.getenv("API_KEY"),
        'sasl.password': os.getenv("API_SECRET_KEY"),
        'group.id': os.getenv('GROUP_ID'),
        'auto.offset.reset': os.getenv("AUTO_OFFSET_RESET")

    }


def main():
    my_topic = "topic_0"
    consumer = Consumer(sasl_config())
    consumer.subscribe([my_topic])
    strdeserial = StringDeserializer(codec='utf-8')
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        value = loads(strdeserial(msg.value()))
        print(value, msg.offset(), msg.partition())
    consumer.close()


main()
