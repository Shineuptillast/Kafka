# loading the necessary libraries
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, IntegerSerializer
from json import dumps, loads
import os
from dotenv import load_dotenv

load_dotenv()


def sasl_config():
    return {'bootstrap.servers': os.getenv('BOOTSTRAP_SERVER'),
            'security.protocol': os.getenv('SECURITY_PROTOCOL'),
            'sasl.mechanisms': os.getenv('SSL_MECHANISM'),
            'sasl.username': os.getenv('API_KEY'),
            'sasl.password': os.getenv('API_SECRET_KEY')
            }


def main():
    my_topic='topic_0'
    int_serializer = IntegerSerializer()
    producer = Producer(sasl_config())
    for i in range(3201,3500):
        print(i)
        producer.produce(topic=my_topic, value=int_serializer(i))
        producer.poll(0)
    producer.flush()
main()



