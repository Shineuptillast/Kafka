from confluent_kafka import Producer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import os
from dotenv import load_dotenv
from uuid import uuid4
import pandas as pd
load_dotenv()
col= ["Date","Year","Month","Customer_Age","Customer_Gender","Country","State","Product_Category","Sub_Category","Quantity","Unit Cost","Unit Price","Cost","Revenue"]
class Stock():
    def __init__(self, record):
        for k, v in record.items():
            setattr(self, k, v)
        self.record = record
    @staticmethod
    def dict_to_Stock(record, ctx):
        return Stock(record=record)
    def __str__(self):
        return f"{self.record}"

def stock_to_dict(stock:Stock, ctx):
    return stock.record

def stock_get_instance(path):
    df = pd.read_csv(path)
    #df = df.fillna(value=0)
    df=df.iloc[:,1:]
    stocks=[]
    for data in df.values:
        stock = Stock(dict(zip(col,data)))

        stocks.append(stock)
        yield stock

def delivery_report(err,msg):
    if err is None:
        print(msg.key(), msg.topic(),  msg.partition(), msg.offset())
    elif err is not None:
        return



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
            'sasl.password': os.getenv('API_SECRET_KEY'),
            'compression.type': 'lz4',
            'linger.ms':140,
            'batch.size':147456
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
      "name": "Date",
      "type": "string",
      "doc": "The int type is a 32-bit signed integer.",
      "default": "null"
    },
    {
      "name": "Year",
      "type": "int",
      "doc": "The double type is a double precision (64-bit) IEEE 754 floating-point number.",
      "default": -1
    },
    {
      "name": "Month",
      "type": "string",
      "doc": "The string is a unicode character sequence.",
      "default": "null"
    },
    {
      "name": "Customer_Age",
      "type": "int",
      "doc": "The string is a unicode character sequence.",
      "default": -1
    },
    {
      "name": "Customer_Gender",
      "type": "string",
      "doc": "The string is a unicode character sequence.",
      "default": "null"
    },
    {
      "name": "Country",
      "type": "string",
      "doc": "The string is a unicode character sequence.",
      "default":" null"
    },
    {
      "name": "State",
      "type": "string",
      "doc": "The string is a unicode character sequence.",
      "default": "null"
    },
    {
      "name": "Product_Category",
      "type": "string",
      "doc": "The string is a unicode character sequence.",
      "default": "null"
    },
    {
      "name": "Sub_Category",
      "type": "string",
      "doc": "The string is a unicode character sequence.",
      "default": "null"
    },
    {
      "name": "Quantity",
      "type": "int",
      "doc": "The string is a unicode character sequence.",
      "default": -1
    },
    {
      "name": "Unit_Cost",
      "type": "int",
      "doc": "The string is a unicode character sequence.",
      "default": -1
    },
    {
      "name": "Unit_Price",
      "type": "double",
      "doc": "The string is a unicode character sequence.",
      "default": -1.0
    },
    {
      "name": "Cost",
      "type": "int",
      "doc": "The string is a unicode character sequence.",
      "default": -1
    },
    {
      "name": "Revenue",
      "type": "int",
      "doc": "The string is a unicode character sequence.",
      "default": -1
    }
  ]
}
    """
    schema_registry_client = SchemaRegistryClient(schema_config())
    avro_serializer = AvroSerializer(schema_registry_client, schema_str, to_dict=stock_to_dict)
    str_serializer= StringSerializer('utf-8')
    producer = Producer(sasl_config())
    producer.poll(0)
    for i in stock_get_instance("./data.csv"):
        print(i)
        producer.produce(my_topic,key=str_serializer(str(uuid4()) ), value=avro_serializer(i, SerializationContext( my_topic, MessageField.VALUE)), on_delivery=delivery_report)
        producer.poll(0.002)
    producer.flush(1000)


main()