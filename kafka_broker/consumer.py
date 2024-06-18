from confluent_kafka import DeserializingConsumer
import os
import sys

sys.path.insert(0,os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.functions import decode_avro
from utils.constant import avro_schema

def consume():
    consumer_conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'trading_data',
        'auto.offset.reset': 'earliest'
    }

    consumer = DeserializingConsumer(consumer_conf)
    consumer.subscribe(['trading_data'])

    while True:
        try:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                print(f'error: {message.error()}')
            else:
                key = message.key()
                value = message.value()
                
                # Assuming value is Avro encoded bytes
                decoded_value = decode_avro(avro_schema, value)
                
                print(f'key: {key}, value: {decoded_value}')
        except KeyboardInterrupt:
            break

    consumer.close()

if __name__ == "__main__":
    consume()
