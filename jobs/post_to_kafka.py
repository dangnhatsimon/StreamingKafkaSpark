# Method posts events to Kafka Server
# run command in kafka server to create topic : 
# ./usr/bin/kafka-topics --create --topic device_data --bootstrap-server kafka:9092 
from kafka import KafkaProducer, KafkaConsumer
import time
import random
from device_events import generate_events
import uuid

__bootstrap_server = "kafka:29092"


def post_to_kafka(data):
    producer = KafkaProducer(bootstrap_servers=__bootstrap_server)
    producer.send('device-data', key=bytes(str(uuid.uuid4()), 'utf-8'), value=data)
    #producer.flush()
    producer.close()


if __name__ == "__main__":
    _offset = 10000
    while True:
        post_to_kafka(bytes(str(generate_events(offset=_offset)), 'utf-8'))
        time.sleep(random.randint(0, 5))
        _offset += 1