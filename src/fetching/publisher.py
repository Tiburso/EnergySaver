from kafka import KafkaProducer
import json
import os

from dotenv import load_dotenv

load_dotenv()

BROKERS = os.environ.get("KAFKA_BROKERS")


class KafkaPublisher:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=BROKERS,
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        )

    def send(self, topic: str, value: dict):
        self.producer.send(topic, value=value)
        self.producer.flush()
