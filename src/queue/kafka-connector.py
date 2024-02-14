from confluent_kafka import Producer
import socket

conf = {"bootstrap.servers": "localhost:29092", "client.id": socket.gethostname()}

producer = Producer(conf)

producer.produce("test", key="key", value="value")
producer.flush()
