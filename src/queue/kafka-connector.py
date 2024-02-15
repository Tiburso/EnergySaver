from kafka import KafkaProducer
import socket

conf = {"bootstrap_servers": "localhost:29092", "client_id": socket.gethostname()}

producer = KafkaProducer(**conf)

producer.send("test", b"Hello, World!", key=b"key")
producer.flush()
