from kafka import KafkaProducer
import json

p = KafkaProducer(
    bootstrap_servers=["localhost:29092"],
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
)

data = {"name": "blahbasdas", "age": 12, "type": "dog"}
p.send("Tutorial2.pets", value=data)
p.flush()
