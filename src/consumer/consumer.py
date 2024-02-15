from kafka import KafkaConsumer
from kafka.errors import KafkaError
import sys


def msg_process(msg):
    print("Received message: {0}".format(msg.value().decode("utf-8")))


def basic_consume_loop(consumer):
    try:
        for msg in consumer:
            print("Received message: {0}".format(msg.value.decode("utf-8")))

    except KeyboardInterrupt:
        sys.stderr.write("%% Aborted by user\n")


def main():

    consumer = KafkaConsumer(
        "test",
        bootstrap_servers="localhost:29092",
        group_id="python-consumer",
        auto_offset_reset="earliest",
    )

    basic_consume_loop(consumer)


main()
