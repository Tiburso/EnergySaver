from confluent_kafka import Consumer, KafkaException, KafkaError
import sys

running = True


def msg_process(msg):
    print("Received message: {0}".format(msg.value().decode("utf-8")))


def basic_consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)

        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write(
                        "%% %s [%d] reached end at offset %d\n"
                        % (msg.topic(), msg.partition(), msg.offset())
                    )
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                msg_process(msg)
    except KeyboardInterrupt:
        sys.stderr.write("%% Aborted by user\n")
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


def main():
    conf = {
        "bootstrap.servers": "localhost:29092",
        "group.id": "python-consumer",
        "auto.offset.reset": "earliest",
    }

    consumer = Consumer(conf)
    topics = ["test"]
    basic_consume_loop(consumer, topics)


main()
