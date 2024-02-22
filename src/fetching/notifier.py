import os
import logging
import ssl
import json

import paho.mqtt.client as mqtt
import paho.mqtt.properties as properties

from dotenv import load_dotenv

from api import OpenDataAPI
from publisher import KafkaPublisher

load_dotenv()

BROKER_DOMAIN = "mqtt.dataplatform.knmi.nl"

# Client ID should be made static, it is used to identify your session, so that
# missed events can be replayed after a disconnect
# https://www.uuidgenerator.net/version4
CLIENT_ID = os.environ.get("UUID")

# Obtain your token at: https://developer.dataplatform.knmi.nl/notification-service
NOTIFICATION_TOKEN = os.environ.get("NOTIFICATION_KEY")

TOPIC_URL = "dataplatform/file/v1"

PROTOCOL = mqtt.MQTTv5

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel("INFO")


class Notifier:
    def __init__(self, datasets: str) -> mqtt.Client:
        self.api = OpenDataAPI()
        self.publisher = KafkaPublisher()

        # Python shenenigans for file path
        cwd = os.path.dirname(__file__)
        datasets = os.path.join(cwd, datasets)

        with open(datasets, "r") as datasets:
            self.datasets = json.load(datasets)

        logger.info(
            f"Notifier initialized with datasets: {self.datasets['continuous']}"
        )

        self._connect_mqtt()

    def _connect_mqtt(self):
        def on_connect(
            c: mqtt, userdata, flags: mqtt.ConnectFlags, rc, reason_code, props=None
        ):
            logger.info(f"Connected using client ID: {str(c._client_id)}")
            logger.info(f"Session present: {str(flags.session_present)}")
            logger.info(f"Connection result: {str(rc)}")

            # Subscribe here so it is automatically done after disconnect
            for dataset in self.datasets["continuous"]:
                logger.info(f"Subscribing to dataset: {dataset['name']}")

                self.subscribe(dataset["name"], dataset["version"], dataset["topic"])

        self.client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=CLIENT_ID,
            protocol=PROTOCOL,
            transport="websockets",
        )

        self.client.tls_set(tls_version=ssl.PROTOCOL_TLS)

        # TODO: check if there are any important properties that are needed to set first
        connect_properties = properties.Properties(properties.PacketTypes.CONNECT)

        connect_properties.SessionExpiryInterval = 3600

        # The MQTT username is not used for authentication, only the token
        username = "token"
        self.client.username_pw_set(username, NOTIFICATION_TOKEN)
        self.client.on_connect = on_connect

        self.client.connect(
            host=BROKER_DOMAIN,
            port=443,
            keepalive=60,
            clean_start=False,
            properties=connect_properties,
        )

    def subscribe(self, name: str, version: str, kafka_topic: str):
        def on_message(c: mqtt.Client, userdata: None, message: mqtt.MQTTMessage):
            logger.info(
                f"Received message on topic {message.topic}: {str(message.payload)}"
            )

            # Decode the payload using the json libary
            payload = json.loads(message.payload)

            dataset_name = payload["data"]["datasetName"]
            dataset_version = payload["data"]["datasetVersion"]
            latest_file = payload["data"]["filename"]

            file_url = self.api.get_file_url(dataset_name, dataset_version, latest_file)
            ds = self.api.download_file_into_xarray(file_url)

            # Convert the xarray dataset into a json so kafka can handle it
            df = ds.to_dataframe().to_json()

            # Upload this file into a kafka topic
            self.publisher.send(kafka_topic, df)

        def on_subscribe(c: mqtt, userdata, mid, granted_qos, *other):
            logger.info(f"Subscribed to topic '{topic}'")

        topic = f"{TOPIC_URL}/{name}/{version}/#"

        self.client.on_subscribe = on_subscribe
        self.client.on_message = on_message

        # QOS=1 is used to ensure that the message is delivered at least once
        self.client.subscribe(topic, qos=1)

    def run(self):
        self.client.enable_logger(logger=logger)
        self.client.loop_forever()


def main():
    notifier = Notifier("datasets.json")
    notifier.run()


if __name__ == "__main__":
    main()
