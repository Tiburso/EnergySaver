import os
import logging
import ssl

import paho.mqtt.client as mqtt
import paho.mqtt.properties as properties

from dotenv import load_dotenv

load_dotenv()

BROKER_DOMAIN = "mqtt.dataplatform.knmi.nl"
# Client ID should be made static, it is used to identify your session, so that
# missed events can be replayed after a disconnect
# https://www.uuidgenerator.net/version4
CLIENT_ID = os.environ.get("UUID")
# Obtain your token at: https://developer.dataplatform.knmi.nl/notification-service
TOKEN = os.environ.get("NOTIFICATION_KEY")
# This will listen to both file creation and update events of this dataset:
# https://dataplatform.knmi.nl/dataset/radar-echotopheight-5min-1-0
# This topic should have one event every 5 minutes
TOPIC = "dataplatform/file/v1/radar_echotopheight_5min/1.0/#"
# Version 3.1.1 also supported
PROTOCOL = mqtt.MQTTv5

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel("INFO")

def connect_mqtt() -> mqtt.Client:
    def on_connect(c: mqtt, userdata, flags: mqtt.ConnectFlags, rc, reason_code, props=None):
        logger.info(f"Connected using client ID: {str(c._client_id)}")
        logger.info(f"Session present: {str(flags.session_present)}")
        logger.info(f"Connection result: {str(rc)}")
        # Subscribe here so it is automatically done after disconnect
        subscribe(c, TOPIC)
        
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=CLIENT_ID, protocol=PROTOCOL, transport="websockets")
    client.tls_set(tls_version=ssl.PROTOCOL_TLS)
    connect_properties = properties.Properties(properties.PacketTypes.CONNECT)
    
    # The MQTT username is not used for authentication, only the token
    username = "token"
    client.username_pw_set(username, TOKEN)
    client.on_connect = on_connect

    client.connect(host=BROKER_DOMAIN, port=443, keepalive=60, clean_start=False)

    return client


def subscribe(client: mqtt.Client, topic: str):
    def on_message(c: mqtt.Client, userdata, message):
        # NOTE: Do NOT do slow processing in this function, as this will interfere with PUBACK messages for QoS=1.
        # A couple of seconds seems fine, a minute is definitely too long.
        
        # Here is probably where i will post into a kafka topic, check the speed first though
        logger.info(f"Received message on topic {message.topic}: {str(message.payload)}")

    def on_subscribe(c: mqtt, userdata, mid, granted_qos, *other):
        logger.info(f"Subscribed to topic '{topic}'")

    client.on_subscribe = on_subscribe
    client.on_message = on_message
    # A qos=1 will replay missed events when reconnecting with the same client ID. Use qos=0 to disable
    client.subscribe(topic, qos=1)


def run():
    client = connect_mqtt()
    client.enable_logger(logger=logger)
    client.loop_forever()


if __name__ == "__main__":
    run()