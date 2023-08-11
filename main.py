"""Main entrypoint for this application"""
import json
import atexit
import logging
import warnings
from typing import List, Tuple, Any
from datetime import datetime

import parse
from environs import Env
from streamz import Stream
from paho.mqtt.client import Client as MQTT, MQTTv5, MQTTMessage
from paho.mqtt.properties import Properties
from paho.mqtt.packettypes import PacketTypes
from psycopg import sql
from psycopg_pool import ConnectionPool

import streamz_nodes  # pylint: disable=unused-import


# Reading config from environment variables
env = Env()

MQTT_BROKER_HOST = env("MQTT_BROKER_HOST")
MQTT_BROKER_PORT = env.int("MQTT_BROKER_PORT", 1883)
MQTT_CLIENT_ID = env("MQTT_CLIENT_ID", None)
MQTT_TRANSPORT = env("MQTT_TRANSPORT", "tcp")
MQTT_TLS = env.bool("MQTT_TLS", False)
MQTT_CLEAN_START = env.bool("MQTT_CLEAN_START", True)
MQTT_SESSION_EXPIRY_INTERVAL = env.int("MQTT_SESSION_EXPIRY_INTERVAL", None)
MQTT_USER = env("MQTT_USER", None)
MQTT_PASSWORD = env("MQTT_PASSWORD", None)
MQTT_SUBSCRIBE_TOPIC = env("MQTT_SUBSCRIBE_TOPIC")
MQTT_SUBSCRIBE_TOPIC_QOS = env.int("MQTT_SUBSCRIBE_TOPIC_QOS", 0)

PG_CONNECTION_STRING = env("PG_CONNECTION_STRING")
PG_TABLE_NAME = env("PG_TABLE_NAME")
PG_POOL_SIZE = env.int("PG_POOL_SIZE", 1)

TOPIC_PARSER_FORMAT = env("TOPIC_PARSER_FORMAT")
PAYLOAD_MAP_FORMAT = env.dict("PAYLOAD_MAP_FORMAT")

PARTITION_SIZE = env.int("PARTITION_SIZE", 25)
PARTITION_TIMEOUT = env.int("PARTITION_TIMEOUT", 5)

DISCARD_NULL_VALUES = env.bool("DISCARD_NULL_VALUES", False)

LOG_LEVEL = env.log_level("LOG_LEVEL", logging.WARNING)

# Setup logger
logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s %(message)s", level=LOG_LEVEL
)
logging.captureWarnings(True)
warnings.filterwarnings("once")
LOGGER = logging.getLogger("pontos-data-ingestor")

# Create mqtt client and confiure it according to configuration
mq = MQTT(client_id=MQTT_CLIENT_ID, transport=MQTT_TRANSPORT, protocol=MQTTv5)
mq.username_pw_set(MQTT_USER, MQTT_PASSWORD)
if MQTT_TLS:
    mq.tls_set()

mq.enable_logger(logging.getLogger("pontos-data-ingestor.mqtt"))

# Create pg client (using a connection pool) but defer connections until later
pool = ConnectionPool(
    PG_CONNECTION_STRING,
    min_size=PG_POOL_SIZE,
    max_size=PG_POOL_SIZE,
    open=False,
    kwargs={"autocommit": True},  # To ensure we make all INSERTs persistent
)

# Create SQL insert statement
SQL_INSERT_STATEMENT = sql.SQL(
    "INSERT INTO {table} VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING"
).format(table=sql.Identifier(*PG_TABLE_NAME.split(".")))
TOPIC_PARSER = parse.compile(TOPIC_PARSER_FORMAT)


@mq.connect_callback()
def on_connect(
    client, userdata, flags, reason_code, props=None
):  # pylint: disable=unused-argument
    """Subscribe on connect"""
    if reason_code != 0:
        LOGGER.error(
            "Connection failed to %s with reason code: %s", client, reason_code
        )
        return

    client.subscribe(MQTT_SUBSCRIBE_TOPIC, qos=MQTT_SUBSCRIBE_TOPIC_QOS)


@mq.disconnect_callback()
def on_disconnect(
    client, userdata, flags, reason_code, props=None
):  # pylint: disable=unused-argument
    """Subscribe on connect"""
    if reason_code != 0:
        LOGGER.error("Disconnected from %s with reason code: %s", client, reason_code)


#### Processing functions ####


def extract_values_from_message(message: MQTTMessage) -> Tuple[Any]:
    """Convert an MQTT message to a sql statement"""

    LOGGER.debug("Converting MQTTMessage to tuple of values")

    fields = {}

    # Topic handling
    res = TOPIC_PARSER.parse(message.topic)
    if not res:
        raise ValueError(
            f"Topic: {message.topic} did not match TOPIC_PARSER_FORMAT: {TOPIC_PARSER_FORMAT}"
        )
    fields.update(res.named)

    # Payload handling
    payload = json.loads(message.payload)
    fields.update(
        {
            field_key: payload[payload_key]
            for field_key, payload_key in PAYLOAD_MAP_FORMAT.items()
        }
    )

    LOGGER.debug("Extracted fields: %s", fields)

    # Create parameter_id from tag and index
    fields["parameter_id"] = f"{fields.pop('tag')}_{fields.pop('index')}"

    # Convert timestamp into datetime format
    fields["timestamp"] = datetime.fromtimestamp(float(fields["timestamp"]))

    LOGGER.debug("After conversions: %s", fields)

    # Convert to tuple
    output = tuple(
        fields[key] for key in ("timestamp", "vessel_id", "parameter_id", "value")
    )

    if DISCARD_NULL_VALUES and None in output:
        raise TypeError(f"Fields of NoneType found! {fields}")

    LOGGER.debug("Tuple for database insertion: %s", output)

    return output


def batch_insert_to_db(batch: List[Tuple[Any]]):
    """Insert batches of SQL statements"""
    LOGGER.debug("Inserting %d statements into database", len(batch))
    with pool.connection() as conn:
        conn.cursor().executemany(SQL_INSERT_STATEMENT, batch)


if __name__ == "__main__":
    # Setup pipeline
    pipe = Stream()

    values_extractor = pipe.map(extract_values_from_message)
    values_extractor.on_exception(logger=LOGGER)

    batcher = values_extractor.partition(PARTITION_SIZE, timeout=PARTITION_TIMEOUT)
    db_sink = batcher.sink(batch_insert_to_db)
    db_sink.on_exception(logger=LOGGER)

    @mq.message_callback()
    def push_to_pipe(client, userdata, message):  # pylint: disable=unused-argument
        """Push each received mqtt message down the processig pipe"""
        LOGGER.debug(
            "Received mqtt message on topic %s with payload %s",
            message.topic,
            message.payload,
        )
        pipe.emit(message)

    # Connect to broker
    LOGGER.info("Connecting to MQTT broker %s %d", MQTT_BROKER_HOST, MQTT_BROKER_PORT)

    # Construct properties
    properties = None  # pylint: disable=invalid-name
    if MQTT_SESSION_EXPIRY_INTERVAL:
        properties = Properties(PacketTypes.CONNECT)
        properties.SessionExpiryInterval = MQTT_SESSION_EXPIRY_INTERVAL

    # Connect!
    mq.connect(
        MQTT_BROKER_HOST,
        MQTT_BROKER_PORT,
        clean_start=MQTT_CLEAN_START,
        properties=properties,
    )

    LOGGER.info(
        "Connecting to %s using %d pooled connections",
        PG_CONNECTION_STRING,
        PG_POOL_SIZE,
    )
    pool.open(wait=True)

    # Close pool appropriately on application exit
    atexit.register(pool.close)

    LOGGER.info("All setup done, lets start processing messages!")
    mq.loop_forever()
