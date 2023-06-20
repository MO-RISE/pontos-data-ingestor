import json
from datetime import datetime

import pytest
from paho.mqtt.client import MQTTMessage

from main import extract_values_from_message


def test_extract_values_from_message():
    # Not correct topic format
    msg = MQTTMessage()
    msg.topic = "PONTOS/1/2".encode()
    msg.payload = json.dumps({"timestamp": 1685078782, "value": 3})

    with pytest.raises(ValueError):
        extract_values_from_message(msg)

    # Not correct keys in JSON payload
    msg = MQTTMessage()
    msg.topic = "PONTOS/1/2/3".encode()
    msg.payload = json.dumps({"timestamp": 1685078782, "sensor_value": 4})

    with pytest.raises(KeyError):
        extract_values_from_message(msg)

    # This should pass!
    msg = MQTTMessage()
    msg.topic = "PONTOS/1/2/3".encode()
    msg.payload = json.dumps({"epoch": 1685078782, "sensor_value": 4})

    res = extract_values_from_message(msg)
    assert res == (datetime.fromtimestamp(1685078782), "1", "2_3", 4)

    # Non-JSON value in payload...
    msg = MQTTMessage()
    msg.topic = "PONTOS/1/2/3".encode()
    msg.payload = "non_json_value"

    with pytest.raises(json.decoder.JSONDecodeError):
        extract_values_from_message(msg)

    msg = MQTTMessage()
    msg.topic = "PONTOS/1/2/non_digit".encode()
    msg.payload = json.dumps({"epoch": 12345678, "sensor_value": 3})

    with pytest.raises(ValueError):
        extract_values_from_message(msg)

    # Null values not accepted
    msg = MQTTMessage()
    msg.topic = "PONTOS/1/2/3".encode()
    msg.payload = json.dumps({"epoch": 1685078782, "sensor_value": None})

    with pytest.raises(TypeError):
        extract_values_from_message(msg)
