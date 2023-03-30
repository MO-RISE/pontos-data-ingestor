import json
from datetime import datetime

import pytest
from paho.mqtt.client import MQTTMessage

from main import extract_values_from_message


def test_extract_values_from_message():
    msg = MQTTMessage()
    msg.topic = "PONTOS/1/2".encode()
    msg.payload = json.dumps({"timestamp": 12345678, "value": 3})

    res = extract_values_from_message(msg)
    assert res == (datetime.fromtimestamp(12345678), "1", "2", 3)

    msg = MQTTMessage()
    msg.topic = "PONTOS/1/2/3".encode()
    msg.payload = json.dumps({"timestamp": 12345678, "value": 3})

    with pytest.raises(TypeError):
        extract_values_from_message(msg)

    msg = MQTTMessage()
    msg.topic = "PONTOS/1/2".encode()
    msg.payload = "non_json_value"

    with pytest.raises(json.decoder.JSONDecodeError):
        extract_values_from_message(msg)

    msg = MQTTMessage()
    msg.topic = "PONTOS/1/2".encode()
    msg.payload = json.dumps({"timestamp": 12345678, "unknown_key": 3})

    with pytest.raises(KeyError):
        extract_values_from_message(msg)

    msg = MQTTMessage()
    msg.topic = "PONTOS/1/2".encode()
    msg.payload = json.dumps({"unknown_key": 12345678, "value": 3})

    with pytest.raises(KeyError):
        extract_values_from_message(msg)
