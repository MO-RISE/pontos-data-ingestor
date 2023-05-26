import time
import json

import psycopg
from paho.mqtt import publish


def fetch_all_rows():
    return (
        psycopg.connect("postgresql://test_user:test_pass@localhost:5432/test")
        .execute("SELECT * FROM test_table;")
        .fetchall()
    )


def publish_single_row():
    publish.single(
        "PONTOS/test_vessel/test_parameter/1",
        json.dumps({"epoch": 12345678, "sensor_value": 42}),
        hostname="localhost",
        port=1883,
    )


def test_write_single_message_to_topic(compose):
    # Adding a single row to the db via mqtt
    publish_single_row()

    # We should not have written to db yet
    rows = fetch_all_rows()
    assert len(rows) == 0

    time.sleep(6)

    # Trying again, this time we should have one
    rows = fetch_all_rows()
    assert len(rows) == 1

    # Publish 30 messages
    for _ in range(30):
        publish_single_row()

    time.sleep(1)

    # Partition size is 25 by default, hence we
    # should now have 26 rows in the db
    rows = fetch_all_rows()
    assert len(rows) == 26

    time.sleep(6)

    # Waiting some more should yield the additional lines
    rows = fetch_all_rows()
    assert len(rows) == 31
