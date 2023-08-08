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


def publish_single_row(epoch: int):
    publish.single(
        "PONTOS/test_vessel/test_parameter/1",
        json.dumps({"epoch": epoch, "sensor_value": 42}),
        hostname="localhost",
        port=1883,
    )


def test_publish_to_single_topic(compose):
    # Adding a single row to the db via mqtt
    publish_single_row(0)

    # We should not have written to db yet
    rows = fetch_all_rows()
    assert len(rows) == 0

    time.sleep(6)

    # Trying again, this time we should have one
    rows = fetch_all_rows()
    assert len(rows) == 1

    # Publish 30 messages
    for ix in range(1, 31):
        publish_single_row(ix)

    time.sleep(1)

    # Partition size is 25 by default, hence we
    # should now have 26 rows in the db
    rows = fetch_all_rows()
    assert len(rows) == 26

    time.sleep(6)

    # Waiting some more should yield the additional lines
    rows = fetch_all_rows()
    assert len(rows) == 31

    ## Duplicates

    # Publish 15 more duplicates
    for _ in range(15):
        publish_single_row(0)

    # Publish another non-duplicate message
    publish_single_row(31)

    # Publish 15 more duplicates
    for _ in range(15):
        publish_single_row(0)

    time.sleep(1)

    # Partition size is 25 by default, hence we
    # should now have 32 rows in the db
    rows = fetch_all_rows()
    assert len(rows) == 32

    time.sleep(6)

    # Waiting some more should still only show 22 rows
    rows = fetch_all_rows()
    assert len(rows) == 32
