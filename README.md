# pontos-data-ingestor
A data ingestor from MQTT to a PostgreSQL database developed for the PONTOS project

## Prerequisites

* A running PostgreSQL instance with a database/table setup according to the setup outlined in [PONTOS-HUB](https://github.com/MO-RISE/pontos-hub/blob/main/database/docker-entrypoint-initdb.d/010-pontos-setup.sql#L7-L12)

* A running MQTT broker 

* A data publisher that publishes to the MQTT broker on a well-defined topic structure using JSON payloads.

### Specifics

The data ingestor extracts data from a single MQTT subscription and inserts into a single, narrow table in a PostgreSQL instance. Specifically, the data fields that are needed to populate the database table are the following:

* `timestamp` (PostgreSQL format: TIMESTAMPZ)
* `vessel_id` (PostgreSQL format: TEXT)
* `parameter_id` (PostgreSQL format: TEXT)
* `value` (PostgreSQL format: TEXT)

These fields are extracted from the MQTT topic and message payload (which is expected to be a JSON key-value construct) using the following "keys":
* `timestamp` (MQTT format: seconds since epoch)
* `vessel_id` (MQTT format: str)
* `tag` (MQTT format: str)
* `index` (MQTT format: int)
* `value` (MQTT format: int/float/str (will be casted to str))

Note: `parameter_id` will be constructed through concatenation of `tag` and `index` as `parameter_id` = `tag`_`index`.

The data ingestor provides a flexible configuration setup of where to find these "keys" in the topic and message payload respectively through two environment variables:

* `TOPIC_PARSER_FORMAT`

  A format string adhering to the principles of the [`parse`](https://github.com/r1chardj0n3s/parse) library.

  Example: 
  
  `TOPIC_PARSER_FORMAT="PONTOS/{vessel_id:w}/{tag:w}/{index:d}"`

  Which will accept, for example, topics like `PONTOS/imo_8602713/anemometer_twd_deg/1` and parse it as: 
  ```python
  {
    "vessel_id": "imo_8602713",
    "parameter_id": "anemometer_twd_deg_1"
  }
  ```

* `PAYLOAD_MAP_FORMAT`

  A set of "keys" mapping to message payload "fields".

  Example: `PAYLOAD_MAP_FORMAT=timestamp=epoch,value=sensor_value`

  Which will accept JSON payloads of the format `{"epoch": 1685078782, "sensor_value": 42, ...}` and parse it as:
  ```python
  {
    "timestamp": 1685078782,
    "value": 42
  }
  ```

**NOTE**: It is up to the user of this service to make sure that the configuration provides a single unique way to determine all 5 "keys from the MQTT topic and message payload!!

## Configuration
The following environment variables can be used for configuring the data ingestor:
```text
MQTT_BROKER_HOST
Hostname or IP of MQTT broker to connect to (Required)

MQTT_BROKER_PORT
Port of MQTT broker to connect to (Optional, default: 1883)

MQTT_CLIENT_ID
Client id to use for MQTT connection (Optional, default: pontos-data-ingestor)

MQTT_TRANSPORT
Underlying MQTT transport protocol, can be either 'tcp' or 'websocket' (Optional, default: 'tcp')

MQTT_TLS
Wether to use TLS when conencting to the MQTT broker (Optional, default: False)

MQTT_CLEAN_START
Wether to request a clean start or not when connecting to the broker (Optional, default: False)

MQTT_SESSION_EXPIRY_INTERVAL
Session expiry interval used for any session created by the broker (Optional, default: None)

MQTT_USER
Username to use when connecting to the MQTT broker (Optional, default: None)

MQTT_PASSWORD
Password to use when connecting to the MQTT broker (Optional, default: None)

MQTT_SUBSCRIBE_TOPIC
Topic or topic wildcard to subscribe to from the MQTT broker (Required)

MQTT_SUBSCRIBE_TOPIC_QOS
Quality of Service level to use when subscribing to MQTT_SUBSCRIBE_TOPIC (Optional, default: 0)

PG_CONNECTION_STRING
PostgreSQL connection string (Required)

PG_TABLE_NAME
Database table name to insert data into (Required)

PG_POOL_SIZE
Database connection pool size (Optional, default: 1)

TOPIC_PARSER_FORMAT
See explanation and examples above (Required)

PAYLOAD_MAP_FORMAT
See explanation and examples above (Required)

PARTITION_SIZE
Size of batches written to database if within PARTITION_TIMEOUT (Optional, default: 25)

PARTITION_TIMEOUT
Maximum time between writes to database (Optional, default: 5)

LOG_LEVEL
Log level of application (Optional, default: WARNING)

DISCARD_NULL_VALUES
Wether to discard null values or not in the parsed fields (Optional, default: false)
```

## Development
The repository includes a devcontainer setup which is the recommended way of creating a development environment. See [here](https://code.visualstudio.com/docs/devcontainers/containers) for a generic get-started in VSCode.

Linting:
```cmd
black .
pylint *.py
```

Testing:
```cmd
pytest -v tests/
```

## License
See [LICENSE](./LICENSE)
