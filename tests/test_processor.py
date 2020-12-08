from datetime import datetime
from unittest.mock import MagicMock, call, patch
import json
import unittest

from felon.processor import EventProcessor

DUMMY_DB_CONFIG = {
    "config": {
        "dbname": "felon",
        "host": "localhost",
        "port": 5432,
        "user": "root",
        "password": "",
    }
}
DUMMY_KAFKA_CONFIG = {
    "config": {
        "bootstrap_servers": "localhost:9092",
        "security_protocol": "PLAINTEXT",
    },
    "topic": "felon-topic",
}
MOCK_EVENTS = [
    MagicMock(
        timestamp=1584284580000,
        value={
            "response_time": 5.01,
            "status_code": 0,
            "regex_matched": None,
        },
    ),
    MagicMock(
        timestamp=1584284590000,
        value={
            "response_time": 0.123,
            "status_code": 200,
            "regex_matched": True,
        },
    ),
]


@patch("felon.processor.DatabaseHandler")
@patch("felon.processor.KafkaConsumer")
class FelonProcessorTestCase(unittest.TestCase):
    def test_create(self, mock_kafka_consumer, mock_database_handler):
        EventProcessor(("table1",), db=DUMMY_DB_CONFIG, kafka=DUMMY_KAFKA_CONFIG)

        mock_database_handler.assert_called_once_with(DUMMY_DB_CONFIG["config"])
        mock_kafka_consumer.assert_called_once_with(
            "felon-topic", value_deserializer=json.loads, **DUMMY_KAFKA_CONFIG["config"]
        )

    def test_process_event(self, mock_kafka_consumer, mock_database_handler):
        mock_kafka_consumer.return_value.__iter__.return_value = MOCK_EVENTS

        processor = EventProcessor(
            ("table1",), db=DUMMY_DB_CONFIG, kafka=DUMMY_KAFKA_CONFIG
        )
        processor.run()

        self.assertEqual(
            mock_database_handler.return_value.add_record.mock_calls,
            [
                call("table1", datetime(2020, 3, 15, 15, 3, 0), 5.01, 0, None),
                call("table1", datetime(2020, 3, 15, 15, 3, 10), 0.123, 200, True),
            ],
        )

    def test_process_event_multiple_tables(
        self, mock_kafka_consumer, mock_database_handler
    ):
        mock_kafka_consumer.return_value.__iter__.return_value = MOCK_EVENTS

        processor = EventProcessor(
            ("table1", "table2", "table_3"),
            db=DUMMY_DB_CONFIG,
            kafka=DUMMY_KAFKA_CONFIG,
        )
        processor.run()

        self.assertEqual(
            mock_database_handler.return_value.add_record.mock_calls,
            [
                call("table1", datetime(2020, 3, 15, 15, 3, 0), 5.01, 0, None),
                call("table2", datetime(2020, 3, 15, 15, 3, 0), 5.01, 0, None),
                call("table_3", datetime(2020, 3, 15, 15, 3, 0), 5.01, 0, None),
                call("table1", datetime(2020, 3, 15, 15, 3, 10), 0.123, 200, True),
                call("table2", datetime(2020, 3, 15, 15, 3, 10), 0.123, 200, True),
                call("table_3", datetime(2020, 3, 15, 15, 3, 10), 0.123, 200, True),
            ],
        )
