from unittest.mock import call, patch
import os
import re
import unittest

from click.testing import CliRunner

from felon.cli.main import cli
from felon.db import DatabaseHandler


class FelonUITestCase(unittest.TestCase):
    def setUp(self):
        self.runner = CliRunner()

    def cli(self, args):
        return self.runner.invoke(cli, args)

    def database_args(
        self,
        db_name="felon",
        db_server="localhost",
        db_user="root",
    ):
        return [
            "--database-name",
            db_name,
            "--database-server",
            db_server,
            "--database-user",
            db_user,
        ]

    def kafka_args(
        self,
        kafka_server="localhost",
        kafka_topic="felon-test",
    ):
        return [
            "--kafka-server",
            kafka_server,
            "--kafka-topic",
            kafka_topic,
        ]


class DbCreateTableCommandTestCase(FelonUITestCase):
    @patch("felon.db.psycopg2.connect")
    @patch("felon.cli.main.validate_table_name")
    @patch.object(DatabaseHandler, "create_table")
    def test(self, mock_create_table, mock_validate_table_name, mock_connect):
        mock_validate_table_name.return_value = True

        result = self.cli(["db", "create-table", *self.database_args(), "my_table"])
        self.assertIn("[INFO][main] Created table my_table", result.output)
        self.assertEqual(result.exit_code, 0)
        self.assertIsNone(result.exception)

        mock_validate_table_name.assert_called_once_with("my_table")
        mock_connect.assert_called_once_with(
            dbname="felon", host="localhost", port=5432, user="root", password=None
        )
        mock_create_table.assert_called_once_with("my_table")

    @patch("felon.db.psycopg2.connect")
    @patch("felon.cli.main.validate_table_name")
    @patch.object(DatabaseHandler, "create_table")
    def test_invalid_table_name(
        self, mock_create_table, mock_validate_table_name, mock_connect
    ):
        mock_validate_table_name.return_value = False

        result = self.cli(["db", "create-table", *self.database_args(), "bad table"])
        self.assertIn(
            "[CRITICAL][main] Invalid PostgreSQL table name: bad table", result.output
        )
        self.assertEqual(result.exit_code, 1)

        mock_validate_table_name.assert_called_once_with("bad table")
        mock_connect.assert_not_called()
        mock_create_table.assert_not_called()

    def test_without_db_args(self):
        result = self.cli(["db", "create-table", "my_table"])
        self.assertIn("Error: Missing option '--database-", result.output)


class MonitorCommandTestCase(FelonUITestCase):
    @patch("felon.cli.main.HTTPMonitor", autospec=True)
    def test(self, mock_monitor):
        result = self.cli(["monitor", *self.kafka_args(), "https://example.com"])
        self.assertIn("[INFO][main] Starting HTTP monitor service", result.output)
        self.assertIsNone(result.exception)
        self.assertEqual(result.exit_code, 0)

        self.assertEqual(
            mock_monitor.mock_calls,
            [
                call(
                    url="https://example.com",
                    interval=300,
                    timeout=5,
                    kafka={
                        "config": {
                            "bootstrap_servers": "localhost:9092",
                            "security_protocol": "PLAINTEXT",
                        },
                        "topic": "felon-test",
                    },
                ),
                call().run(),
            ],
        )

    @patch("felon.cli.main.HTTPMonitor", autospec=True)
    def test_with_invalid_regex(self, mock_monitor):
        result = self.cli(
            [
                "monitor",
                *self.kafka_args(),
                "--regex",
                "id_[0-9]+(",
                "https://example.com",
            ]
        )
        self.assertIn("[ERROR][main] Invalid regular expression ('", result.output)
        self.assertIn(
            "[ERROR][main] Regular expression checking will be skipped", result.output
        )
        self.assertIn("[INFO][main] Starting HTTP monitor service", result.output)
        self.assertIsNone(result.exception)
        self.assertEqual(result.exit_code, 0)

        self.assertEqual(
            mock_monitor.mock_calls,
            [
                call(
                    url="https://example.com",
                    interval=300,
                    timeout=5,
                    kafka={
                        "config": {
                            "bootstrap_servers": "localhost:9092",
                            "security_protocol": "PLAINTEXT",
                        },
                        "topic": "felon-test",
                    },
                ),
                call().run(),
            ],
        )

    @patch("felon.cli.main.HTTPMonitor", autospec=True)
    def test_with_regex(self, mock_monitor):
        result = self.cli(
            [
                "monitor",
                *self.kafka_args(),
                "--regex",
                "id_[0-9]+",
                "https://example.com",
            ]
        )
        self.assertIn("[INFO][main] Starting HTTP monitor service", result.output)
        self.assertIsNone(result.exception)
        self.assertEqual(result.exit_code, 0)

        self.assertEqual(
            mock_monitor.mock_calls,
            [
                call(
                    url="https://example.com",
                    interval=300,
                    timeout=5,
                    kafka={
                        "config": {
                            "bootstrap_servers": "localhost:9092",
                            "security_protocol": "PLAINTEXT",
                        },
                        "topic": "felon-test",
                    },
                    regex=re.compile(r"id_[0-9]+"),
                ),
                call().run(),
            ],
        )

    def test_without_kafka_args(self):
        result = self.cli(["monitor", "https://example.com"])
        self.assertIn("Error: Missing option '--kafka-", result.output)
        self.assertEqual(result.exit_code, 2)


class ProcessCommandTestCase(FelonUITestCase):
    @patch("felon.cli.main.validate_table_name", lambda x: True)
    @patch("felon.cli.main.EventProcessor", autospec=True)
    def test(self, mock_processor):
        result = self.cli(
            ["process", *self.kafka_args(), *self.database_args(), "test1"]
        )
        self.assertEqual(result.exit_code, 0)

        self.assertEqual(
            mock_processor.mock_calls,
            [
                call(
                    tables=("test1",),
                    db={
                        "config": {
                            "dbname": "felon",
                            "host": "localhost",
                            "port": 5432,
                            "user": "root",
                            "password": None,
                        }
                    },
                    kafka={
                        "config": {
                            "bootstrap_servers": "localhost:9092",
                            "security_protocol": "PLAINTEXT",
                        },
                        "topic": "felon-test",
                    },
                ),
                call().run(),
            ],
        )

    @patch.dict(
        os.environ,
        {
            "FELON_KAFKA_SERVER": "kafka.example.com",
            "FELON_KAFKA_PORT": "1337",
            "FELON_KAFKA_PROTOCOL": "SSL",
            "FELON_KAFKA_TOPIC": "test-kafka",
            # TODO: Fix, not portable.
            "FELON_KAFKA_CA_LOCATION": "/dev/null",
            "FELON_KAFKA_CERT_LOCATION": "/dev/null",
            "FELON_KAFKA_KEY_LOCATION": "/dev/null",
            "FELON_DB_NAME": "test-db",
            "FELON_DB_SERVER": "db.example.com",
            "FELON_DB_PORT": "7331",
            "FELON_DB_USER": "reimu",
            "FELON_DB_PASSWORD": "asdf",
        },
    )
    @patch("felon.cli.main.validate_table_name", lambda x: True)
    @patch("felon.cli.main.EventProcessor", autospec=True)
    def test_with_envvars(self, mock_processor):
        result = self.cli(["process", "test1"])
        self.assertEqual(result.exit_code, 0)

        self.assertEqual(
            mock_processor.mock_calls,
            [
                call(
                    tables=("test1",),
                    db={
                        "config": {
                            "dbname": "test-db",
                            "host": "db.example.com",
                            "port": 7331,
                            "user": "reimu",
                            "password": "asdf",
                        }
                    },
                    kafka={
                        "config": {
                            "bootstrap_servers": "kafka.example.com:1337",
                            "security_protocol": "SSL",
                            "ssl_cafile": "/dev/null",
                            "ssl_keyfile": "/dev/null",
                            "ssl_certfile": "/dev/null",
                        },
                        "topic": "test-kafka",
                    },
                ),
                call().run(),
            ],
        )

    @patch("felon.cli.main.validate_table_name", lambda x: False)
    @patch("felon.cli.main.EventProcessor", autospec=True)
    def test_invalid_table_name(self, mock_processor):
        result = self.cli(
            ["process", *self.kafka_args(), *self.database_args(), "bad table"]
        )
        self.assertIn(
            "[CRITICAL][main] Invalid PostgreSQL table name: bad table", result.output
        )
        self.assertEqual(result.exit_code, 1)

        self.assertEqual(mock_processor.mock_calls, [])
