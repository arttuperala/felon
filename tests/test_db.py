from datetime import datetime
from unittest.mock import MagicMock, call, patch
import re
import unittest

from felon.db import DatabaseHandler, validate_table_name

DUMMY_DB_CONFIG = {
    "dbname": "felon",
    "host": "db.example.com",
    "port": 1337,
    "user": "root",
    "password": "p4ssw0rd",
}


class FelonDBHandlerTestCase(unittest.TestCase):
    def _format_raw_sql(self, sql):
        return re.sub(r"\s{2,}", " ", sql.replace("\n", " ")).strip()

    @patch("felon.db.psycopg2._connect")
    def test_create(self, mock_db_connect):
        DatabaseHandler(DUMMY_DB_CONFIG)
        mock_db_connect.assert_called_once_with(
            "dbname=felon host=db.example.com port=1337 user=root password=p4ssw0rd",
            connection_factory=None,
        )

    @patch("felon.db.psycopg2.connect")
    def test_add_record(self, mock_connect):
        db_handler = DatabaseHandler(DUMMY_DB_CONFIG)
        db_handler.add_record("table1", datetime(2020, 3, 15, 15, 3, 0), 5.01, 0, None)

        mock_cursor = mock_connect.return_value.cursor.return_value
        execute_calls = mock_cursor.__enter__.return_value.execute.mock_calls
        self.assertEqual(len(execute_calls), 1)
        sql_statement = self._format_raw_sql(execute_calls[0].args[0])
        self.assertEqual(
            sql_statement,
            "INSERT INTO table1 ( timestamp, response_time, status_code, "
            "regex_matched ) VALUES (%s, %s, %s, %s);",
        )
        self.assertEqual(
            execute_calls[0].args[1], (datetime(2020, 3, 15, 15, 3, 0), 5.01, 0, None)
        )

    @patch("felon.db.psycopg2.connect")
    def test_create_table(self, mock_connect):
        db_handler = DatabaseHandler(DUMMY_DB_CONFIG)
        db_handler.create_table("table2")

        mock_cursor = mock_connect.return_value.cursor.return_value
        execute_calls = mock_cursor.__enter__.return_value.execute.mock_calls
        self.assertEqual(len(execute_calls), 1)
        execute_args = execute_calls[0].args
        self.assertEqual(len(execute_args), 1)
        sql_statement = self._format_raw_sql(execute_args[0])
        self.assertEqual(
            sql_statement,
            "CREATE TABLE table2 ( id BIGSERIAL PRIMARY KEY, timestamp TIMESTAMP, "
            "response_time NUMERIC(8, 3), status_code SMALLINT, regex_matched BOOLEAN );",
        )

class TableNameValidationTestCase(unittest.TestCase):
    def test_alphanumerical(self):
        self.assertTrue(validate_table_name("table1"))

    def test_alphanumerical_with_diacritical(self):
        # Actually possible with PostgreSQL, but disallowed for the time being.
        self.assertFalse(validate_table_name("táble"))

    def test_alphanumerical_with_dollar_sign(self):
        # Actually possible with PostgreSQL, but disallowed for the time being.
        self.assertFalse(validate_table_name("table$2"))

    def test_alphanumerical_with_nonlatin(self):
        # Actually possible with PostgreSQL, but disallowed for the time being.
        self.assertFalse(validate_table_name("täble"))

    def test_alphanumerical_with_underscore(self):
        self.assertTrue(validate_table_name("table_2"))

    def test_cjk(self):
        self.assertFalse(validate_table_name("table_有り"))

    def test_starting_dollar_sign(self):
        # Completely disallowed with PostgreSQL.
        self.assertFalse(validate_table_name("$table1"))

    def test_starting_numeral(self):
        self.assertFalse(validate_table_name("1table"))

    def test_starting_underscore(self):
        self.assertTrue(validate_table_name("_table1"))

    def test_too_long(self):
        self.assertFalse(validate_table_name("table_" + 64 * "a"))
