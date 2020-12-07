import logging
import re

import psycopg2

logger = logging.getLogger(__name__)


class DatabaseHandler:
    def __init__(self, connection_details: dict):
        self.connection = psycopg2.connect(**connection_details)

    def add_record(
        self,
        table_name: str,
        timestamp: "datetime.datetime",
        response_time: float,
        status_code: int,
        regex_matched: bool,
    ):
        """Store HTTP monitoring event in a given table."""
        with self.connection.cursor() as cursor:
            cursor.execute(
                f"""
                INSERT INTO {table_name} (
                    timestamp,
                    response_time,
                    status_code,
                    regex_matched
                )
                VALUES (%s, %s, %s, %s);
                """,
                (timestamp, response_time, status_code, regex_matched),
            )
        self.connection.commit()

    def create_table(self, table_name: str):
        """Create a suitable table for storing HTTP monitoring events."""
        with self.connection.cursor() as cursor:
            cursor.execute(
                f"""
                CREATE TABLE {table_name} (
                    id BIGSERIAL PRIMARY KEY,
                    timestamp TIMESTAMP,
                    response_time NUMERIC(8, 3),
                    status_code SMALLINT,
                    regex_matched BOOLEAN
                );
                """
            )
        self.connection.commit()


def validate_table_name(table_name: str) -> bool:
    """Validate that a given table name conforms to PostgreSQL naming rules.

    For simplicity's sake, we're only allowing ASCII characters at this point despite
    PostgreSQL also allowing "letters with diacritical marks and non-Latin letters". This
    should hopefully be sufficient for most use cases now. We're also disallowing dollar
    signs for simplicity and possible future portability reasons.

    https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
    """
    match = re.match(r"^[A-z_][A-z0-9_]+$", table_name)
    if not match:
        return False
    return len(table_name) <= 63  # Assuming default NAMEDATALEN.
