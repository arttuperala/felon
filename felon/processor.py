import datetime
import json
import logging
import time

from kafka import KafkaConsumer

from felon.db import DatabaseHandler


logger = logging.getLogger(__name__)


class EventProcessor:
    """Website availability data processor.

    Processes the data from a Kafka topic and into one or more PostgreSQL database tables.
    """

    def __init__(self, tables, db, kafka):
        self.tables = tables
        self.db = DatabaseHandler(db["config"])
        self.consumer = KafkaConsumer(
            kafka["topic"], value_deserializer=json.loads, **kafka["config"]
        )

    def _process_event(self, record):
        message = record.value
        logger.debug("Received message: %s", message)
        struct_time = time.gmtime(record.timestamp // 1000)
        timestamp = datetime.datetime(
            year=struct_time.tm_year,
            month=struct_time.tm_mon,
            day=struct_time.tm_mday,
            hour=struct_time.tm_hour,
            minute=struct_time.tm_min,
            second=struct_time.tm_sec,
            microsecond=record.timestamp % 1000 * 1000,
        )
        for table_name in self.tables:
            logger.debug("Inserting into %s", table_name)
            self.db.add_record(
                table_name,
                timestamp,
                message["response_time"],
                message["status_code"],
                message["regex_matched"],
            )

    def run(self):
        """Run the event processor."""
        for record in self.consumer:
            self._process_event(record)
