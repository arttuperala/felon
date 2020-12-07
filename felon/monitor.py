from decimal import Decimal
import json
import re
import time
import logging

from kafka import KafkaProducer
import requests

logger = logging.getLogger(__name__)


class HTTPMonitor:
    def __init__(
        self,
        url: str,
        interval: int,
        timeout: int,
        kafka: dict,
        regex: "re.Pattern" = None,
    ):
        """Website availability monitoring class.

        Performs GET requests to a URL and submits the following data from it to Kafka:
            - Status code
            - Response time
            - Was given regular expression found in the response body (optional)
        """
        self.interval = interval
        self.regex = regex
        self.timeout = timeout
        self.url = url
        self.topic = kafka["topic"]
        self.producer = KafkaProducer(
            value_serializer=lambda x: json.dumps(x, separators=(",", ":")).encode(
                "utf-8"
            ),
            **kafka["config"],
        )

    def _fetch(self):
        start_time = time.time()
        logger.info("HTTP GET %s", self.url)
        try:
            response = requests.get(self.url, timeout=self.timeout)
        except requests.exceptions.ReadTimeout:
            logger.warning("Request timeout")
            response = None
        response_time = Decimal(time.time() - start_time).quantize(Decimal("0.001"))
        if self.regex and response:
            regex_matched = bool(re.search(self.regex, response.text))
        else:
            regex_matched = None

        # Documentation states that we need to look up the "error code returned".
        # Instructions are unclear, but considering that we are working in the context of
        # general website availability, it probably refers to the HTTP status code.
        payload = {
            "response_time": float(response_time),
            "status_code": response.status_code if response else 0,
            "regex_matched": regex_matched,
        }
        logger.debug("Sending message: %s", payload)
        self.producer.send(self.topic, payload)

    def run(self):
        """Run the monitoring service."""
        while True:
            self._fetch()
            logger.debug("Waiting for %s seconds", self.interval)
            time.sleep(self.interval)
