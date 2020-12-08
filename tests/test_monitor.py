from unittest.mock import MagicMock, call, patch
import logging
import re
import time
import unittest

from requests.exceptions import ReadTimeout

from felon.monitor import HTTPMonitor


def fake_get(*args, **kwargs):
    # Slows down the tests a bit, but not to a degree that I'd worry about it.
    time.sleep(0.150)
    return MagicMock(status_code=200, text="<body><p>Page 1</p></body>")


def fake_timeout(*args, **kwargs):
    time.sleep(0.3)
    raise ReadTimeout


@patch("requests.get")
@patch("felon.monitor.KafkaProducer")
class FelonMonitorTestCase(unittest.TestCase):
    def setUp(self):
        logger = logging.getLogger("felon.monitor")
        logger.disabled = True

    def test_fetch(self, mock_producer, mock_get):
        mock_get.side_effect = fake_get

        monitor = HTTPMonitor(
            url="https://example.com",
            interval=300,
            timeout=7,
            kafka={
                "config": {
                    "bootstrap_servers": "localhost:9092",
                    "security_protocol": "PLAINTEXT",
                },
                "topic": "felon-topic",
            },
        )
        monitor._fetch()

        mock_get.assert_called_once_with("https://example.com", timeout=7)
        last_call = mock_producer.mock_calls[-1]
        self.assertEqual(last_call.args[0], "felon-topic")
        self.assertAlmostEqual(last_call.args[1]["response_time"], 0.15, delta=0.01)
        self.assertEqual(last_call.args[1]["status_code"], 200)
        self.assertIsNone(last_call.args[1]["regex_matched"])

    def test_fetch_regex_found(self, mock_producer, mock_get):
        mock_get.side_effect = fake_get

        monitor = HTTPMonitor(
            url="https://example.com",
            interval=300,
            timeout=7,
            regex=re.compile(r"<p>Page [0-9]</p>"),
            kafka={
                "config": {
                    "bootstrap_servers": "localhost:9092",
                    "security_protocol": "PLAINTEXT",
                },
                "topic": "felon-topic",
            },
        )
        monitor._fetch()

        mock_get.assert_called_once_with("https://example.com", timeout=7)
        last_call = mock_producer.mock_calls[-1]
        self.assertEqual(last_call.args[0], "felon-topic")
        self.assertAlmostEqual(last_call.args[1]["response_time"], 0.15, delta=0.01)
        self.assertEqual(last_call.args[1]["status_code"], 200)
        self.assertTrue(last_call.args[1]["regex_matched"])

    def test_fetch_regex_not_found(self, mock_producer, mock_get):
        mock_get.side_effect = fake_get

        monitor = HTTPMonitor(
            url="https://example.com",
            interval=300,
            timeout=7,
            regex=re.compile(r"<p>Page [0-9]{2,}</p>"),
            kafka={
                "config": {
                    "bootstrap_servers": "localhost:9092",
                    "security_protocol": "PLAINTEXT",
                },
                "topic": "felon-topic",
            },
        )
        monitor._fetch()

        mock_get.assert_called_once_with("https://example.com", timeout=7)
        last_call = mock_producer.mock_calls[-1]
        self.assertEqual(last_call.args[0], "felon-topic")
        self.assertAlmostEqual(last_call.args[1]["response_time"], 0.15, delta=0.01)
        self.assertEqual(last_call.args[1]["status_code"], 200)
        self.assertFalse(last_call.args[1]["regex_matched"])

    def test_fetch_timeout(self, mock_producer, mock_get):
        mock_get.side_effect = fake_timeout

        monitor = HTTPMonitor(
            url="https://example.com",
            interval=300,
            timeout=7,
            regex=re.compile(r"<p>Page [0-9]</p>"),
            kafka={
                "config": {
                    "bootstrap_servers": "localhost:9092",
                    "security_protocol": "PLAINTEXT",
                },
                "topic": "felon-topic",
            },
        )
        monitor._fetch()

        mock_get.assert_called_once_with("https://example.com", timeout=7)
        last_call = mock_producer.mock_calls[-1]
        self.assertEqual(last_call.args[0], "felon-topic")
        self.assertAlmostEqual(last_call.args[1]["response_time"], 0.3, delta=0.01)
        self.assertEqual(last_call.args[1]["status_code"], 0)
        self.assertFalse(last_call.args[1]["regex_matched"])
