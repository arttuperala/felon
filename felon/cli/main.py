import logging
import re
import sys

import click

from felon.cli.helpers import CommonOptions
from felon.db import DatabaseHandler, validate_table_name
from felon.monitor import HTTPMonitor
from felon.processor import EventProcessor

logger = logging.getLogger("felon")
formatter = logging.Formatter(
    fmt="[%(asctime)s][%(levelname)s][%(module)s] %(message)s"
)


KAFKA_DEFAULT_PORT = 9092
DB_DEFAULT_PORT = 5432


class kafka_options(CommonOptions):
    """Common command options for connecting to an Apache Kafka instance."""

    options = (
        click.option(
            "--kafka-server",
            envvar="FELON_KAFKA_SERVER",
            required=True,
            help="Kafka server address.",
        ),
        click.option(
            "--kafka-port",
            envvar="FELON_KAFKA_PORT",
            type=int,
            default=KAFKA_DEFAULT_PORT,
            help=f"Kafka server port. Defaults to {KAFKA_DEFAULT_PORT}.",
        ),
        click.option(
            "--kafka-topic",
            envvar="FELON_KAFKA_TOPIC",
            required=True,
            help="Kafka topic.",
        ),
        click.option(
            "--kafka-protocol",
            envvar="FELON_KAFKA_PROTOCOL",
            required=True,
            type=click.Choice(["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"]),
            default="PLAINTEXT",
            help="Kafka connection protocol.",
        ),
        click.option(
            "--kafka-ca-location",
            envvar="FELON_KAFKA_CA_LOCATION",
            type=click.Path(exists=True, resolve_path=True),
            help="Path to CA certificate for verifying the broker's key.",
        ),
        click.option(
            "--kafka-cert-location",
            envvar="FELON_KAFKA_CERT_LOCATION",
            type=click.Path(exists=True, resolve_path=True),
            help="Path to client's private key (PEM) used for authentication.",
        ),
        click.option(
            "--kafka-key-location",
            envvar="FELON_KAFKA_KEY_LOCATION",
            type=click.Path(exists=True, resolve_path=True),
            help="Path to client's private key (PEM) used for authentication.",
        ),
    )

    def process(self, context, kwargs):
        server = kwargs.pop("kafka_server")
        port = kwargs.pop("kafka_port", KAFKA_DEFAULT_PORT)
        configuration = {
            "bootstrap_servers": f"{server}:{port}",
            "security_protocol": kwargs.pop("kafka_protocol"),
        }

        if ca_location := kwargs.pop("kafka_ca_location"):
            configuration["ssl_cafile"] = ca_location
        if key_location := kwargs.pop("kafka_key_location"):
            configuration["ssl_keyfile"] = key_location
        if certificate_location := kwargs.pop("kafka_cert_location"):
            configuration["ssl_certfile"] = certificate_location

        context.obj["kafka"] = {
            "config": configuration,
            "topic": kwargs.pop("kafka_topic"),
        }


class postgres_options(CommonOptions):
    """Common command options for connecting to a PostgreSQL database."""

    options = (
        click.option(
            "--database-name",
            envvar="FELON_DB_NAME",
            required=True,
            help="PostgreSQL database name.",
        ),
        click.option(
            "--database-server",
            envvar="FELON_DB_SERVER",
            required=True,
            help="PostgreSQL server address.",
        ),
        click.option(
            "--database-port",
            envvar="FELON_DB_PORT",
            type=int,
            default=DB_DEFAULT_PORT,
            help=f"PostgreSQL server port. Defaults to {DB_DEFAULT_PORT}.",
        ),
        click.option(
            "--database-user",
            envvar="FELON_DB_USER",
            required=True,
            help="PostgreSQL connection username.",
        ),
        click.option(
            "--database-password",
            envvar="FELON_DB_PASSWORD",
            help="PostgreSQL connection password.",
        ),
    )

    def process(self, context, kwargs):
        configuration = {
            "dbname": kwargs.pop("database_name"),
            "host": kwargs.pop("database_server"),
            "port": kwargs.pop("database_port", DB_DEFAULT_PORT),
            "user": kwargs.pop("database_user"),
            "password": kwargs.pop("database_password"),
        }
        context.obj["db"] = {"config": configuration}


@click.group()
@click.pass_context
@click.option(
    "--log-level",
    type=click.Choice(
        ["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"], case_sensitive=False
    ),
    default="INFO",
)
def cli(context, log_level):
    """Website availability monitoring tool."""
    context.ensure_object(dict)

    log_level = getattr(logging, log_level, logging.INFO)
    logger.setLevel(log_level)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(log_level)
    logger.addHandler(console_handler)


@cli.group()
@click.pass_context
def db(context):
    """Manage the PostgreSQL database."""


@db.command()
@click.pass_context
@postgres_options
@click.argument("table", nargs=1)
def create_table(context, table):
    """Create a table suitable for storing monitoring events."""
    if not validate_table_name(table):
        logger.critical("Invalid PostgreSQL table name: %s", table)
        sys.exit(1)

    db = DatabaseHandler(context.obj["db"]["config"])
    db.create_table(table)
    logger.info("Created table %s", table)


@cli.command()
@click.pass_context
@kafka_options
@click.argument("url", nargs=1)
@click.option(
    "-i",
    "--interval",
    type=int,
    default=300,
    help="Duration between requests (in seconds). Defaults to 300.",
)
@click.option(
    "-t",
    "--timeout",
    type=int,
    default=5,
    help="Timeout for HTTP requests (in seconds). Defaults to 5.",
)
@click.option(
    "-r",
    "--regex",
    help="Optional regular expression pattern to check for in the response.",
)
def monitor(context, url, **kwargs):
    """Start a HTTP monitoring service for a given URL."""
    if regex := kwargs.pop("regex", None):
        try:
            regex = re.compile(regex)
        except re.error as exception:
            logger.error("Invalid regular expression ('%s')", exception)
            logger.error("Regular expression checking will be skipped")
        else:
            context.obj["regex"] = regex

    context.obj["url"] = url
    context.obj.update(kwargs)

    http_monitor = HTTPMonitor(**context.obj)
    logger.info("Starting HTTP monitor service")
    http_monitor.run()


@cli.command()
@click.pass_context
@postgres_options
@kafka_options
@click.argument("tables", metavar="TABLE", nargs=-1, required=True)
def process(context, **kwargs):
    """Stream results from Kafka into a database."""
    for table_name in kwargs["tables"]:
        if not validate_table_name(table_name):
            logger.critical("Invalid PostgreSQL table name: %s", table_name)
            sys.exit(1)
    context.obj.update(kwargs)

    event_processor = EventProcessor(**context.obj)
    logger.info("Starting event processing service")
    event_processor.run()
