# felon

Website availability monitoring service.

## Description

felon is a tool to monitor website availability. The tool has two parts: the monitor and the processor.

The monitor runs periodic HTTP GET requests on a given website to get details about its availability and response. These details will be submitted to a given Apache Kafka topic for processing.

The processor consumes messages from a given Apache Kafka topic and submits the information to a PostgreSQL database. Events can be processed into multiple tables at the same time.

## Installation

You can install felon easily with pip.

    pip install git+https://github.com/arttuperala/felon

## Usage

To print out a list of all possible commands, use `felon --help`. For help with a particular command, use `felon <COMMAND> --help`.

### Environment variables

To ease deployment, all options that involve the Apache Kafka instance or the PostgreSQL database server can also be supplied as environment variables. All Kafka options will be in the form of `FELON_KAFKA_X` and all database options will be in the form of `FELON_DB_X`.

For example: `--kafka-key-location` can be supplied as `FELON_KAFKA_KEY_LOCATION` while `--database-user` can be supplied as `FELON_DB_USER`.
