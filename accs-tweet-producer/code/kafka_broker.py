#!/usr/bin/env python

# standard libraries
from __future__ import print_function
import config
import json
import logging

# kafka-python libraries
from kafka import KafkaProducer
from kafka.errors import KafkaError
from any_broker import run_listener


def run_kafka_broker():
    """
    
    """
    logging.basicConfig(filename='run_kafka_broker.log',level=config.log_level)
    logging.info("logging to " + config.kafka_host)
    mykafkaservers = [config.kafka_host]
    producer = KafkaProducer(
        bootstrap_servers=mykafkaservers, 
        value_serializer=lambda m: json.dumps(m).encode('utf-8'),
        api_version=(0,10,1)
    )

    produce = lambda vals: producer.send(config.topic, vals)
    run_listener(produce)

if __name__ == "__main__":
    run_kafka_broker()