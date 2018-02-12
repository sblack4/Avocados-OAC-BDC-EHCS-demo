#!/bin/python
from __future__ import print_function
import tweepy
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import config
import json
import socket
import logging


class AnyListener(StreamListener):
    """Custom StreamListener for streaming data."""

    def __init__(self, callback):
        self.callback = callback
        logging.basicConfig(filename='anylistener.log',level=config.log_level)
        logging.info("started logging")

    def on_data(self, data):
        try:
            msg = json.loads(data)
            logging.info(msg)
            self.callback(msg)
            return True
        except BaseException as e:
            logging.error("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        logging.error(status)
        return True


def run_listener(callback):
    auth = OAuthHandler(config.consumer_key, config.consumer_secret)
    auth.set_access_token(config.access_token, config.access_secret)
    api = tweepy.API(auth)
    twitter_stream = Stream(auth, AnyListener(callback))
    twitter_stream.filter(track=config.track_string.split(" "))
    # logging.info("listening for " + config.track_string)

