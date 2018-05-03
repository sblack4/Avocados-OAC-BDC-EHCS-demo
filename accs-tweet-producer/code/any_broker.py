#!/bin/python
from __future__ import print_function
import tweepy
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import config
import json
import logging 


class AnyListener(StreamListener):
    """
    Custom StreamListener for streaming data.
    Allows you to push twitter data to a callback 
    """

    def __init__(self, callback):
        """
        Takes callback function which the listener 
        will pass data to 
        """
        self.callback = callback
        logging.basicConfig(filename='anylistener.log',level=config.log_level)
        logging.info("started logging")

    def on_data(self, data):
        """passes twitter data to callback
        
        Args:
            data (string): tweet data
        """
        try:
            self.callback(data.encode('utf-8'))
            return True
        except BaseException as e:
            logging.error("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        """necessary to implement StreamListener
        
        Arguments:
            status {string} -- error message
        
        Returns:
            None -- no return
        """

        logging.error(status)
        return True


def run_listener(callback):
    """
    Use this function to set up a tweet broker 
    that will push twitter data (as specified in the config file)
    to a callback function 
    """
    auth = OAuthHandler(config.consumer_key, config.consumer_secret)
    auth.set_access_token(config.access_token, config.access_secret)
    api = tweepy.API(auth)
    twitter_stream = Stream(auth, AnyListener(callback))
    twitter_stream.filter(track=config.track_string.split(" "))
    logging.info("listening for " + config.track_string)
