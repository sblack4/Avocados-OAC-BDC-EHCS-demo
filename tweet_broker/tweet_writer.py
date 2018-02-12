from __future__ import print_function
import tweepy
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import time
import json
import socket


class MyListener(StreamListener):
    """Custom StreamListener for streaming data."""

    def __init__(self, filename):
        self.file = filename

    def on_data(self, data):
        try:
            msg = json.loads(data)
            with open(self.file, "a+") as fileHandle:
                fileHandle.write(msg + "\n")
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        print(status)
        return True


def run(config, filename):
    auth = OAuthHandler(config.consumer_key, config.consumer_secret)
    auth.set_access_token(config.access_token, config.access_secret)
    api = tweepy.API(auth)
    twitter_stream = Stream(auth, MyListener(filename))
    twitter_stream.filter(track=["Pandora", "Spotify"])


class config:
    def __init__(self, consumer_key, consumer_secret, access_token, access_secret):
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        self.access_token = access_token
        self.access_secret = access_secret


if __name__ == "__main__":
    conf = config("", "", "", "")
    run(conf, "tweets.json")
