from __future__ import print_function
import tweepy
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import time
import config
import json
import socket


class MyListener(StreamListener):
    """Custom StreamListener for streaming data."""

    def __init__(self, filename):
        self.client_socket = filename

    def on_data(self, data):
        try:
            msg = json.loads(data)
            print( msg['text'].encode('utf-8') )
            self.client_socket.send( msg['text'].encode('utf-8') )
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        print(status)
        return True


def run(c_socket):
    auth = OAuthHandler(config.consumer_key, config.consumer_secret)
    auth.set_access_token(config.access_token, config.access_secret)
    api = tweepy.API(auth)
    twitter_stream = Stream(auth, MyListener(c_socket))
    twitter_stream.filter(track=["Pandora", "Spotify"])


if __name__ == "__main__":
    s = socket.socket()         # Create a socket object
    host = "127.0.0.1"      # Get local machine name
    port = 5555                 # Reserve a port for your service.
    s.bind((host, port))        # Bind to the port

    print("Listening on port: %s" % str(port))

    s.listen(5)                 # Now wait for client connection.
    c, addr = s.accept()        # Establish connection with client.

    print("Received request from: " + str(addr))

    run(c)
