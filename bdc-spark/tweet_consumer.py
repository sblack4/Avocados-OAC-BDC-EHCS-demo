#!/bin/python
from __future__ import print_function

# general packages
import sys
import config
import logging
import json
from random import choice

# pyspark streaming
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

# spark sql
from pyspark.sql import HiveContext
from pyspark.sql.types import *
from pyspark.sql.functions import desc
from pyspark.sql.functions import col
from pyspark.sql.functions import expr


def HandleJson(df):
    """Structures dataframes and appends them to respective tables
    
    Args:
        df (dataframe): dataframe containing tweets
    """

    # check for possibly_sensitive & get rid of sensitive material
    if df.select("possibly_sensitive").show() == "true":
        return
    tweets = df.select("id",
        "created_at",
        expr('COALESCE(text, "null") AS text'),
        expr('COALESCE(favorite_count, 0) AS favorite_count'),
        expr('COALESCE(retweet_count, 0) AS retweet_count'),
        expr('COALESCE(quote_count, 0) AS quote_count'),
        expr('COALESCE(reply_count, 0) as reply_count'),
        expr('COALESCE(lang, "und") as lang'),
        expr('COALESCE(coordinates, 0) as coordinates'),
        expr('COALESCE(place, "null") as place'),
        col("user.id").alias("user_id"),
        expr("good_day() as date"),
        expr("rand_state() as state"),
        expr("rand_provider() as provider")
    )
    tweets.write.mode("append").insertInto("default.tweets")

    users = df.select("user.id",
        "user.name",
        "user.description",
        "user.followers_count",
        "user.location",
        "user.friends_count",
        "user.screen_name"
    )
    users.write.mode("append").insertInto("default.users")

def handleRDD(rdd):
    """Reads RDD containing twitter API data
    
    Args:
        rdd (RDD): RDD of twitter data, each row will be a Tweet as JSON
    """

    if not rdd:
        return
    try:
        df=sqlContext.read.json(rdd.map(lambda x: x[1]))
        HandleJson(df)
    except Exception as ex:
        print(ex)

if __name__ == "__main__":
    sc = SparkContext("yarn", "TweetConsumer")
    ssc = StreamingContext(sc, 1)
    sqlContext = HiveContext(sc)
    # ssc.checkpoint("file:///" + getcwd())

    broker, topic = config.broker, config.topic
    lines = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": broker})

    lines.foreachRDD(handleRDD)
    ssc.start()
    ssc.awaitTermination()