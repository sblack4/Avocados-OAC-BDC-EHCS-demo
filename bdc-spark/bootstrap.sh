#!/bin/bash

# run on BDC 
# sets up Pandora POC
#
# ### FILL IN THESE VAIRABLES ###
# ### ### BEFORE RUNNING ### ###
consumer_key = "6vdawFFMZUQXMfGEqVhyOZlEM"
consumer_secret = "1c2aVGTE0cM2mXxi74tt88H7MgY7NlZXkZTziPRTw564fyqr8l"
access_token = "13405372-immDs258VJbq5OdhwLVLZioxWn37otbMfdrD3kiWg"
access_secret = "RBDmGu0Ls2GSHWGa7LXq8nrU1AyRdo1bUzHLnQIArlpzs"


#
### Install my favorite tools ###
#  clean yum
echo "cleaning up yum metadata just in case"
yum clean metadata

# install helper tools
echo "installing tools"
yum install -y mlocate git wget vim tree

# update database for locate
updatedb

#
### Configure Spark ###
# TODO: (sblack) this will depend on size of instance, no?
# see https://cwiki.apache.org/confluence/display/AMBARI/Modify+configurations 
# 



#
### Install Pyspark Consumer App ### 
# TODO: (sblack) install pyspark app from git
# 
app_home=/home/opc/tweet_consumer
echo "installing pyspark consumer app in ${app_home}"
mkdir "${app_home}"
cat  > "$app_home"/config.py <<EOF
import logging

consumer_key = $consumer_key
consumer_secret = $consumer_secret
access_token = $access_token
access_secret = $access_secret

track_string = "pandoramusic spotify"

topic = "gse00013079-Pandora"
kafka_host = "localhost:6667"

log_level = logging.WARN

EOF 

curl -O https://raw.githubusercontent.com/sblack4/spark-streaming-twitter/master/tweet_consumer/tweet_consumer.py

#
### Start Consumer ###
# TODO: (sblack) 