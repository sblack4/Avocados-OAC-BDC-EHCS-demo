#!/bin/bash
 
# Author: Steven.Black@Oracle.com
# this script was written for Oracle's BDC 

### Install my favorite tools ###
#  clean yum
echo "cleaning up yum metadata just in case"
yum clean metadata
# install helper tools
echo "installing tools"
yum update
yum install -y mlocate git wget vim tree
# update database for locate
# updatedb

#
### Configure Spark ###
# see https://cwiki.apache.org/confluence/display/AMBARI/Modify+configurations 
# 
# TODO: (sblack) this will depend on size of instance, no?
wget -nc https://github.com/sblack4/streaming-data-analytics-demo/raw/master/accs-tweet-producer/code.zip

unzip code.zip


#
### Install Pyspark Consumer App ### 
#


### Start Consumer ###
# TODO: (sblack) 

