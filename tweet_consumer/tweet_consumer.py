from __future__ import print_function
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import desc
from os import getcwd


sc = SparkContext("local[2]", "TweetConsumer")
ssc = StreamingContext(sc, 10)
sqlContext = SQLContext(sc)
# ssc.checkpoint("file:///" + getcwd())
socket_stream = ssc.socketTextStream("127.0.0.1", 5555)
lines = socket_stream.window(20)

print("lines")
print(lines)

from collections import namedtuple
fields = ("tag", "count")
Tweet = namedtuple('Tweet', fields)

(lines.flatMap(lambda text: text.split(" "))
 .filter(lambda word: word.lower().startswith("#"))
 .map(lambda word: (word.lower(), 1))
 .reduceByKey(lambda a, b: a + b)
 .map(lambda rec: Tweet(rec[0], rec[1]))
 .foreachRDD(lambda rdd: rdd.toDF().sort(desc("count"))
             .limit(10).registerTempTable("tweets")))

ssc.start()
ssc.awaitTermination()
