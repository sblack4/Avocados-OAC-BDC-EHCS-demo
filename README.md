# stream twitter data to spark!

## About 

right now there's three brokers and one consumer

### Brokers
* port broker - sends the incoming tweets to localhost:5555
* tweet writer - writes the incoming data to a file 
* kafka broker - send the data to kafka 

### Consumers
* the spark consumer just takes the data, prints how many 
records it got, and the first line of the record (which would be the first tweet)

## Thanks && Acknowledgements 

* https://gist.github.com/bonzanini/af0463b927433c73784d
* http://www.awesomestats.in/spark-twitter-stream/ 
