from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from datetime import datetime
from elasticsearch import Elasticsearch
import json 

TCP_IP = 'localhost'
TCP_PORT = 9001
id=0

# Pyspark
# create spark configuration
conf = SparkConf()
conf.setAppName('TwitterApp')
conf.setMaster('local[2]')
# create spark context with the above configuration
sc = SparkContext(conf=conf)

# create the Streaming Context from spark context with interval size 2 seconds
ssc = StreamingContext(sc, 4)
ssc.checkpoint("checkpoint_TwitterApp")
# read data from port 900
dataStream = ssc.socketTextStream(TCP_IP, TCP_PORT)


es = Elasticsearch(['localhost:9200'])
tmp = es.indices.create(index="idx", body={
  "mappings": {
    "doc": { 
      "properties": { 
        "text":    { "type": "text"  }, 
        "location":     { "type": "geo_point"  }, 
        "sentiment":      { "type": "text" },
	"time":	{"type":"date", "format":"yyyy-MM-dd HH:mm:ss"}
      }
    }
  }
})


######### your processing here ###################
#dataStream.pprint()

words = dataStream.map(lambda x: json.loads(x.encode('utf-8'))).map(lambda x: (None, x))
words.foreachRDD(lambda line : line.saveAsNewAPIHadoopFile(
            path="-", 
            outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
            keyClass="org.apache.hadoop.io.NullWritable", 
            valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
	    conf={ "es.nodes" : "localhost",
			"es.port": "9200", 
			"es.resource" : "idx/doc"
			}))


words.pprint()
#wordcount = words.map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y)
#wordcount.pprint()
#################################################


ssc.start()
ssc.awaitTermination()
