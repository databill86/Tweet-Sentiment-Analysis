import json 
import tweepy
import socket
import re
import requests
import datetime
from pycorenlp import StanfordCoreNLP


ACCESS_TOKEN = '875014891710672897-lVfe5adwbH8XXnuj0Isjs70aSBk4dpb'
ACCESS_SECRET = 'wkkPFswWbVtYLmxQqECbXWtjjLLHxhv0Ko2naimUnYDP4'
CONSUMER_KEY = 'qKWTMFsSj8KlGcWmzDh8P3v76'
CONSUMER_SECRET = 'mifiug4l5WvcCvmjulNlQBwESXDmEKCzTnuiF4d4ieK8pb8ojk'

auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)


hashtag = '#trump'

TCP_IP = 'localhost'
TCP_PORT = 9001


# create sockets
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# s.connect((TCP_IP, TCP_PORT))
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
conn, addr = s.accept()
class MyStreamListener(tweepy.StreamListener):
    def on_status(self, status):

	# get location information from tweet
	location = status.user.location
	str = ""

	# stanfordnlp analysis
	res = stanfordnlp(status.text.encode('utf-8'))

	# can i get location information from tweet?
	if location:

	    # send request to geocoding api
	    r = requests.post('https://maps.googleapis.com/maps/api/geocode/json?address='+location+'&key=AIzaSyCHP9BC1156HrxYjwK9_MbVUO_BsUyFOZc')

	    # get results list from response
	    list = json.loads(r.text)['results']

	    # can i get coordinate from geocoding api?
	    if len(list)>0:		

		# find the coordinate from results list		
		b = list[0]['geometry']['location']

		# create coordinate dict
		d = {"lat": b["lat"], 'lon': b["lng"]}

		# create dict including text, location, sentiment
		str = {"text":status.text.encode('utf-8'),"location":None,"sentiment":frequentSentiment(res),"time":datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
		str["location"] = d

		# send to spark
		c = json.dumps(str) + "\n"
		print(c)
        	conn.send(c)
	    else:
		print("location coordinate is not available")
    	else:
	    print("location coordinate is not available")

    def on_error(self, status_code):
        if status_code == 420:
            return False
        else:
            print(status_code)

def stanfordnlp(para):
    nlp = StanfordCoreNLP('http://localhost:9000')
    text=re.sub('[^ A-Za-z0-9,.]+', '', para)
    res = nlp.annotate(text, properties={
                       		'annotators': 'sentiment',
                       		'outputFormat': 'json',
                       		'timeout': 10000})
    return res

# find most frequent sentiment of tweet
def frequentSentiment(res):
    result = ""
    count = 0
    for s in res["sentences"]:
	tmp = s["sentiment"].encode('utf-8')
	if count == 0:
	    result = tmp
	    count = count + 1
	elif result == tmp:
	    count = count + 1
	else:
		count = count - 1
	return result

myStream = tweepy.Stream(auth=auth, listener=MyStreamListener())

myStream.filter(track=[hashtag])







