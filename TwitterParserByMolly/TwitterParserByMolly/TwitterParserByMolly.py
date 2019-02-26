#1. Create a class that allows us to connect to the Twitter API
#2. Create some code that connects to our database and reads the data into the correct columns 

#Use the Tweepy Library to connect to the API and start streaming data. 
import mysql.connector
from mysql.connector import Error
import tweepy
import json
from dateutil import parser
import time
import os
import subprocess

#Our Credentials for Twitter API App
consumer_key = "DrhJOGilzxVB8FvV5WYvJ5t11"
consumer_secret= "MaWhCzZelZbBwa6Q6bqWM0meI8rqDmElPF03IdsfGNSuqpil4U"
access_token= "1098313089630564352-D8UckNvBgVKQ63NOu6KYDksG7PUeyv"
access_token_secret="1Xc10HWckHCbEzdWj9ilCarM2rngJXCWj2HVnmNPj9KY6"


auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

#Connecting to the columns of our database
def connect(username, created_at, tweet, retweet_count, place, location):
	try:
		con = mysql.connector.connect(host = 'localhost',
		database= 'twitterdb', user='root', password = 'sesame', charset = 'utf8')

		if con.is_connected():
			cursor = con.cursor()
			#twitter, golf
			query = "INSERT INTO sample_tweet_data (username, created_at, tweet, retweet_count, place, location) VALUES (%s, %s, %s, %s, %s, %s)"
			cursor.execute(query, (username, created_at, tweet, retweet_count, place, location))
			con.commit()
	except Error as e: 
			print(e)
	
	cursor.close()
	con.close()

	return

#Tweepy Class to Access Twitter API 

class Streamlistener(tweepy.StreamListener):
	def on_connect(self):
		print("You are connected to the Twitter API")

	def on_error(self):
		if status_code != 200:
			print("error found")
			return False # may want to change to true if we dont want to kill the stream

	def on_data(self, data):
		try:
			raw_data = json.loads(data)
			if 'text' in raw_data:
					username = raw_data['user']['screen_name']
					created_at = parser.parse(raw_data['created_at'])
					tweet = raw_data['text']
					retweet_count = raw_data['retweet_count']

					if raw_data['place'] is not None:
						place = raw_data['place']['country']
						print(place)
					else:
						place = None
					
					location = raw_data['user']['location']

					#insert data just collected into MySql database 

					connect(username, created_at, tweet, retweet_count, place, location)
					print("Tweet collected at: {}".format(str(created_at)))
		except Error as e:
			print(e)

if __name__ == '__main__':
	auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_token, access_token_secret)
	api = tweepy.API(auth, wait_on_rate_limit=True)

	#create instance of Streamlistener

	listener = Streamlistener(api = api)
	stream = tweepy.Stream(auth, listener = listener)

	track = ['march madness', 'basketball']

	#choose what we want to filter by 

	stream.filter(track = track, languages = ['en'])

