#Use the Tweepy Library to connect to the API and start streaming data. 
import tweepy
import json
from dateutil import parser
import time
import os
import subprocess
import pyodbc 
import http.client

#Our Credentials for Twitter API App
consumer_key = "DrhJOGilzxVB8FvV5WYvJ5t11"
consumer_secret= "MaWhCzZelZbBwa6Q6bqWM0meI8rqDmElPF03IdsfGNSuqpil4U"
access_token= "1098313089630564352-D8UckNvBgVKQ63NOu6KYDksG7PUeyv"
access_token_secret="1Xc10HWckHCbEzdWj9ilCarM2rngJXCWj2HVnmNPj9KY6"
#Authentication to use Tweepy
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

#Azure database credential details
server = 'rateurrefserver.database.windows.net' 
database = 'RateUrRefDb' 
dbUsername = 'RateUrRef' 
password = 'CSGals1234' 

#Connecting to the columns of our database
#list of hashtags and user mentions are going to need to be stored in their own tables and related to the tweet table
def connect(tweet_id, text,quoted_id, retweet_id, username, screen_name, created_at, user_location, place, retweet_count, favorite_count, verified, hashtags, user_mentions):
	try:
		#con = mysql.connector.connect(host = 'localhost',
		#database= 'twitterdb', user='root', password = 'sesame', charset = 'utf8')
		cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+dbUsername+';PWD='+ password)
		#Some attributes we can use pyodbc.connect(Driver={ODBC Driver 13 for SQL Server};Server=tcp:rateurrefserver.database.windows.net,1433;Database=RateUrRefDb;Uid=RateUrRef@rateurrefserver;Pwd={your_password_here};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;)
		cursor = cnxn.cursor()

		#Insert into tweets table
		tweet_query = "INSERT INTO [dbo].[tweets] (tweet_id, text,quoted_id,retweet_id,username,screen_name, created_at, user_location, place, verified) VALUES (?,?,?,?, ?, ?, ?, ?, ?, ?)"
		cursor.execute(tweet_query, (tweet_id, text, quoted_id,retweet_id, username, screen_name, created_at, user_location, place, verified))
		
		#Insert into Hashtag table
		for aHashtag in hashtags: #Stores all hashtags from tweet into hashtag table
			hashtag_query = "INSERT INTO [dbo].[hashtags] (tweet_id, hashtag) VALUES (?,?)"
			cursor.execute(hashtag_query, (tweet_id, aHashtag['text']))
		
		#Insert into User_mentions table
		for aUserMention in user_mentions: #Stores all user_mentions from tweet into user_mention table
			user_mention_query = "INSERT INTO [dbo].[user_mentions]  (tweet_id, name, screen_name) VALUES (?,?,?)"
			cursor.execute(user_mention_query, (tweet_id, aUserMention['name'], aUserMention['screen_name']))
		
		cnxn.commit()
	except (RuntimeError,TypeError, NameError, ValueError, http.client.IncompleteRead) as e: 
			print(e)
	
	finally:
		cursor.close()
		del cursor
		cnxn.close()
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
			#Gather tweet information from the json data
			if 'text' in raw_data:
					tweet_id = raw_data['id_str']
					username = raw_data['user']['name']
					screen_name = raw_data['user']['screen_name']
					created_at = parser.parse(raw_data['created_at'])

					#Ensure that text > 140 characters is retrieved from the extended_tweet object
					if 'extended_tweet' in raw_data:
						if 'full_text' in raw_data['extended_tweet']:
							text = raw_data['extended_tweet']['full_text']
					elif 'text' in raw_data:
						text = raw_data['text']
					
					#Does tweet contain a quote? If yes, store quoted tweet's id
					if raw_data['is_quote_status']== True:
						quoted_id = raw_data['quoted_status_id_str']
					else: quoted_id = None

					#Is tweet a retweet? If yes, store retweets tweet's id
					if 'retweeted_status' in raw_data:
						if 'id_str' in raw_data['retweeted_status']:
							retweet_id = raw_data['retweeted_status']['id_str']
					else:retweet_id = None

					user_location = raw_data['user']['location']
					retweet_count = raw_data['retweet_count']
					favorite_count = raw_data['favorite_count']
					verified = raw_data['user']['verified']
					hashtags = raw_data['entities']['hashtags']
					user_mentions = raw_data['entities']['user_mentions']

					if raw_data['place'] is not None:
						place = raw_data['place']['name'] #might want to change to full_name
						print(place)
					else:
						place = None				

					connect(tweet_id, text,quoted_id, retweet_id,username,screen_name, created_at, user_location, place, retweet_count, favorite_count, verified, hashtags, user_mentions)
					print("Tweet collected at: {}".format(str(created_at)))
		except (RuntimeError,TypeError, NameError, ValueError, http.client.IncompleteRead) as e: 
			print(e)

if __name__ == '__main__':
	auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_token, access_token_secret)
	api = tweepy.API(auth, wait_on_rate_limit=True)

	#create instance of Streamlistener
	listener = Streamlistener(api = api)
	stream = tweepy.Stream(auth, listener = listener, tweet_mode = 'extended')

	#List of Words to search tweets for in the live Listener/ If we have too many words than an Incomplete error is thrown because can't keep up with the tweets
	track = ['bad call','ref','refs', 'referee', 'referees']
	#possible other words (taking out to test = 'championship','college bball', 'college basketball','elite 8', 'elite eight','final 4', 'final four','march madness','National Championship','NCAA basketball','NCAA bball','NCAA hoops', ,'official', 'officials', 'officiating',,'Round of 64','Round of 32','sweet 16', 'sweet sixteen') 
	stream.filter(track = track, languages = ['en'], stall_warnings=True)

