"""
Retrieve metadata for list of tweet IDs using Twitter's REST API

This script will iterate through a folder of .txt files, where each file
contains break-seperated tweetIDs of tweets to get metadata for. Intended to 
be used as the processing step after get_conversations.py, which will produce
separate .txt files of tweetIDs connected to a given seed tweet.

Requires: 
	twitter_api_auth_round.json : file with Twitter authorization information

Output:
	For each .txt file, creates a .json.gzip file with full metadata for tweets
	
"""

### Parameters to set ###
query_list = ['Manchin', 'Murkowski', 'Collins','Heitkamp','Cruz','Hyde-Smith', 'Flake']
output_path = "../data/REST/"
### End customization

import tweepy
from tweepy import OAuthHandler
from datetime import datetime
import os
import json
from auth import TwitterAuth
from time import sleep
import sys
import gzip


### Step 1: validate credentials ###
auth = tweepy.OAuthHandler(TwitterAuth.API_key, TwitterAuth.API_secret)
auth.set_access_token(TwitterAuth.access_token, TwitterAuth.access_token_secret)

my_twitter_app = tweepy.API(auth)
requests = 0

for query in query_list:
	print('Searching tweets with keyword %s' %query)
	output_file = '%s_REST.json.gzip' %query
	max_tweets = 10000

	searched_tweets = []
	last_id = -1

	while len(searched_tweets) < max_tweets:
	    count = 100

	    try:
	    	new_tweets = my_twitter_app.search(q=query, count=count, max_id=str(last_id - 1))

	        if not new_tweets:
	            break

	        searched_tweets.extend(new_tweets)
	        last_id = new_tweets[-1].id

	        requests += 1
	        if requests%179 == 0:
	        	print('Sleeping for 15 minutes...')
	        	sleep(15 *60)

	    except tweepy.TweepError as e:
	        # depending on TweepError.code, one may want to retry or wait
	        # to keep things simple, we will give up on an error
	        break

	# if we found tweets...
	if len(searched_tweets) > 0:
		with gzip.open(output_path + output_file, 'a') as fout:
			for tweet in searched_tweets:
				print >> fout, json.dumps(tweet._json)

	print('%s tweets written to file for query %s.' %(len(searched_tweets), query))
