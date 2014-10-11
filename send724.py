#Background Info
#maximum of 350 OAuth calls/hour 
#2400 tweets per day leaves 25 tweets every fifteen minutes

#This script maintains a list of up to 25 Status objects.  Each index in the list references a list
#of twitter statuses which are pulled from twitter using the tweepy API every 15 minutes.  This is done
#in the replenish function.  The lists are then traversed, holding the depth reference constant, and replying to
#the tweets in order.  After 25 tweets are sent the program will sleep for 15 minutes

import tweepy, sys, time, datetime, hashlib, logging
from keys import keys,list
try:
    set
except NameError:
    from sets import Set as set
from threading import Thread
import Queue
######################################################################################
#MAX number of tweets to be sent at a time
MAX=15
#interval, in compliance with twitter standards, with which messages will be relayed
NAP = 900
#How long elapses between tweet group executions
TWEET_GROUP_NAP = 180
#maximum size of tweet list
MAX_TWEETLIST_SIZE = 8000
#Number of executions in Nap interval
TWEET_THRESHOLD = (NAP / TWEET_GROUP_NAP)
#create a list of lists, each list corresponding to a queue of tweets mentioning its respective hashtag
tweet_search_results = [] 
#Number of accounts from which tweets are being sent from
num_acct = len(list)
#hash of previous tweets (used to prevent duplicates in replenish()
previous_tweets = set()	
#list of Authentications
apis = []
#key value pair for tags and messages
message_query_pairs = {}
#list of tweets to be shared between producer and consumer
shared_list = []
######################################################################################

class keyValue:
	def __init__(self,hashtag,message,actualTweet):
		self.hashtag = hashtag
		self.message = message
		self.actualTweet = actualTweet


for account in range(0, num_acct):
	key = list[account]
	#import of twitter variables from keys class 
	CONSUMER_KEY = key['consumer_key']
	CONSUMER_SECRET = key['consumer_secret']
	ACCESS_TOKEN = key['access_token']
	ACCESS_TOKEN_SECRET = key['access_token_secret']

	#necessary authentication for tweepy		
	auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
	auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
	apis.append(tweepy.API(auth))

logger = logging.getLogger('logFILE')
hdlr = logging.FileHandler('./logFILE.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr) 
logger.setLevel(logging.INFO)


#alternating lines of input file store keywords and messages, respectively.  Load these lines into query and message,
#keeping track of the number of queries using the number variable
def grabInput():
	logger.info("reading hashtags and messages")
	global number
	global message_temp
	global message
	global query
	global query_temp
	query = []
	message = []
	number = 0
	count = 0
	f = open("input.txt", "r")
	while True:
		current = f.readline().rstrip()
		if not current: break 		#check for consice
		number = number+1
									#odd numbered lines, which are hashtags
		if number%2 == 1:
			query.append(current)
			query_temp=current
									#even numbered lines, what are corresponding messages
		elif number%2 == 0:
			count = count + 1
			if len(current) <= 140:
				message.append(current)
				message_temp=current
				message_query_pairs.update({query_temp:message_temp})
				# Check if access time is CONSTANT
			else:
				query.pop()
				count = count + 1
						
	number = number - count
	num_mess = "There are %d hashtags with corresponding messages" % (number)
	logger.info(num_mess)
	
#clear the lists, and fill them with tweets fetched from the api call.  make sure tweets have not been sent 
#by logging sent tweets in previous_tweets and checking each tweet against this hash.  only append recent tweets
#and those not in the blacklist.  Distribute api call work between the list of accounts 
def replenish():
	
	del tweet_search_results[ 0:len(tweet_search_results) ]
	
	logger.info("fetching tweets")
	logger.info("replenishing done")
	
	total_valid_new_tweets=0
	current_account = 0
	
	#all tweets are compared against a single time
	current = datetime.datetime.utcnow()	
	
	for hashtag_query_index in range(0, number):
		
		goodtime_but_blacklist = 0
		tweet_results = []
		#valid_tweets_to_append = []
		tweet_results = apis[current_account].search(q=query[hashtag_query_index], count=MAX)
			
		count_for_hashtag = 0

		for tweet_result_index in range(0, len(tweet_results)): 
			
			validate_tweet = tweet_results[tweet_result_index]
			
			tweet_time = validate_tweet.created_at
			timer = (current - tweet_time).seconds
						
			if (validate_tweet.id not in previous_tweets):
				if (timer < NAP):
					if (validate_tweet.user.screen_name not in BLACKLIST):
						
						if(len(shared_list) >= MAX_TWEETLIST_SIZE):
							shared_list.pop(0)
							
						object_to_queue = keyValue(query[hashtag_query_index],message[hashtag_query_index],validate_tweet)
						
						shared_list.append(object_to_queue)
						previous_tweets.add(validate_tweet.id)
						count_for_hashtag = count_for_hashtag + 1
						total_valid_new_tweets = total_valid_new_tweets + 1
							
					else:
						
						goodtime_but_blacklist = goodtime_but_blacklist + 1
						
			
		#this is where the log file is written to
		valid_tweet_message = "***HASHTAG QUERY*** Valid New:%d    Blacklisted:%d    Old:%d    Hashtag:%s" % (count_for_hashtag, 		goodtime_but_blacklist, MAX - count_for_hashtag - goodtime_but_blacklist, query[hashtag_query_index])
		
		logger.info(valid_tweet_message)
		
		#manage the account reference
		current_account = current_account + 1
		if current_account >= num_acct:
			current_account = 0
			time.sleep(5)	
			
	logger.info("Total valid new tweets: %d" % (total_valid_new_tweets))
	logger.info("The number of items in the shared list is %d" % (len(shared_list)))
	
def QuerySend(): 
	logger.info("QuerySend() beings")

	account_reference = 0
	
	while True:
		if len(shared_list) != 0:
			
				
			obj = shared_list.pop() 		
			tweet = obj.actualTweet
				
			twitter_user = tweet.user.screen_name
			temp = obj.hashtag.rstrip()
			reply = message_query_pairs[temp]
				
			message_to_send = "@%s %s" % (twitter_user, reply)
			#Send tweets in a try-except block
			
			try:
						
				tweepy_user =  apis[account_reference].me().id
				apis[account_reference].update_status(message_to_send, tweet.id)
				print_log = "TWEETid: %d    Response to:%s      sent: %s" % 				(tweet.id, obj.hashtag, message_to_send) 
				logger.info(print_log)
						
			except Exception as e:
				#e = sys.exc_info()[0]
				logger.info("%s, account is %d" % (e, tweepy_user))
				#pass						

			finally:
					
				#We want to always updat the account refernence.  To account for the send if
				#try works, and to prevent a hang if we catch an exception on that specific
				#account authorization
				account_reference = account_reference + 1
					
		#prevent out of control spin
		else:
			time.sleep(1)	
						
		#this equality being true implies all accounts have been cycled through
		if account_reference == num_acct:
					
			logger.info("****Sleeping for %d seconds****" % (TWEET_GROUP_NAP))
			time.sleep(TWEET_GROUP_NAP)
			account_reference = 0

def createblacklistHash():
	logger.info("creating blacklist hash")
	global BLACKLIST
	BLACKLIST = set()
	f = open("blacklist.txt", "r")
	while True:
		current = f.readline()
		if not current: break 	
		BLACKLIST.add(current.rstrip())

def produce_caller():
	while True:
		try:
			createblacklistHash()	
			grabInput()
			replenish()
			time.sleep(NAP)
		except Exception as e:
			logger.info("producer thread error: \n%s" % e)
			pass
		

def consume_caller():
	try:
		QuerySend()
	except Exception as e:
		logger.info("consumer thread error: \n%s" % e)
		pass
		

#thread 1 is producer
t1 = Thread(target=produce_caller)

#thread 2 is consumer
t2 = Thread(target=consume_caller)


t1.start()

t2.start()


		
