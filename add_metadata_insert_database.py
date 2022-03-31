# Library imports
import json
import gzip
import tweepy
from tweepy import OAuthHandler
import pymongo
import datetime
import time
import pandas as pd
import numpy as np
from tweetedat.script import TimestampEstimator

# Twitter Authentication
with open("Twitter_API_keys_melihcanyardi.json", 'r') as f:
    keys_tokens = json.load(f)
    
## tweepy.Client
bearer_token = keys_tokens['bearer_token']
client = tweepy.Client(bearer_token=bearer_token, wait_on_rate_limit=True)

## tweepy.API
consumer_key = keys_tokens['consumer_key']
consumer_secret = keys_tokens['consumer_secret']
access_token = keys_tokens['access_token']
access_token_secret = keys_tokens['access_token_secret']

auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth, wait_on_rate_limit=True)


# Connect to Database
with open("MongoDB_keys.json", 'r') as f:
    mongodb_keys = json.load(f)
    
username = mongodb_keys["username"]
password = mongodb_keys["password"]
db_name = mongodb_keys["db_name"]
mongo_client = pymongo.MongoClient('mongodb://' + username + ':' + password + '@' + db_name + '/?ssl=true&ssl_ca_certs=rds-combined-ca-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false')

db = mongo_client.twitter_db
user_col = db.user_collection # user_col: user objects to be read (without metadata)
twitter_col = db.twitter_collection # twitter_col: user objects to be written (with metadata)


# Add Metadata
def get_tweet_id_year(twid):
    tstamp = TimestampEstimator.find_tweet_timestamp(int(twid))
    utcdttime = datetime.datetime.utcfromtimestamp(tstamp / 1000)
    return utcdttime.date().strftime("%y%m%d")

def add_metadata(user_object):
    """Get users_following, users_followers, tweets (original, reply,
    quote, retweet), follower_count following_count of a user;
    append them to the user object."""

    # drop "meta" and "_id"
    user_object.pop("meta")
    user_object.pop("_id")

    user_id = int(user_object["id_str"])
    
    # users_following
    users_following = client.get_users_following(id=user_id)
    if users_following.data == None:
        following = []
    else:
        following = [str(user.id) for user in users_following.data]
        
    # users_followers
    users_followers = client.get_users_followers(id=user_id)
    if users_followers.data == None:
        followers = []
    else:
        followers = [str(user.id) for user in users_followers.data]
        
    # users_tweets
    users_tweets = client.get_users_tweets(id=user_id,
                                           tweet_fields=["referenced_tweets"],
                                           expansions=["referenced_tweets.id",
                                                       "referenced_tweets.id.author_id"])
    if users_tweets.data == None:
        tweets = []
    else:
        tweets = []

        for tweet in users_tweets.data:
            ## Referenced Tweets:
            if "referenced_tweets" in tweet.data.keys():
                ## Replied Tweets:
                if tweet.data["referenced_tweets"][0]["type"] == "replied_to":
                    replied_tweet_id = tweet.data["referenced_tweets"][0]["id"]
                    # If "text" for Replied Tweet is not available:
                    if client.get_tweet(id=replied_tweet_id).data == None:
                        tweets.append({"twt_id_str":str(tweet.id),
                                       "twt_txt": tweet.text,
                                       "ref_twt_id_str": str(replied_tweet_id),
                                       "ref_twt_txt": "",
                                       "ref_usr_id_str": "",
                                       "type": "reply"})
                    else:
                        replied_user_id = client.get_tweet(id=replied_tweet_id, expansions=["author_id"]).includes['users'][0].id
                        tweets.append({"twt_id_str":str(tweet.id),
                                       "twt_txt": tweet.text,
                                       "ref_twt_id_str": str(replied_tweet_id),
                                       "ref_twt_txt": client.get_tweet(replied_tweet_id).data.text,
                                       "ref_usr_id_str": str(replied_user_id),
                                       "type": "reply"})
                ## Quote Tweets:
                if tweet.data["referenced_tweets"][0]["type"] == "quoted":
                    quoted_tweet_id = tweet.data["referenced_tweets"][0]["id"]
                    ## If "text" for Quoted Tweet is not available
                    if client.get_tweet(id=quoted_tweet_id).data == None:
                        tweets.append({"twt_id_str":str(tweet.id),
                                       "twt_txt": tweet.text,
                                       "ref_id_str": str(quoted_tweet_id),
                                       "ref_txt": "",
                                       "type": "quote"})
                    else:
                        quoted_user_id = client.get_tweet(id=quoted_tweet_id, expansions=["author_id"]).includes['users'][0].id
                        tweets.append({"twt_id_str":str(tweet.id),
                                       "twt_txt": tweet.text,
                                       "ref_twt_id_str": str(quoted_tweet_id),
                                       "ref_twt_txt": client.get_tweet(quoted_tweet_id).data.text,
                                       "ref_usr_id_str": str(quoted_user_id),
                                       "type": "quote"})
                ## Retweets:
                if tweet.data["referenced_tweets"][0]["type"] == "retweeted":
                    retweeted_tweet_id = tweet.data["referenced_tweets"][0]["id"]
                    ## If "text" for Retweet is not available
                    if client.get_tweet(id=retweeted_tweet_id).data == None:
                        tweets.append({"ref_twt_id_str": str(retweeted_tweet_id),
                                       "ref_twt_txt": "",
                                       "type": "retweet"})
                    else:
                        retweeted_user_id = client.get_tweet(id=retweeted_tweet_id, expansions=["author_id"]).includes['users'][0].id
                        tweets.append({"ref_twt_id_str": str(retweeted_tweet_id),
                                       "ref_twt_txt": client.get_tweet(retweeted_tweet_id).data.text,
                                       "ref_usr_id_str": str(retweeted_user_id),
                                       "type": "retweet"})
            else:
                tweets.append({"twt_id_str": str(tweet.id), "twt_txt": tweet.text, "type": "original"})
                
    # likes
    liked_tweets = client.get_liked_tweets(id=user_id, expansions=["author_id"])
    if liked_tweets.data != None:
        tweets.extend([{"ref_twt_id_str": str(tweet.id),
                        "ref_twt_txt": tweet.text,
                        "ref_usr_id_str": str(tweet.author_id),
                        "type": "fav"} for tweet in liked_tweets.data])
    
    # append new key-value pairs
    user_object["following"] = following
    user_object["followers"] = followers
    user_object["tweets"] = tweets
    
    # add date to tweets
    if user_object["tweets"]:
        for tweet in user_object["tweets"]:
            if tweet["type"] == "original":
                tweet["twt_date"] = get_tweet_id_year(tweet["twt_id_str"])
            else:
                tweet["twt_date"] = get_tweet_id_year(tweet["ref_twt_id_str"])
                
    try:
        user_info = api.get_user(user_id=user_id)
        # add "followers_count" and "following_count"
        user_object["followers_count"] = user_info.followers_count
        user_object["following_count"] = user_info.friends_count
        # add profile image URL
        pp_url = user_info.profile_image_url_https
        if pp_url.split("/")[2] == 'pbs.twimg.com':
            return_pp_url = '/'.join(pp_url.split("/")[-2:])
        elif pp_url.split("/")[2] == 'abs.twimg.com':
            return_pp_url = ""
        user_object["pp"] = return_pp_url

    except:
        # add "followers_count" and "following_count"
        user_object["followers_count"] = np.nan
        user_object["following_count"] = np.nan
        # add profile image URL
        return_pp_url = ""
        user_object["pp"] = return_pp_url
        
    # add downloaded date
    user_object["downloaded"] = datetime.datetime.now().strftime("%y%m%d")
    
    return user_object

while True:
    try:
        # Insert metadata-added user objects into "twitter_col" & update "meta" key in "user_col"
        for user in user_col.find({"meta": False}).batch_size(15):
            user_id_str = user['id_str']
            print(f"User ID: {user_id_str}")
            print(f"Adding metadata to {user_id_str} & inserting it into twitter_collection.")
            twitter_col.insert_one(add_metadata(user)) # insert into "twitter_col"
            print(f"Updating 'meta' in user_collection for {user_id_str}.\n")
            user_col.update_one(filter={'id_str': user_id_str}, update={'$set': {"meta": True}}) # update "meta" in "user_col"
    except:
        print(f"Curser timed out.\nDocument count of twitter_collection: {twitter_col.count_documents({})}\n\n\n")
        time.sleep(15)
