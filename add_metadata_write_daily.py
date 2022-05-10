# Library imports
import json
import gzip
import tweepy
import datetime
import numpy as np
from tweetedat.script import TimestampEstimator
import os
import sys

batch_no = sys.argv[1]
with open("bearer_tokens.txt", "r") as f:
    bearer_tokens = [x.strip() for x in f.readlines()]
bearer_token = bearer_tokens[int(sys.argv[1])-1]

## tweepy.Client
client = tweepy.Client(bearer_token=bearer_token, wait_on_rate_limit=True)


# Read User Objects
with gzip.open(f"user_object_batches/province_gender_available_uos_batch_{batch_no}.txt.gz", 'rb') as f:
    users = f.readlines()


# Add Metadata
def get_tweet_id_year(twid):
    tstamp = TimestampEstimator.find_tweet_timestamp(int(twid))
    utcdttime = datetime.datetime.utcfromtimestamp(tstamp / 1000)
    return utcdttime.date().strftime("%y%m%d")

def add_metadata(user_object):
    """Get users_following, users_followers, tweets (original, reply,
    quote, retweet), follower_count following_count of a user;
    append them to the user object."""

    # convert from byte to json string
    user_object = json.loads(user_object.decode("utf-8"))

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
                
    # get "profile_image_url", "followers_count" and "following_count" from user_info
    user_info = client.get_user(id=user_id, user_fields=["profile_image_url", "public_metrics"])
    
    if user_info.data:
        # add "followers_count" and "following_count"
        user_object["followers_count"] = user_info.data.public_metrics["followers_count"]
        user_object["following_count"] = user_info.data.public_metrics["following_count"]
        # add profile image URL
        pp_url = user_info.data.profile_image_url
        if pp_url.split("/")[2] == 'pbs.twimg.com':
            return_pp_url = '/'.join(pp_url.split("/")[-2:])
        elif pp_url.split("/")[2] == 'abs.twimg.com':
            return_pp_url = ""
        user_object["pp"] = return_pp_url
    else:
        user_object["followers_count"] = np.nan
        user_object["following_count"] = np.nan
        return_pp_url = ""
        user_object["pp"] = return_pp_url

        
    # add downloaded date
    user_object["downloaded"] = datetime.datetime.now().strftime("%y%m%d")
    
    return user_object

# Create "daily_downloads" folder if not exists
if not os.path.exists("daily_downloads"):
    os.mkdir("daily_downloads")
    
# Create empty txt file if not exists
if f"users_downloaded-batch_{batch_no}.txt" not in os.listdir("daily_downloads"):
    with open(f'daily_downloads/users_downloaded-batch_{batch_no}.txt', 'w') as f:
        pass

# Download user objects
print(f"Batch {batch_no} Progress:")
while True:
    user = users[np.random.randint(low=0, high=100_001)]
    try:
        # Add metadata to user objects & write to daily gzip files
        # Check if user has already been downloaded
        with open(f"daily_downloads/users_downloaded-batch_{batch_no}.txt", "r") as f:
            users_downloaded = [x.strip() for x in f.readlines()]
        if json.loads(user.decode("utf-8"))["id_str"] not in users_downloaded:
            # Write metadata-added user object to gzip file
            with open(f"daily_downloads/province_gender_available_metadata_added-{datetime.datetime.now().strftime('%y%m%d')}_{batch_no}.txt.gz", 'ab') as f:
                f.write(gzip.compress(f"{json.dumps(add_metadata(user))}\n".encode('utf-8')))
            # Write User ID to txt file
            with open(f"daily_downloads/users_downloaded-batch_{batch_no}.txt", "a") as f:
                f.write(f'{json.loads(user.decode("utf-8"))["id_str"]}\n')
        print(f"{len(users_downloaded)}/{len(users)} | {len(users_downloaded)/len(users)*100:.2f}%")
    except:
        continue