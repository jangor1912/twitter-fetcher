#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Downloads all tweets from a given user.
Uses twitter.Api.GetUserTimeline to retreive the last 3,200 tweets from a user.
Twitter doesn't allow retreiving more tweets than this through the API, so we get
as many as possible.
t.py should contain the imported variables.
"""

from __future__ import print_function

import json

import twitter
from twitter import TwitterError

from config.config import get_config


class Fetcher(object):
    def __init__(self):
        conf = get_config()
        consumer_key = conf["twitter"]["keys"]["api"]
        consumer_secret_key = conf["twitter"]["keys"]["api_secret"]
        access_token = conf["twitter"]["access_tokens"]["access_token"]
        access_token_secret = conf["twitter"]["access_tokens"]["access_token_secret"]

        api = twitter.Api(consumer_key,
                          consumer_secret_key,
                          access_token,
                          access_token_secret)
        self.api = api

    def get_user_timeline_tweets(self, screen_name=None, since_id=None):
        tweets = list()
        try:
            tweets = self.api.GetUserTimeline(screen_name=screen_name,
                                              count=200,
                                              since_id=since_id)
            if len(tweets) == 0:
                print("Timeline of user={} is up to date".format(screen_name))
                return []
            earliest_tweet = min(tweets, key=lambda x: x.id).id
            print("getting tweets before:", earliest_tweet)

            while True:
                new_tweets = self.api.GetUserTimeline(screen_name=screen_name,
                                                      max_id=earliest_tweet,
                                                      count=200,
                                                      since_id=since_id)
                new_earliest = min(new_tweets, key=lambda x: x.id).id

                if not new_tweets or new_earliest == earliest_tweet:
                    break
                else:
                    earliest_tweet = new_earliest
                    print("getting tweets before:", earliest_tweet)
                    tweets += new_tweets
        except TwitterError as e:
            print(e.message)
            pass
        return tweets

    def get_tweets_by_term(self, term=None, since_id=None, since=None):
        tweets = list()
        try:
            tweets = self.api.GetSearch(term=term,
                                        count=200,
                                        since_id=since_id,
                                        since=since)
            earliest_tweet = min(tweets, key=lambda x: x.id).id
            print("getting tweets before:", earliest_tweet)

            while True:
                new_tweets = self.api.GetSearch(term=term,
                                                max_id=earliest_tweet,
                                                count=200,
                                                since_id=since_id,
                                                since=since)
                new_earliest = min(new_tweets, key=lambda x: x.id).id

                if not new_tweets or new_earliest == earliest_tweet:
                    break
                else:
                    earliest_tweet = new_earliest
                    print("getting tweets before:", earliest_tweet)
                    tweets += new_tweets
        except TwitterError as e:
            print(e.message)
            pass
        return tweets

    def get_users_that_like_tweet(self, tweet):
        raise NotImplementedError("Getting users that liked specific tweet is not implemented yet")

    def get_users_that_retweet_tweet(self, tweet):
        tweet_id = tweet["id"]
        retweets = self.api.GetRetweets(tweet_id)
        retweets = [retweet._json["user"]["screen_name"] + "_" + retweet._json["user"]["id_str"]
                    for retweet in retweets]
        return retweets


if __name__ == "__main__":
    # term = "eu"
    # print(term)
    # # results = Fetcher().get_user_timeline_tweets(screen_name=screen_name)
    # results = Fetcher().get_tweets_by_term(term=term)
    #
    # with open('examples/timeline.json', 'w+') as f:
    #     f.write("[")
    #     for tweet in results:
    #         f.write(json.dumps(tweet._json))
    #         f.write(',\n')
    #     f.write("]")
    user_names = Fetcher().get_users_that_retweet_tweet({"id": 1000034550872002565})
    print("User name no: {}".format(len(user_names)))
    for user_name in user_names:
        print(user_name)
