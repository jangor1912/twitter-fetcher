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

from config.config import get_config


class Fetcher(object):
    def __init__(self):
        conf = get_config()
        consumer_key = conf["keys"]["api"]
        consumer_secret_key = conf["keys"]["api_secret"]
        access_token = conf["access_tokens"]["access_token"]
        access_token_secret = conf["access_tokens"]["access_token_secret"]

        api = twitter.Api(
            consumer_key, consumer_secret_key, access_token, access_token_secret
        )
        self.api = api

    def get_tweets(self, screen_name=None):
        timeline = self.api.GetUserTimeline(screen_name=screen_name, count=200)
        earliest_tweet = min(timeline, key=lambda x: x.id).id
        print("getting tweets before:", earliest_tweet)

        while True:
            tweets = self.api.GetUserTimeline(
                screen_name=screen_name, max_id=earliest_tweet, count=200
            )
            new_earliest = min(tweets, key=lambda x: x.id).id

            if not tweets or new_earliest == earliest_tweet:
                break
            else:
                earliest_tweet = new_earliest
                print("getting tweets before:", earliest_tweet)
                timeline += tweets

        return timeline


if __name__ == "__main__":
    screen_name = "realDonaldTrump"
    print(screen_name)
    timeline = Fetcher().get_tweets(screen_name=screen_name)

    with open('examples/timeline.json', 'w+') as f:
        for tweet in timeline:
            f.write(json.dumps(tweet._json))
            f.write('\n')
