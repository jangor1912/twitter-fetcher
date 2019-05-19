import random

import boto3

from config.config import get_config
from fetcher.fetcher import Fetcher


def lambda_handler(event, context):
    since = "2019-01-01"
    fetcher = Fetcher()
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('Tweet')

    queries = get_config(config_file_path="queries/queries.yml")
    screen_names = queries["users"]
    tags = queries["tags"]

    # shuffle lists
    random.shuffle(tags)
    random.shuffle(screen_names)

    fetched_tweets = []

    for screen_name in screen_names:
        latest_user_tweet_id = None
        results = table.query(KeyConditionExpression={'user': {"screen_name": screen_name}},
                              ScanIndexForward=False,
                              Limit=1)
        if results:
            print("DynamoDB query results: " + results)
            latest_user_tweet_id = results[0]["id"]
        print("Latest user tweet id: " + latest_user_tweet_id)

        # TODO - change since_id
        fetched_tweets += fetcher.get_user_timeline_tweets(screen_name=screen_name,
                                                           since_id=None)

    for tag in tags:
        latest_tag_tweet_id = None
        results = table.query(KeyConditionExpression={'entities': {"hashtags": tag}},
                              ScanIndexForward=False,
                              Limit=1)
        if results:
            print("DynamoDB query results: " + results)
            latest_tag_tweet_id = results[0]["id"]
        print("Latest user tweet id: " + latest_tag_tweet_id)

        # TODO - change since_id
        fetched_tweets += fetcher.get_tweets_by_term(term=tag,
                                                     since_id=None,
                                                     since=since)

    # write to DynamoDB
    with table.batch_writer() as batch:
        for tweet in fetched_tweets:
            batch.put_item(Item=dict(tweet._json))
