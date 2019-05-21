import json
import random

import boto3
from boto3.dynamodb.conditions import Key
from decimal import Decimal

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

    # for screen_name in screen_names:
    #     latest_user_tweet_id = None
    #     results = table.query(KeyConditionExpression=Key('{"user": {"screen_name")').eq(screen_name),
    #                           ScanIndexForward=False,
    #                           Limit=1)
    #     if results:
    #         print("DynamoDB query results: " + results)
    #         latest_user_tweet_id = results[0]["id"]
    #     print("Latest user tweet id: " + latest_user_tweet_id)
    #
    #     # TODO - change since_id
    #     fetched_tweets += fetcher.get_user_timeline_tweets(screen_name=screen_name,
    #                                                        since_id=None)

    for tag in tags:
        # latest_tag_tweet_id = None
        # results = table.query(KeyConditionExpression=Key('{"entities": {"hashtags")').eq(tag),
        #                       ScanIndexForward=False,
        #                       Limit=1)

        # results = table.query(ScanIndexForward=False,
        #                       Limit=1)
        # if results:
        #     print("DynamoDB query results: " + results)
        #     latest_tag_tweet_id = results[0]["id"]
        # print("Latest user tweet id: " + latest_tag_tweet_id)

        # TODO - change since_id
        fetched_tweets += fetcher.get_tweets_by_term(term=tag,
                                                     since_id=None,
                                                     since=since)

    print("Writing to DynamoDb")
    # write to DynamoDB
    first_tweet = True
    with table.batch_writer() as batch:
        for tweet in fetched_tweets:
            if first_tweet:
                print("My first tweet is:")
                print(tweet._json)
                first_tweet = False
            print("Adding tweet ({}) to batch".format(tweet._json["id"]))
            data = tweet._json
            data = json.loads(json.dumps(data), parse_float=Decimal)
            batch.put_item(Item=data)
    print("Finished writing to DynamoDb")
