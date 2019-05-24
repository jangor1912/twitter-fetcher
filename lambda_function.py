import json
import random

import boto3
from boto3.dynamodb.conditions import Key
from decimal import Decimal

from config.config import get_config
from fetcher.fetcher import Fetcher
from uils import remove_nones


def lambda_handler(event, context):
    since_date = "2019-01-01"
    fetcher = Fetcher()
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('TweetSecond')

    queries = get_config(config_file_path="queries/queries.yml")
    screen_names = queries["users"]
    tags = queries["tags"]

    # shuffle lists
    random.shuffle(tags)
    random.shuffle(screen_names)

    fetched_tweets = []
    unique_ids = []

    for screen_name in screen_names:
        latest_user_tweet_id = None
        try:
            result = table.query(IndexName="user_screen_name-id_str-index",
                                 KeyConditionExpression=Key('user_screen_name').eq(screen_name),
                                 ScanIndexForward=False,
                                 Limit=1)
            latest_user_tweet_id = int(result["Items"][0]["id_str"])
        except Exception as e:
            print(e)

        print("Latest user tweet id: " + str(latest_user_tweet_id))

        currently_fetched_tweets = fetcher.get_user_timeline_tweets(screen_name=screen_name,
                                                                    since_id=latest_user_tweet_id)
        for tweet in currently_fetched_tweets:
            tweet_dict = tweet._json
            tweet_dict["user_screen_name"] = screen_name
            tweet_dict["hashtag"] = "None"
            tweet_dict = json.loads(json.dumps(tweet_dict), parse_float=Decimal)
            tweet_dict = remove_nones(tweet_dict)

            if tweet_dict["id_str"] not in unique_ids:
                fetched_tweets.append(tweet_dict)
                unique_ids.append(tweet_dict["id_str"])

    for tag in tags:
        results = []
        latest_tag_tweet_id = None
        try:
            result = table.query(IndexName="hashtag-id_str-index",
                                 KeyConditionExpression=Key('hashtag').eq(tag),
                                 ScanIndexForward=False,
                                 Limit=1)
            latest_tag_tweet_id = int(result["Items"][0]["id_str"])
        except Exception as e:
            print(e)
        if results:
            print("DynamoDB query results: " + str(results))
        print("Latest user tweet id: " + str(latest_tag_tweet_id))

        currently_fetched_tweets = fetcher.get_tweets_by_term(term=tag,
                                                              since_id=latest_tag_tweet_id,
                                                              since=since_date)
        for tweet in currently_fetched_tweets:
            tweet_dict = tweet._json
            tweet_dict["user_screen_name"] = "None"
            tweet_dict["hashtag"] = tag
            tweet_dict = json.loads(json.dumps(tweet_dict), parse_float=Decimal)
            tweet_dict = remove_nones(tweet_dict)

            if tweet_dict["id_str"] not in unique_ids:
                fetched_tweets.append(tweet_dict)
                unique_ids.append(tweet_dict["id_str"])

    print("Writing to DynamoDb")
    # write to DynamoDB
    with table.batch_writer() as batch:
        for tweet in fetched_tweets:
            batch.put_item(Item=tweet)
    print("Finished writing to DynamoDb")
