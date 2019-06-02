import csv

import boto3

from config.config import get_config


class DynamoDB(object):
    def __init__(self):
        conf = get_config()
        aws_access_key_id = conf["amazon"]["access_tokens"]["access_key_id"]
        aws_secret_access_key = conf["amazon"]["access_tokens"]["secret_access_key"]

        sts_client = boto3.client('sts',
                                  aws_access_key_id=aws_access_key_id,
                                  aws_secret_access_key=aws_secret_access_key)

        # Call the assume_role method of the STSConnection object and pass the role
        # ARN and a role session name.
        assumed_role_object = sts_client.assume_role(
            RoleArn="arn:aws:iam::835348665944:role/dynamo_db_full_access",
            RoleSessionName="AssumeRoleSession1")

        # From the response that contains the assumed role, get the temporary
        # credentials that can be used to make subsequent API calls
        credentials = assumed_role_object['Credentials']

        # Use the temporary credentials that AssumeRole returns to make a
        # connection to Amazon S3
        self.dynamodb = boto3.resource(
            'dynamodb',
            aws_access_key_id=credentials['AccessKeyId'],
            aws_secret_access_key=credentials['SecretAccessKey'],
            aws_session_token=credentials['SessionToken'],
            region_name='eu-west-1')

        self.tweet_table = self.dynamodb.Table('TweetSecond')
        
    def yield_tweets(self, batches=None):
        response = self.tweet_table.scan()
        data = response['Items']
        first = True
        batch_no = 1
        while 'LastEvaluatedKey' in response:
            response = self.tweet_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            batch_no += 1

            if batches and batch_no >= batches:
                raise StopIteration

            if first:
                data.extend(response['Items'])
                first = False
                yield data
            else:
                yield response["Items"]

    def save_to_csv(self, path):
        first = True
        keys = list()
        with open(path, 'a', encoding='utf-8') as fd:
            writer = csv.writer(fd)
            try:
                for batch in self.yield_tweets(batches=3):
                    for tweet in batch:
                        if first:
                            keys = list(tweet.keys())
                            writer.writerow(keys)
                            first = False
                        try:
                            fields = list()
                            for key in keys:
                                fields.append(str(tweet[key]))
                                writer.writerow(fields)
                        except KeyError as e:
                            print(str(e))
                            pass
            except Exception as err:
                print(str(err))


if __name__ == "__main__":
    DynamoDB().save_to_csv("D:\\ZTIS\\database.csv")
