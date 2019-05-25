import boto3
import networkx as nx

from config.config import get_config
from fetcher.fetcher import Fetcher


class GraphBuilder(object):
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
            RoleArn="arn:aws:iam::835348665944:role/non_root_dynamo_db",
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
        )

        # self.dynamodb = boto3.resource('dynamodb',
        #                                aws_session_token=aws_session_token,
        #                                aws_access_key_id=aws_access_key_id,
        #                                aws_secret_access_key=aws_secret_access_key,
        #                                region_name=region)

        self.tweet_table = self.dynamodb.Table('tweetSecond')

        self.graph = nx.DiGraph()
        self.fetcher = Fetcher()

    def _yield_tweets(self, batches=None):
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

    def save_to_file(self, output_file_path):
        nx.write_gexf(self.graph, output_file_path)

    def _tweet_to_edges(self, tweet, parameter='favourites'):
        tweet_owner = tweet["user"]["screen_name"] + "_" + tweet["user"]["id_str"]
        if parameter == 'favourites':
            user_list = self.fetcher.get_users_that_like_tweet(tweet)
        elif parameter == 'retweets':
            user_list = self.fetcher.get_users_that_retweet_tweet(tweet)
        else:
            raise RuntimeError("Incorrect parameter. Parameter should be one of {}"
                               .format(str(["favourites", "retweets"])))
        for user_name in user_list:
            if self.graph.has_edge(tweet_owner, user_name):
                # we added this one before, just increase the weight by one
                self.graph[tweet_owner][user_name]['weight'] += 1
            else:
                # new edge. add with weight=1
                self.graph.add_edge(tweet_owner, user_name, weight=1)

    def build(self, parameter="favourites"):
        for tweet_batch in self._yield_tweets():
            for tweet in tweet_batch:
                self._tweet_to_edges(tweet, parameter)


if __name__ == '__main__':
    builder = GraphBuilder()
    # builder.build(parameter="retweets")
    # builder.save_to_file("C:\\Users\\Scarf_000\\Desktop\\ZTIS\\favourites_graph.gexf")
