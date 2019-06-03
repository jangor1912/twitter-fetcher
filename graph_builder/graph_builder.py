import boto3
import networkx as nx
from boto3.dynamodb.conditions import Key, Attr

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
            region_name='eu-west-1'
        )

        # self.dynamodb = boto3.resource('dynamodb',
        #                                aws_session_token=aws_session_token,
        #                                aws_access_key_id=aws_access_key_id,
        #                                aws_secret_access_key=aws_secret_access_key,
        #                                region_name=region)

        self.tweet_table = self.dynamodb.Table('TweetSecond')
        self.already_created_tweet_ids = list()

        self.graph = nx.DiGraph()
        self.fetcher = Fetcher()

    def _yield_tweets(self, batches=None):
        response = self.tweet_table.scan(FilterExpression=Attr('retweet_count').gt(10000))
        data = response['Items']
        first = True
        batch_no = 1
        while 'LastEvaluatedKey' in response:
            response = self.tweet_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'],
                                             FilterExpression=Attr('retweet_count').gt(10000))
            batch_no += 1

            if batches and batch_no >= batches:
                raise StopIteration

            if first:
                data.extend(response['Items'])
                first = False
                yield data
            else:
                yield response["Items"]

    def _get_popular_tweets(self):
        main_dict_length = 500
        tmp_dict_length = 1000
        tweets_by_retweet_no = dict()
        temporary_dict = dict()
        for batch in self._yield_tweets(batches=100):
            for tweet in batch:
                if self.check_if_tweet_has_correct_date(tweet):
                    yield tweet

    def check_if_tweet_has_correct_date(self, tweet):
        date_str = tweet["created_at"]
        date_array = date_str.split(" ")
        try:
            day_of_the_week = date_array[0]
            month = date_array[1]
            day = date_array[2]
            hour = date_array[3]
            year = date_array[5]

            if year == '2019' and month == 'May':
                return True
            print("Returned false tweet from: {}".format(date_str))
            return False
        except IndexError:
            print("IndexError!!")
            return False

    def save_to_file(self, output_file_path):
        nx.write_gexf(self.graph, output_file_path)

    def _tweet_to_edges(self, tweet, parameter='favourites'):
        original_tweet_owner = tweet["retweeted_status"]["user"]["screen_name"] +\
                               tweet["retweeted_status"]["user"]["id_str"]
        original_tweet_id = tweet["retweeted_status"]["id_str"]
        if original_tweet_id in self.already_created_tweet_ids:
            print("Tweet with id = {}, of user = {} already appended".format(original_tweet_id, original_tweet_owner))
            return

        if parameter == 'favourites':
            user_list = self.fetcher.get_users_that_like_tweet(original_tweet_id)
        elif parameter == 'retweets':
            user_list = self.fetcher.get_users_that_retweet_tweet(original_tweet_id)
        else:
            raise RuntimeError("Incorrect parameter. Parameter should be one of {}"
                               .format(str(["favourites", "retweets"])))

        self.already_created_tweet_ids.append(original_tweet_id)
        for user_name in user_list:
            if self.graph.has_edge(original_tweet_owner, user_name):
                # we added this one before, just increase the weight by one
                self.graph[original_tweet_owner][user_name]['weight'] += 1
            else:
                # new edge. add with weight=1
                self.graph.add_edge(original_tweet_owner, user_name, weight=1)

    def build(self, parameter="favourites"):
        # for tweet_batch in self._yield_tweets():
        #     for tweet in tweet_batch:
        #         self._tweet_to_edges(tweet, parameter)
        twitter_calls = 0
        for tweet in self._get_popular_tweets():
            try:
                self._tweet_to_edges(tweet, parameter)
                twitter_calls += 1
            except Exception as e:
                try:
                    print(str(e))
                    code = e.args[0][0]["code"]
                    if code == 88:
                        print("Twitter was called {} times".format(twitter_calls))
                        return
                    pass
                except (IndexError, TypeError):
                    pass


if __name__ == '__main__':
    builder = GraphBuilder()
    builder.build(parameter="retweets")
    builder.save_to_file("C:\\Users\\Scarf_000\\Desktop\\ZTIS\\favourites_graph.gexf")
