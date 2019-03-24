import pymongo


class MongoDB(object):
    def __init__(self):
        self.client = pymongo.MongoClient("mongodb://localhost:27017/")
        self.database = self.client["twitter_db"]
        self.users = self.database["users"]
