from database.mongo_db import MongoDB
from fetcher.fetcher import Fetcher

if __name__ == "__main__":
    fetcher = Fetcher()
    database = MongoDB()

    screen_name = "realDonaldTrump"
    print(screen_name)
    timeline = fetcher.get_tweets(screen_name=screen_name)

    for tweet in timeline:
        database.users.insert_one(dict(tweet._json))
