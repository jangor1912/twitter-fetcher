import csv

from amazon.dynamo_db import DynamoDB


class TweetPerDate(object):
    def __init__(self):
        self.tweet_per_date = {}
        self.dynamo_db = DynamoDB()

    def save_to_csv(self, path):
        with open(path, 'a') as fd:
            writer = csv.writer(fd)
            for year, month_dict in self.tweet_per_date.items():
                for month, day_dict in month_dict.items():
                    for day, hour_dict in day_dict.items():
                        for hour, tweet_no in hour_dict.items():
                            key = year + "-" + month + "-" + day + "-" + hour
                            fields = [key, tweet_no]

                            if year == 2019 and month == 'May':
                                writer.writerow(fields)

    def save_to_csv_correct_order(self, path):
        year = '2019'
        month = 'May'
        days = [str(day_no) for day_no in range(1, 32)]
        hours = []
        for hour in range(0, 25):
            hour = str(hour)
            if len(hour) == 1:
                hour = '0' + hour
            hours.append(hour)
        day_of_the_week_list = ["Wed", "Thu", "Fri", "Sat", "Sun", "Mon", "Tue"]

        with open(path, 'a') as fd:
            writer = csv.writer(fd)
            for day in days:
                for hour in hours:
                    try:
                        for day_of_the_week, hours_dict in self.tweet_per_date[year][month][day].items():
                            tweet_no = hours_dict[hour]
                    except KeyError:
                        tweet_no = 0
                        day_of_the_week = day_of_the_week_list[(int(day)-1) % 7]
                    key = year + "-" + month + "-" + day + "-" + day_of_the_week + "-" + hour
                    fields = [key, tweet_no]
                    writer.writerow(fields)

    def create_histogram_dict(self, batches=None):
        for batch in self.dynamo_db.yield_tweets(batches=batches):
            for tweet in batch:
                self.add_tweet(tweet)

    def add_tweet(self, tweet):
        date_str = tweet["created_at"]
        date_array = date_str.split(" ")
        try:
            day_of_the_week = date_array[0]
            month = date_array[1]
            day = date_array[2]
            hour = date_array[3]
            year = date_array[5]

            # parse hour
            hour = hour.split(":")[0]

            self.add_date(year, month, day, day_of_the_week, hour)
        except IndexError:
            pass

    def add_date(self, year, month, day, day_of_the_week, hour):
        if year not in self.tweet_per_date:
            self.tweet_per_date[year] = {}
        if month not in self.tweet_per_date[year]:
            self.tweet_per_date[year][month] = {}
        if day not in self.tweet_per_date[year][month]:
            self.tweet_per_date[year][month][day] = {}
        if day_of_the_week not in self.tweet_per_date[year][month][day]:
            self.tweet_per_date[year][month][day][day_of_the_week] = {}
        if hour not in self.tweet_per_date[year][month][day][day_of_the_week]:
            self.tweet_per_date[year][month][day][day_of_the_week][hour] = 0

        self.tweet_per_date[year][month][day][day_of_the_week][hour] += 1


if __name__ == "__main__":
    tpd = TweetPerDate()
    tpd.create_histogram_dict(batches=10)
    tpd.save_to_csv_correct_order('C:\\Users\\Scarf_000\\PycharmProjects\\twitter-fetcher\\csv_files\\tweet_per_hour.csv')
