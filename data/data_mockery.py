#!/usr/bin/env python
"""Creates mock data for Oracle Spark-Streaming-Data-Analysis

This script generates data for consumption by spark.

Tables:
    -- tweets, 
    root
    |-- id: long (nullable = true)
    |-- text: string (nullable = true)          tweet
    |-- favorite_count: long (nullable = true)  usually 0, occasionally 1-200
    |-- retweet_count: long (nullable = true)   usually 0, occasionally 1-200
    |-- quote_count: long (nullable = true)     usually 0, occasionally 1-200
    |-- reply_count: long (nullable = true)     usually 0, occasionally 1-200
    |-- lang: string (nullable = true)          
        ['en', 'ro', 'sl', 'und', 'lv', 'pl', 'pt', 'tl', 'in', 'ko', 'cs', 'tr', 'de', 'is', 'es', 'eu', 'it', 'sv', 'nl', 'ru', 'th', 'lt', 'ht', 'no', 'cy', 'hi', 'et', 'zh', 'fr', 'ja', 'da', 'fi']
    |-- user_id: long (nullable = true)         e.g. 221153076
    |-- datetime: string (nullable = true)      YYYY-MM-DD, e.g. 2017-03-23	
    |-- state: string (nullable = true)         us state, e.g. "Virginia"
    |-- provider: string (nullable = true)      ['A', 'B', 'C']
"""
from __future__ import print_function 

from itertools import cycle
from random import randrange
from random import seed
from random import randint
from random import uniform
from csv import writer
from csv import QUOTE_ALL

from datetime import date
from datetime import timedelta
from calendar import monthrange

from faker import Faker 


HEADER = ['id','text', 'favorite_count', 'retweet_count', 
    'quote_count', 'reply_count', 'lang', 'user_id', 'datetime', 'state', 'provider' 
]
        
def generator_factory(iterable_list):
    """Returns a generator that repeats the given list
    
    Args:
        iterable_list (any[]): list to iterate over

    Returns: 
        A generator that repeats the list passed in
    """
    infinitely_iterable = cycle(iterable_list)
    def infinite_generator():
        ret = next(infinitely_iterable)
        return ret
    return infinite_generator

def to_csv(row_as_list, header=[""], filename="data-{}.csv".format(date.today())):
    """Writes an iterable of lists as rows to csv file
    
    Args:
        row_as_list (string[] | int[]): iterable of lists 
    """
    with open(filename, "wb") as filehandle:
        csv_writer = writer(filehandle, quoting=QUOTE_ALL)
        csv_writer.writerow(header)
        for row in row_as_list:
            csv_writer.writerow(row)

def count_generator():
    not_zero = randrange(0,100) <= 3
    return randrange(1,200) if not_zero else 0

class TextGenerator:
    """Generator Factory for text
    manipulates text to be positive 
    """
    def __init__(self, fake, date_generator):
        self.fake = fake
        self.date_gen = date_generator
        # dict maps month (int) to % positive tweets
        self.positivity_map = {
            1: .5, 
            2: .5, 
            3: .5, 
            4: .7, 
            5: .85, 
            6: .75, 
            7: .7, 
            8: .65, 
            9: .6, 
            10: .55, 
            11: .5, 
            12: .5 
        }
        positive_text = ["good", "happy", "great", "best", "a+", "accolades", "adorable", "advantage", "avocados are delicious"]
        positivity_generator = generator_factory(positive_text)
        self.positivity_generator = lambda: " ".join([positivity_generator() for x in range(3)])
    
    def positive_words(self):
        for_month = self.date_gen().month
        positive_tweet = randrange(0, 100) <= self.positivity_map[for_month] * 100
        if positive_tweet:
            ret = self.positivity_generator()
            return ret
        return ""

    def text_generator(self):
        text = self.fake.text(max_nb_chars=100) + self.positive_words()
        return text

    def get_text_generator(self):
        return self.text_generator


class Date_Generator:
    """A date generator Factory 
    """
    def __init__(self, start_date, records_per_increment):
        """Returns a Date Generator Factory
        
        Args:
            start_date (date): start date as valid python date, e.g. `date(2018,01,06)`
            records_per_increment (int): # records to return before incrementing date
        """
        self.start_date = start_date
        self.records_per_increment = records_per_increment
        self.indx = 0
        self.current_date = start_date

    def date_generator(self):
        if 'function' in str(type(self.records_per_increment)):
            increment_bar = self.records_per_increment(self.current_date)
        else:
            increment_bar = self.records_per_increment
        num_days = int(self.indx / increment_bar)
        # print("i: {}, num_days: {}".format(self.indx, num_days))
        current_date = self.start_date + timedelta(days=num_days)
        if current_date != self.current_date:
            self.start_date = current_date
            self.current_date = current_date
            self.indx = 0
        self.indx += 1
        return self.current_date

    def get_date_generator(self):
        return self.date_generator
    

def get_rows(NUMBER):
    fake = Faker()

    TWEETS_PER_DAY = {
        1: 140, 
        2: 150, 
        3: 160, 
        4: 200, 
        5: 215, 
        6: 210, 
        7: 215, 
        8: 200, 
        9: 180, 
        10: 160, 
        11: 150, 
        12: 140 
    }
    START_DATE = date(2017, 01, 01)
    NUMBER = NUMBER if "-n" in argv else calculate_total_records(TWEETS_PER_DAY)
    languages=['en', 'und', 'lv', 'pn', 'en', 'tl', 'en',
     'en', 'en', 'tr', 'en', 'is', 'es', 'en', 'it', 'sv', 'en', 'ru', 
     'en', 'en', 'ht', 'no', 'en', 'hi', 'en', 'zh', 'fr', 'en', 'da', 'fi'
    ]
    states=['Utah', 'Hawaii', 'Minnesota', 'Ohio', 'Arkansas', 'Oregon', 'Texas', 
        'North Dakota', 'Pennsylvania', 'Connecticut', 'Nebraska', 'Vermont', 'Nevada', 
        'Washington', 'Illinois', 'Oklahoma', 'District of Columbia', 'Delaware', 
        'Alaska', 'New Mexico', 'West Virginia', 'Missouri', 'Rhode Island', 'Georgia', 
        'Montana', 'Michigan', 'Virginia', 'North Carolina', 'Wyoming', 'Kansas', 
        'New Jersey', 'Maryland', 'Alabama', 'Arizona', 'Iowa', 'Massachusetts', 
        'Kentucky', 'Louisiana', 'Mississippi', 'New Hampshire', 'Tennessee', 
        'Florida', 'Indiana', 'Idaho', 'South Carolina', 'South Dakota', 'California', 
        'New York', 'Wisconsin', 'Colorado', 'Maine'
    ]

    def daily_tweets_by_month(tweet_day):
        mo = int(tweet_day.month)
        return TWEETS_PER_DAY[mo]

    dategen1 = Date_Generator(START_DATE, daily_tweets_by_month)
    date_gen_for_tweets = dategen1.get_date_generator()

    id_gen = lambda: randrange(968323611915358210, 998323611915358210)
    text_gen = TextGenerator(fake, date_gen_for_tweets).get_text_generator()
    # use count_generator for fav, retweet, quote, reply counts
    lang_gen = generator_factory(languages)
    usrid_gen = lambda: randrange(35293, 968318781410938881)
    date_gen = Date_Generator(START_DATE, daily_tweets_by_month).get_date_generator()
    state_gen = generator_factory(states)
    provider_gen = generator_factory(['A', 'B', 'C'])

    rows = []
    
    for i in range(NUMBER):
        new_row = [
            id_gen(),
            text_gen().replace("\n"," ").replace(",", ""),
            count_generator(), # favs
            count_generator(), # retweets
            count_generator(), # quotes
            count_generator(), # replys
            lang_gen(),
            usrid_gen(),
            date_gen(),
            state_gen(),
            provider_gen()
        ]
        rows.append(new_row)
    return rows

def calculate_total_records(tweets_dict):
    total = 0
    for key in tweets_dict:
        days_in_month = monthrange(2017, key)[1]
        tweets_per_day = tweets_dict[key]
        total += days_in_month * tweets_per_day
    return total

def test(num):
    rows = get_rows(num)
    for row in rows:
        print(row)

def main(num):
    rows = get_rows(num)
    to_csv(rows, header=HEADER)


if __name__ == "__main__":
    from sys import argv

    # Global Variables
    DEBUG = False
    NUMBER = 101

    try:
        DEBUG = True if "-t" in argv else False

        if "-n" in argv:
            n_indx = argv.index("-n") + 1
            NUMBER = int(argv[n_indx])
        
        if DEBUG:
            test(NUMBER)
            exit(0)
        main(NUMBER)
    except Exception as e:
        from sys import exc_info
        print("\n {} \n {} \n".format(e, exc_info()))
        print("""

        USAGE python data_mockery.py [-t] [-n <#>]
        Run to generate data with optional flags:
        -t      to run tests
        -n #    to produce # records

        Run without arguments to generate data
        or add  to run tests

        """)