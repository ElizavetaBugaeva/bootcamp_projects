import pymongo
import sqlalchemy  # use a version prior to 2.0.0 or adjust creating the engine and df.to_sql()
import psycopg2
from configetl import tokens
import time
import logging
#import pandas as pd


# mongo db definitions
client = pymongo.MongoClient('my_mongo', port=27017)  # my_mongo is the hostname (= service in yml file)
db = client.reddit #change this to what your mongodb database is called
coll = db.posts #change this to whatever your collection in that db is called

# postgres db definitions
USERNAME_PG = tokens['USERNAME_PG']
PASSWORD_PG = tokens['PASSWORD_PG']
HOST_PG = tokens['HOST_PG']
PORT_PG = tokens['PORT_PG']
DATABASE_NAME_PG = 'reddits_pgdb'

conn_string_pg = f"postgresql://{USERNAME_PG}:{PASSWORD_PG}@{HOST_PG}:{PORT_PG}/{DATABASE_NAME_PG}"
time.sleep(5)  # safety margine to ensure running postgres server
pg = sqlalchemy.create_engine(conn_string_pg)


# Create the table
create_table_string = sqlalchemy.text("""CREATE TABLE IF NOT EXISTS reddits (
                                         subreddit_id NUMERIC, 
                                         time TEXT,
                                         time TEXT,
                                         subreddit TEXT, 
                                         title TEXT
                                         reddit TEXT,
                                         sentiment NUMERIC
                                         );

                                      """)
pg.execute(create_table_string)


def extract(limit=5):
    # We are loading only the last 5 entries for speed/debugging
    extracted_reddits = list(coll.find(limit = limit))
    logging.critical(f"\n---- {limit} reddits extracted ----\n")
    return extracted_reddits

def regex_clean(reddit):
    #placeholder function for removing things from your reddit text, e.g. links
    pass

def sentiment_analysis(text):
    #placeholder for real sentiment analysis function
    return 1

def transform(extracted_reddits):
    transformed_reddits = []
    for item in extracted_reddits:
        post=item['found_reddit']
        # optional just to see what is going on:
        logging.critical(f"{post}")

        #here we select the 'text' key from the dictoinary
        text = post['text']

        #... clean it using the regex cleaning function (which currently does nothing)
        text = regex_clean(text)

        #...perform sentiment analysis (currently returns 1 for all text - yours will be different)
        sentiment = sentiment_analysis(text)

        #... add a field to the post dictionary called "sentiment" that contains this value
        post['sentiment'] = sentiment

        # ... and finally append the post to our list of transformed reddits
        transformed_reddits.append(post)

        # can also optionally add a logging statement
        #(-think about if you want this inside or outside the for-loop)
        logging.critical("\n---- new reddit post successfully transformed ----\n")

    return transformed_reddits

def load(transformed_reddits):
    for post in transformed_reddits:
        insert_query = "INSERT INTO reddits (id, time, reddit, sentiment) VALUES (%s, %s, %s, %s) ON CONFLICT (id) DO UPDATE SET time = excluded.time, reddit = excluded.reddit, sentiment = excluded.sentiment;"
        pg.execute(insert_query, (post['_id'], post['time'], post['text'], post['sentiment'])) ,
        logging.critical(f"New reddit post incoming: {post['text']} with sentiment score {post['sentiment']}")
        logging.critical("\n---- new reddit post loaded to postgres db ----\n")
    return None 


if __name__ == '__main__': #checks if the script is being run as the main program, and not imported as a module into another program
    while True:
        extracted_reddits = extract()
        transformed_reddits = transform(extracted_reddits)
        load(transformed_reddits)
        time.sleep(120)
        
