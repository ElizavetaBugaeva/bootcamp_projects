import pymongo
import sqlalchemy  # use a version prior to 2.0.0 or adjust creating the engine and df.to_sql()
import psycopg2
import time
import logging
from config import tokens 
#import pandas as pd


# mongo db definitions
client = pymongo.MongoClient('mongodb', port=27017)  # my_mongo is the hostname (= service in yml file)
db = client.reddit #change this to what your mongodb database is called
coll = db.posts #change this to whatever your collection in that db is called

# postgres db definitions
HOST_PG = 'postgresdb'  # my_postgres is the hostname (= service in yml file)
PORT_PG = 5432
DATABASE_NAME_PG = 'reddits_pgdb'
USERNAME_PG = tokens['username']
PASSWORD_PG = tokens['password']


conn_string_pg = f"postgresql://{USERNAME_PG}:{PASSWORD_PG}@{HOST_PG}:{PORT_PG}/{DATABASE_NAME_PG}"
time.sleep(10)  # safety margine to ensure running postgres server
pg = sqlalchemy.create_engine(conn_string_pg, echo=True)
#connecting client to pg database
pg_client_connect = pg.connect()

# Create the table
create_table_string = sqlalchemy.text("""CREATE TABLE IF NOT EXISTS reddits (
                                         _id TEXT, 
                                         sub_id TEXT,
                                         date TEXT,
                                         text TEXT,
                                         sentiment NUMERIC
                                         );

                                      """)
#trying clientconnect instead of client
pg_client_connect.execute(create_table_string)
#trying commit 
pg_client_connect.commit()



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
        post=item
        # optional just to see what is going on:
        #logging.critical(f"{post}")

        #here we select the 'text' key from the dictoinary
        text = post['text']

        #clean it using the regex cleaning function (which currently does nothing)
        text = regex_clean(text)

        #perform sentiment analysis (currently returns 1 for all text - yours will be different)
        sentiment = 1

        #add a field to the post dictionary called "sentiment" that contains this value
        post['sentiment'] = sentiment

        #append the post to our list of transformed reddits
        transformed_reddits.append(post)

        # can also optionally add a logging statement
        #(-think about if you want this inside or outside the for-loop)
        logging.critical("\n---- new reddit post successfully transformed ----\n")

    return transformed_reddits

def load(transformed_reddits):
    #pg_connect = pg.connect()
    for post in transformed_reddits:
        insert_query = sqlalchemy.text(
            """
            INSERT INTO reddits (_id, sub_id, date, text, sentiment) 
            VALUES (:_id, :sub_id, :date, :text, :sentiment) ON CONFLICT DO NOTHING;
            """
        )
        pg_client_connect.execute(
            insert_query,
            {
                "_id": post["_id"],
                "sub_id": post["sub_id"],
                "date": post["date"],
                "text": post["text"],
                "sentiment": post["sentiment"],
            },
        )
        pg_client_connect.commit()

if __name__ == '__main__': #checks if the script is being run as the main program, and not imported as a module into another program
    while True:
        extracted_reddits = extract()
        transformed_reddits = transform(extracted_reddits)
        load(transformed_reddits)
        time.sleep(150)
        