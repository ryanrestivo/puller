# catch up script for embeddings

import spacy
import requests
import json
from datetime import datetime, timedelta
import pandas as pd
import os
import ast
import re

quote_dates = os.getenv('DATES_ENDPOINT')
quote_table = os.getenv("QUOTE_TABLE")

service_api = os.getenv("BACKEND_API")
if not service_api:
    raise ValueError("service_api not found in .env.  Ensure it's set correctly.")

feed_str = os.getenv("MY_SECRET_JSON")  # Get the environment variable (as a string)
if feed_str:
    try:
        feed = json.loads(feed_str)  # Convert JSON string to dictionary
        token = feed['token']
        endpoint = feed['endpoint']
        link = feed['link']
        validation = feed['validation']
        #teamID = feed['teamID']
        database = feed['database']
    except json.JSONDecodeError as e:
        print("Error decoding JSON:", e)
else:
    print("Environment variable MY_SECRET_JSON is not set.")

def dataRequestsGet(database_name, collection_name, mongo_query, mongo_query_type, metric=None):
    mongo_query_str = json.dumps(mongo_query)
    z = requests.get(service_api,
                            headers={'Validation': validation, 'Content-Type': 'application/json', 'database-name': database_name, 'collection-name': collection_name, 'mongo-query': mongo_query_str, 'mongo-query-type': mongo_query_type, 'metric': metric})
    if z.status_code == 200:
        data = z.json()
        z.close()
        return data
    else:
        z.close()
        return 'Fail'

def inputDataRequests(database_name, collection_name, data):
    z = requests.post(service_api,
                            headers={'Validation': validation, 'Content-Type': 'application/json', 'database-name': database_name, 'collection-name': collection_name},json=data)
    if z.status_code == 200:
        data = z.json()
        z.close()
        return data
    else:
        z.close()
        return 'Fail'
    
def dataRequestsPUT(database_name, collection_name, mongo_query_str, update_task):
    z = requests.put(service_api,
                            headers={'Validation': validation, 'Content-Type': 'application/json', 'database-name': database_name, 'collection-name': collection_name}, json={'mongo-query': mongo_query_str, 'update-task': update_task})
    if z.status_code == 200:
        data = z.json()
        z.close()
        return data
    else:
        data = z.json()
        z.close()
        return data

nlp = spacy.load("en_core_web_md")

def create_embeddings(nlp, text):
    doc = nlp(text)
    return doc.vector 

def embeddings_for_items(text, site, person, team_id, quote=False):
  text_embeddings = create_embeddings(nlp, text)
  embeddings_data = [float(i) for i in text_embeddings]
  if quote == True:
    dataRequestsPUT(team_id, quote_table, {
        "person": person,
        "mentions": {
            "$elemMatch": {
                "site": site,
                "quotes": text
            }
        }
    },
    {
        "$set": {
            "mentions.$.quotesEmbeddings": embeddings_data
        }
    })
  if quote == False:
    dataRequestsPUT(team_id, quote_table, {
        "person": person,
        "mentions": {
            "$elemMatch": {
                "site": site,
                "mention": text
            }
        }
    },
    {
        "$set": {
            "mentions.$.mentionsEmbeddings": embeddings_data
        }
    })



def scaling_vector_data(team_id):
  pipeline = [
            {
                "$match": {
                    "date": {
                        "$exists": True
                    },
                    "quotevectors": {
                        "$exists": False
                    }
                }
            },
            {
                "$project": {
                    "date": 1,
                    "_id": 0
                }
            }
        ]
  data = dataRequestsGet(team_id, quote_dates, pipeline, "aggregate")
  dates_list = [i['date'] for i in data]
  dates_list_run = dates_list[:5] # test with 5 to confirm works
  for date_num in dates_list_run: 
    try:
      pipeline_two = [
                {
                    "$match": {
                        "mentions.publishDate": {
                            "$eq": date_num
                        }
                    }
                },
                {
                    "$project": {
                    "_id": 1,
                    "person": 1,
                    "mentions.mention": 1,
                    "mentions.site": 1,
                    "mentions.quotes": 1
                  }
                }
            ]
      data_two = dataRequestsGet(team_id, quote_table, pipeline_two, "aggregate")
      for i in data_two:
        person = i['person']
        for a in i['mentions']:
          embeddings_for_items(a['mention'], a['site'], person, team_id, False)
          if a['quotes'] == None:
            pass
          else:
            embeddings_for_items(a['quotes'], a['site'], person, team_id, True)
      dataRequestsPUT(team_id, quote_dates, {
          "date": date_num },{
          "$set": {
              "quotevectors": True
          }})
      print(f"{date_num} completed: {datetime.now()}")
    except Exception as e:
      print(f"Error {date_num}: {e}")



if __name__ in "__main__":
    feed_string = os.getenv("NEWSROOM_VARIABLE") 
    if feed_string:
        try:
            endpoint_space = json.loads(feed_string)  # Convert JSON string to dictionary
        except json.JSONDecodeError as e:
            print("Error decoding JSON:", e)
    else:
        print("Environment variable NEWSROOM_VARIABLE is not set.")
    print(f"Running for {endpoint_space['name']}")
    past_story_ids = scaling_vector_data(endpoint_space['team_id'])