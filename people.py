import spacy
import requests
import json
from datetime import datetime, timedelta
import pandas as pd
import os
import ast
import re

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

nlp = spacy.load("en_core_web_md")

def entsTracking(doc):
    return [{ent.label_: ent.text} for ent in doc.ents]

def person_processor(data):
  people_list = []
  for i in range(0,len(data)):
    doc = nlp(data[i]['paragraphText'])
    ent_data = entsTracking(doc)
    people = [doc['PERSON'] for doc in ent_data if 'PERSON' in doc and len(doc['PERSON']) > 3 and len(doc['PERSON'].split()) > 1 and doc['PERSON'] is not data[i]['author']]
    people_list.extend(people)
  return people_list

def missingDates(teamID, table):
    # table: "completedDates"
    last_100_days = []
    current_date = datetime.now()
    for i in range(2, 10000):
        date = current_date - timedelta(days=i)
        last_100_days.append(date.strftime("%Y-%m-%d"))
    existing_dates_agg = [
    {
        "$group": {
            "_id": None,
            "dates": {"$push": "$date"}
        }
    },
    {
        "$project": {
            "dates": {"$setUnion": ["$dates", []]}
        }
    },
    {
        "$unwind": "$dates"
    },
    {
        "$group": {
            "_id": None,
            "dates": {"$push": "$dates"}
        }
    }]
    existing_dates = dataRequestsGet(teamID, table, existing_dates_agg, "aggregate")
    existing_dates_list = [doc['dates'] for doc in existing_dates][0]
    missing_dates = [date for date in last_100_days if date not in existing_dates_list]
    return missing_dates

def run_date(teamID, date_str):
    pipeline = [
        {
            "$match": {
                "publishDate": {
                    "$eq": date_str  # If publishDate is a Date type
                }
            }
        },
        {
            "$project": {
                "paragraphText": 1,
                "author": 1,
                "_id": 0
            }
        }
    ]
    data = dataRequestsGet(teamID, "storyData", pipeline, "aggregate")
    if 'error' in data:
        raise Exception
    people_list_full = person_processor(data)
    try:
      pipeline = [
      {
          "$match": {
              "person": {
                  "$in": people_list_full
              }
          }
      },
      {
          "$project": {
              "person": 1,
              "_id": 0
          }
      }]
      data_two = dataRequestsGet(teamID, "quotesData", pipeline, "aggregate")
      existing_names = [doc['person'] for doc in data_two]
      existing_set = set(existing_names)
      unique_new_people = [name for name in people_list_full if name not in existing_set]
      if len(unique_new_people) == 0:
        print("No people")
      else:
        people_data_listing = [{'person': i} for i in list(set(unique_new_people))]
        inputDataRequests(teamID, "quotesData", {"rows": people_data_listing})
      inputDataRequests(teamID, "completedDates", {"rows": [{"date": date_str}]})
      print(f"{date_str} completed")
    except Exception as e:
      print(date_str, e)



if __name__ == "__main__":
    feed_string = os.getenv("NEWSROOM_VARIABLE") 
    if feed_string:
        try:
            endpoint_space = json.loads(feed_string)  # Convert JSON string to dictionary
            print([a for a in endpoint_space])
        except json.JSONDecodeError as e:
            print("Error decoding JSON:", e)
    else:
        print("Environment variable NEWSROOM_VARIABLE is not set.")
    print(f"Running for {endpoint_space['name']}")
    missing_dates = missingDates(endpoint_space['team_id'], 'completedDates')
    for date_num in missing_dates:
        try:
            run_date(endpoint_space['team_id'], date_num)
        except Exception as e:
            print(f"ERROR: {date_num}, {e}")