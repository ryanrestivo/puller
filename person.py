import requests
import json
from datetime import datetime, timedelta
import os
import ast
import re


quote_dates = os.getenv('DATES_ENDPOINT')
quote_table = os.getenv("QUOTE_TABLE")
llm_service = os.getenv("LLM_SERVICE")
llm_key = os.getenv('LLM_HEADER')

service_api = os.getenv("BACKEND_API")
if not service_api:
    raise ValueError("service_api not found in .env.  Ensure it's set correctly.")

feed_str = os.getenv("MY_SECRET_JSON")  # Get the environment variable (as a string)
if feed_str:
    try:
        feed = json.loads(feed_str)  # Convert JSON string to dictionary
        validation = feed['validation']
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

llm_data_endpoint = os.getenv('SHOT_ENDPOINT')
llm_data_endpoint_secret = os.getenv('SHOT_ENDPOINT_SECRET')


def shot_taker(data):
    data['process'] = llm_data_endpoint_secret
    r = requests.post(url=llm_data_endpoint, headers={"Validation": llm_key, 'Content-Type': 'application/json'}, json=data)
    if r.status_code == 200:
        return_data = r.json()
        r.close()
    else:
        return_data = r.json()
        r.close()
    return return_data


def people_reader(person):
  comparison_readout = shot_taker({'training': f'You are evaluating a string to determine the likelihood of it being a person or not. You are receiving a string determined by an NLP that it might be a person and providing conclusion to it.',
                  'rule': f'All you need to do is return boolean True or False. If the text string is likely to be a person return True, if they are not return False. ONLY RETURN THE BOOLEAN TRUE or FALSE. ',
                  'text': f'Here is the text string: {person}'})
  try:
    llm_data = ast.literal_eval(comparison_readout['choices'][-1]['message']['content'])
  except Exception:
    start_index = comparison_readout['choices'][-1]['message']['content'].find('{')
    end_index = comparison_readout['choices'][-1]['message']['content'].rfind('}')
    if start_index != -1 and end_index != -1:
        json_string = comparison_readout['choices'][-1]['message']['content'][start_index:end_index + 1]
        try:
            llm_data = ast.literal_eval(json_string)
        except Exception:
            llm_data = {}
  return llm_data



if __name__ in "__main__":
    feed_string = os.getenv("NEWSROOM_VARIABLE") 
    if feed_string:
        try:
            endpoint_space = json.loads(feed_string)  # Convert JSON string to dictionary
        except json.JSONDecodeError as e:
            print("Error decoding JSON:", e)
    else:
        print("Environment variable NEWSROOM_VARIABLE is not set.")
    print(f"Running {endpoint_space['team_id']}")
    team_id = endpoint_space['team_id']
    pipeline = [{
        '$match': {
            'mentions.mention': {'$exists': True},
            'mentions.quotes': {'$exists': True},
            'isPerson': {'$exists': False},
        }
    },
    {
        '$sort': {
            'mentions.mention.length': -1
        }
    },
    {
        '$project': {
            '_id': 0,
            'person': '$person',
        }
    }]
    top_people = dataRequestsGet(team_id, quote_table, pipeline, "aggregate")
    print(top_people)
    person_list = [i['person'] for i in top_people] if 'error' not in top_people else []
    for person in person_list:
        bio_data = {}
        print(person)
        try:
            data = people_reader(person)
            if type(data) == bool:
                bio_data['isPerson'] = data
                dataRequestsPUT(team_id,quote_table, {'person': person}, { "$set": bio_data })
        except:
            pass