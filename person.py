import requests
import json
from datetime import datetime, timedelta
import os
import ast
import re
import spacy


quote_table = os.getenv("QUOTE_TABLE")
llm_service = os.getenv("LLM_SERVICE")
llm_key = os.getenv('LLM_HEADER')

# Load spaCy for NER — no LLM needed for person detection
nlp = spacy.load("en_core_web_md")

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
    """Determine if 'person' is a real person's name using spaCy NER — no LLM needed.
    
    OLD: sent text to LLM to ask 'is this a person?' (wasted LLM token)
    NEW: uses spaCy to count PERSON entities in the name itself
    
    If the input is just a name like 'John Smith', spaCy should tag it as PERSON.
    If it's a thing like 'World Health Organization', it gets ORG, not PERSON.
    """
    doc = nlp(person)
    person_ents = [ent for ent in doc.ents if ent.label_ == 'PERSON']
    # If there are PERSON entities in the name itself, it's likely a real person
    has_person = len(person_ents) > 0
    
    # Also check: if the string is all capitalized words or proper case, 
    # and has at least 2 words, it's more likely a person name
    words = person.split()
    has_multiple_parts = len(words) >= 2
    is_proper_case = all((w[0].isupper() or w.isnumeric()) for w in words if w)
    
    if has_person and has_multiple_parts:
        return {'isPerson': True}
    elif has_person and len(person.split()[0]) > 3:
        # Single name > 3 chars with PERSON tag is probably a person
        return {'isPerson': True}
    else:
        return {'isPerson': False}



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
            if data.get('isPerson', False):
                bio_data['isPerson'] = data['isPerson']
                dataRequestsPUT(team_id,quote_table, {'person': person}, { "$set": bio_data })
        except:
            pass