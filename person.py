import requests
import json
from datetime import datetime, timedelta
import os
import ast
import re


quote_table = os.getenv("QUOTE_TABLE")
llm_service = os.getenv("LLM_SERVICE")
llm_key = os.getenv('LLM_HEADER')

# Use /nlp service for NER — saves the puller from needing spacy installed
# This keeps the puller lightweight and delegates NLP to the dedicated service
nlp_base_url = os.getenv("NLP_BASE_URL")

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
    """Use /nlp service for NER person detection — no local spacy needed.
    
    OLD: loaded spacy.en_core_web_md locally and parsed doc.ents for PERSON type
    NEW: delegates to /nlp service's /personentities endpoint
    This saves the puller from needing spacy installed and keeps NLP in its dedicated service.
    
    Returns: {'isPerson': bool}
    """
    # Normalize the input name for comparison (strip commas, extra spaces)
    normalized = person.strip().replace(',', '')
    # Extract surname — last word of the input — works for 2, 3, 4+ word names
    input_surname = normalized.split()[-1].lower() if normalized.split() else ''

    try:
        # Use /nlp service /entities endpoint — returns entities with types
        url = f'{nlp_base_url}/entities'
        payload = {
            'text': person,
            'lang_model': 'en_core_web_md'
        }
        resp = requests.post(
            url,
            json=payload,
            headers={'Validation': llm_key, 'Content-Type': 'application/json'},
            timeout=5
        )
        if resp.status_code == 200:
            data = resp.json()
            ents = data.get('ents', [])
            
            # Check for a full match first — works when spacy extracts the exact name span
            # Check for surname match — works for 2, 3, 4+ word names regardless of prefix
            has_match = False
            for e in ents:
                for label, text in e.items():
                    if label == 'PERSON':
                        # Full match check
                        full_text = text.strip().replace(',', '')
                        if full_text == normalized:
                            has_match = True
                            break
                        # Surname match — compare only the last word of the detected entity
                        entity_surname = full_text.split()[-1].lower() if full_text.split() else ''
                        # Also check reverse: could the person's first names overlap with the entity's prefix?
                        # e.g., person="John Smith" but entity="John Doe Smith"
                        if entity_surname == input_surname:
                            has_match = True
                            break
            
            if has_match:
                return {'isPerson': True}
            else:
                # Fallback: heuristics on the name format when no NER match found
                words = person.split()
                has_multiple_parts = len(words) >= 2
                is_proper_case = all((w[0].isupper() or w.isnumeric()) for w in words if w)
                if is_proper_case and has_multiple_parts:
                    # Check if it's a single-word all-caps (likely ORG, not person)
                    # e.g., 'IBM', 'NASA', 'FBI'
                    if person.isupper():
                        return {'isPerson': False}
                    return {'isPerson': True}
                return {'isPerson': False}
        else:
            # Service unavailable — fall back to heuristics
            words = person.split()
            has_multiple_parts = len(words) >= 2
            is_proper_case = all((w[0].isupper() or w.isnumeric()) for w in words if w)
            if is_proper_case and has_multiple_parts and not person.isupper():
                return {'isPerson': True}
            return {'isPerson': False}
    except Exception as e:
        # If everything fails, use basic heuristics as last resort
        words = person.split()
        return {'isPerson': len(words) >= 2 and all((w[0].isupper()) for w in words if w)}




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