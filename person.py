import requests
import json
from datetime import datetime, timedelta
import os
import ast
import re


quote_table = os.getenv("QUOTE_TABLE")
llm_service = os.getenv("LLM_SERVICE")
llm_key = os.getenv('LLM_HEADER')

# Delegate NER to the /nlp service — no local spacy needed.
# The puller fetches data; the /nlp service handles NLP. This saves ~200MB
# of model data and keeps the puller container lightweight.
nlp_base_url = os.getenv("NLP_BASE_URL")

service_api = os.getenv("BACKEND_API")
if not service_api:
    raise ValueError("service_api not found in .env. Ensure it's set correctly.")

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
    """Determine if a string is likely a person name by delegating NER to the /nlp service.

    OLD behavior:
        Loaded spacy.en_core_web_md locally and parsed doc.ents for PERSON type.
        Used exact string matching against entity spans — only worked for 2-word names.

    NEW behavior:
        POSTs to /nlp base URL /personentities endpoint for NER person detection.
        Uses last-word (surname) matching so 2, 3, 4+ word names are handled correctly.

    Returns:
        {'isPerson': bool}
    """
    # Normalize the input name for comparison (strip commas, extra spaces)
    normalized = person.strip().replace(',', '')
    # Extract surname — last word of the input — works for 2, 3, 4+ word names
    input_surname = normalized.split()[-1].lower() if normalized.split() else ''

    try:
        # POST to /nlp service /personentities endpoint — returns only PERSON entities
        url = f'{nlp_base_url}/personentities'
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
            # Expected format from /personentities: a list of strings (person names)
            person_ents = data.get('person_ents', [])
            if isinstance(data, dict):
                person_ents = data.get('person_ents', [])

            # Strategy 1 — exact full match (works when /nlp returns the full name)
            for name in person_ents:
                clean_name = name.strip().replace(',', '').replace('\\n', ' ')
                if clean_name.lower() == normalized.lower():
                    return {'isPerson': True}

            # Strategy 2 — surname (last word) matching
            # Handles cases like input="John Smith" matching entity="John Doe Smith"
            # or entity="James Robert Smith" — as long as the last word matches
            for name in person_ents:
                clean_name = name.strip().replace(',', '').replace('\\n', ' ')
                entity_surname = clean_name.split()[-1].lower() if clean_name.split() else ''
                if entity_surname == input_surname and input_surname:
                    return {'isPerson': True}

            # Strategy 3 — reverse surname check
            # Handles cases where the input surname is contained within a named entity
            for name in person_ents:
                clean_name = name.strip().replace(',', '').replace('\\n', ' ')
                if input_surname in clean_name.lower() and len(input_surname) > 2:
                    return {'isPerson': True}

            # No match from NLP — fall through to heuristic fallback
            words = person.split()
            has_multiple_parts = len(words) >= 2
            is_proper_case = all((w[0].isupper() or w.isnumeric()) for w in words if w)
            if is_proper_case and has_multiple_parts:
                # Single-word all-caps (likely ORG, not person) — e.g., 'IBM', 'NASA'
                if person.isupper():
                    return {'isPerson': False}
                return {'isPerson': True}
            return {'isPerson': False}
        else:
            # Service returned unexpected status — fall back to heuristics
            words = person.split()
            has_multiple_parts = len(words) >= 2
            is_proper_case = all((w[0].isupper() or w.isnumeric()) for w in words if w)
            if is_proper_case and has_multiple_parts and not person.isupper():
                return {'isPerson': True}
            return {'isPerson': False}
    except Exception as e:
        # If the /nlp service call fails entirely, use basic heuristics as last resort
        words = person.split()
        has_multiple_parts = len(words) >= 2
        is_proper_case = all((w[0].isupper()) for w in words if w)
        if is_proper_case and has_multiple_parts and not person.isupper():
            return {'isPerson': True}
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
