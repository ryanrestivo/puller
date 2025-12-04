# WRITE BIOS FOR NEWSROOM SOURCES
import requests
import json
from datetime import datetime, timedelta
import os
import ast
import re
from wikilookup import searching_person

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
    
def flex_llm_point(data):
    r = requests.post(llm_service, headers={"Validation": llm_key, 'Content-Type': 'application/json'}, json=data)
    if r.status_code == 200:
        return_data = r.json()
        r.close()
    else:
        return_data = r.json()
        r.close()
    return return_data




def find_bio_less(team_id):
    pipeline = [{
        '$match': {
            'mentions.mention': {'$exists': True},
            'mentions.quotes': {'$exists': True},
            'biography': {'$exists': False},
            'mentions.speaker': {'$ne': None}, 
            'mentions.fullNameMentioned': {'$eq': True},
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
    update_people = [i['person'] for i in top_people]
    return update_people # the list of names


def find_update_people(team_id):
    pipeline = [{
        '$match': {
            #'mentions.mention': {'$exists': True},  # Ensure 'mention' field exists
            'mentions.quotes': {'$exists': True},  # Ensure 'quotes' field exists
            'update': {"$eq": True}
        }
    },
    {
        '$project': {
            '_id': 0,
            'person': '$person',
        }
    }]
    top_people = dataRequestsGet(team_id, quote_table, pipeline, "aggregate")
    return [i['person'] for i in top_people] if 'error' not in top_people else [] # list of people names



def bio_creator(team_id, person):
  ## GET THEIR BIO INFO FROM THEIR TEAM
  pipeline = [
        {"$match": {"person": person}},
        {"$unwind": "$mentions"},
        {"$match": {"mentions.publishDate": {"$exists": True}}},
        {
            "$match": {
                "mentions.mention": {"$ne": None},
                "mentions.quotes": {"$ne": None},
            }
        },
        {
            "$project": {
                "_id": 0,
                "mention": "$mentions.mention",
                "quote": "$mentions.quotes",
            }
        }]
  data = dataRequestsGet(team_id, quote_table, pipeline, "aggregate")
  try:
    item_text = ' '.join(list(set([i['mention'] for i in data] + [i['quote'] for i in data])))
    item_total = len([i['mention'] for i in data]) + len([i['quote'] for i in data])
  except:
    item_text = ' '.join(list(set([i['mention'] for i in data])))
    item_total = len([i['mention'] for i in data]) 
  ### RUN DATA THROUGH LLM ENDPOINT
  readout = flex_llm_point({'training': f'Base all of what you know about this source from the text. You must be certain when using this information. Create a python dict of items. Create a value "biography" as one long string that does not exceed 1500 characters, create a "role" and "organization" if applicable. Use all of the information given to write your best approximation on who {person} is from the quotes they have said. Be as specific on who they are from their quotes. Use from what they said and how they are mentioned to create a biography of them like this is a solid source to write the bio. DO NOT RETURN ANYTHING OTHER THAN THE DICT. If an item is repeated verbatim, assume that the text is duplicated.',
                                        'rule': f'Here is the mention of the text to use: ',
                                        'text': item_text})
  try:
    llm_data = ast.literal_eval(readout['choices'][-1]['message']['content'])
  except Exception:
    start_index = readout['choices'][-1]['message']['content'].find('{')
    end_index = readout['choices'][-1]['message']['content'].rfind('}')
    if start_index != -1 and end_index != -1:
        json_string = readout['choices'][-1]['message']['content'][start_index:end_index + 1]
        try:
            llm_data = ast.literal_eval(json_string)
        except Exception:
            llm_data = {}
  # CHECK KEYS WE NEED TO HAVE 
  keys_to_check = ['biography', 'role', 'organization']
  if all(key in llm_data for key in keys_to_check):
      print("All required keys are present.")
  else:
      raise Exception
  llm_data['model'] = readout['model']
  llm_data['updatedDate'] = datetime.now().strftime('%Y-%m-%d')
  llm_data['total_data'] = item_total
  return llm_data


def manual_information(team_id, person, biography):
  pipeline = [
    {"$match": {"person": person}},
    {
        "$project": {
            "person": 1,
            "information": 1,
            "role": 1,
            "organization": 1,
            "other-information": 1
        }
    }]
  data2 = dataRequestsGet(team_id, quote_table, pipeline, "aggregate")
  data2 = data2[-1]
  seen = set()
  unique_data = []
  for item in ['information', 'other-information','role','organization']:
      if item in data2:
          mention = f"{item.title()}: {''.join(data2[item])}\n" 
          if mention not in seen:
              seen.add(mention)
              unique_data.append(mention)
  other_text = ''.join(unique_data)
  if len(other_text) == 0:
    return None
  else:
    readout_two = flex_llm_point({'training': f'You are about to be given complimentary information about the source {person} gathered by the staff of the paper. Create a python dict of items. Create a value "biography" as one long string that does not exceed 2000 characters. Use all of the information given to write your best approximation on who {person} is. DO NOT RETURN ANYTHING OTHER THAN THE DICT. ',
                                      'rule': f'Here is the text from the staff of the paper.\n\n {other_text}. Use this data that is known about the source to rewrite this biography of the source and add more detail where necessary. Keep as much as possible, but add details where necessary. Here is the current biography to edit: ',
                                      'text': biography})
    try:
      llm_data = ast.literal_eval(readout_two['choices'][-1]['message']['content'])
    except Exception:
      start_index = readout_two['choices'][-1]['message']['content'].find('{')
      end_index = readout_two['choices'][-1]['message']['content'].rfind('}')
      if start_index != -1 and end_index != -1:
          json_string = readout_two['choices'][-1]['message']['content'][start_index:end_index + 1]
          try:
              llm_data = ast.literal_eval(json_string)
          except Exception:
              llm_data = {}
  keys_to_check = ['biography']
  if all(key in llm_data for key in keys_to_check):
      print("All required keys are present.")
      return llm_data['biography']
  else:
      print("Not all required keys are present.")
      return None
  


def people_run_through(team_id, people_list, limit=None):
    if limit:
        limit = limit
    else:
        limit = len(people_list)
    for person in people_list[:limit]:
        try:
            bio_data = bio_creator(team_id, person)
            alt_bio = manual_information(team_id, person, bio_data['biography'])
            if alt_bio:
                bio_data['biography'] = alt_bio # alt_bio becomes bio
            
            if bio_data['total_data'] > 10: # extreme threshold to start
                # if you have a lot, assume you're famous
                merged_bio = wiki_search(person, bio_data['biography'])
                if merged_bio:
                    bio_data['biography'] = merged_bio
            dataRequestsPUT(team_id,'quotesData', {'person': person}, { "$set": bio_data })
        except Exception as e:
            print(f"ERROR: {person}: {e}")
            pass
        
        
        


def wiki_search(person, biography):
    data = {}
    wiki_data = searching_person(person)
    logical_bio_compare = comparison(person, biography, wiki_data['exact_match']['extract'])
    if logical_bio_compare == True:
        # PUSH BIO DATA
        data['wikipedia'] = wiki_data
        data['updatedDate'] = datetime.now().strftime('%Y-%m-%d')
        dataRequestsPUT(team_id,'quotesData', {'person': person}, { "$set": data })
        updated_biography = merge_bio_create(person, biography, wiki_data['exact_match']['extract'])
        return updated_biography
    else:
        return None

def merge_bio_create(person, biography, merging_bio):
    readout_two = flex_llm_point({'training': f'Here is authoritative infomration about the source {person}. Create a python dict of items. Create a value "biography" as one long string that does not exceed 3000 characters. Use all of the information given to write your best approximation on who {person} is. DO NOT RETURN ANYTHING OTHER THAN THE DICT. ',
                                      'rule': f'Here is the biography formed by the data we have.\n\n {biography}. Use the data from the authoritative source to mix with the data we have to create a more robust biography, one that could exceed 3000 characters. Here is the new information to add to the biography: ',
                                      'text': merging_bio})
    try:
      llm_data = ast.literal_eval(readout_two['choices'][-1]['message']['content'])
    except Exception:
      start_index = readout_two['choices'][-1]['message']['content'].find('{')
      end_index = readout_two['choices'][-1]['message']['content'].rfind('}')
      if start_index != -1 and end_index != -1:
          json_string = readout_two['choices'][-1]['message']['content'][start_index:end_index + 1]
          try:
              llm_data = ast.literal_eval(json_string)
          except Exception:
              llm_data = {}
    keys_to_check = ['biography']
    if all(key in llm_data for key in keys_to_check):
        print("All required keys are present.")
        return llm_data['biography']
    else:
        print("Not all required keys are present.")
        return None
  

def comparison(person, text1, text2):
    comparison_readout = flex_llm_point({'training': f'You are evaluating two biographies for {person}. You are reading them to understand if they are similar in nature or if they diverge and are not talking about the same person.',
                                  'rule': f'All you need to do is return boolean True or False. If the two biographies are matching the same person, return True, if they are not return False. ONLY RETURN THE BOOLEAN TRUE or FALSE. Here is the text to evaluate: ',
                                  'text': f'Here are the two texts to evaluate\n\n TEXT1: {text1}\n\nTEXT2: {text2}'})
    return bool(comparison_readout['choices'][-1]['message']['content'])

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
    people_list = find_bio_less(team_id)
    people_list_for_updating = find_update_people(team_id)
    if len(people_list_for_updating) > 0:
        people_run_through(endpoint_space['team_id'], people_list_for_updating)
    ### start with 100 to test
    people_run_through(endpoint_space['team_id'], people_list, 100)