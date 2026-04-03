import requests
import json
from datetime import datetime, timedelta
import os
import ast
import re

quote_table = os.getenv("QUOTE_TABLE")
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
    
def person_data(team_id, person):
  pipeline = [
      {"$match": {"person": person}},
      {
          "$project": {
              "person": 1,
              "information": 1,
              "role": 1,
              "organization": 1,
              "other-information": 1,
              "isPerson": 1,
              "mentions": 1
          }
      }]
  data2 = dataRequestsGet(team_id, quote_table, pipeline, "aggregate")
  return data2


def author_other_title_finder(team_id):
    pipeline = [
    {
        "$match": {
            "person": {
                "$exists": True
            },
            "$expr": {
                "$gte": [
                    { "$size": { "$split": ["$person", " "] } },
                    3
                ]
            }
        }
    },
    {
        "$project": {
            "_id": 0,
            "person": "$person",
                "biography": 1,
                "isPerson": 1,
            }
        }
    ]
    results = dataRequestsGet(team_id, quote_table, pipeline, "aggregate")
    pipeline2 = [{
            "$match": {
                "author": {
                    "$exists": True
                },
        }},
        {
                '$project': {
                    '_id': 0,
                    'author': '$author',
                }
            },
            {
                '$sort': {
                    'author': -1
                }
            },
            {
                '$limit': 1000
            }]
    results_authors = dataRequestsGet(team_id, 'storyData', pipeline2, "aggregate")
    authors = [re.sub(r'[\s\u00A0]+', ' ', name).strip() for entry in results_authors for name in entry['author'].split(',') if name.strip()]
    # Remove duplicates and format the output
    unique_authors = list(set(authors))
    formatted_authors = ','.join(unique_authors).split(',')
    titles_and_parties = [
        'Assemblymember', 
        'Democrat', 
        'Republican', 
        'Pro Tem', 
        'Assemblywoman', 
        'Congressmember', 
        'Deputy Speaker', 
        'Speaker', 
        'Chair', 
        'Vice Chair',
        'Prof',
        'Professor',
        'Archbishop',
        'Getty Images',
        'Councilmember',
        'Minnesotan',
        'Treasurer', 
        'Republicans',
        'Democrats',
        'of',
        'Pharmacist',
        'Reader',
        'Researcher',
        'Academic',
        'Author',
        'Director',
        'Independent',
        "Biostatistician", "Astronomer", 
    "Protozoologist", "Biogeochemist", "Biophysicist", "Chef", "Oncologist", "Arachnologist", 
    "Egyptologist", "Nanoengineer", "Acoustician", "Reader", "Geroscientist", "Author", 
    "Writer", "Physicist", "Historian", "Artist", "Politician", "Scientist", "Engineer", 
    "Publisher", "University Affiliated", "Researcher", "Scholar", "Academician", "Specialist", 
    "Expert", "Researcher", "Doctor", "Professor", "Institute Affiliated", "Institute", 
    "Institute Member", "Institute Associate", "Institute Fellow", "Institute Researcher", 
    "Institute Scholar", "Institute Affiliate", "Institute Partner", "Institute Collaborator", 
    "Institute Contributor", "Institute Researcher"]
    for i in results:
        if 'isPerson' in i and i['isPerson'] == True:
            #print(i['person'])
            if i['person'] in formatted_authors:
                print(i['person'])
                print("CURRENT AUTHOR")
                ## if a person is a current author they need to be given an isPerson False flag
                ## and also overall isAuthor True on their person $set not just inside mentions 
                dataRequestsPUT(team_id, quote_table, {"person": i['person']},{"$set": {"isPerson": False, "isAuthor": True}})
            cleaned_name = re.sub(r'[\\*\\*\\(\\)\\#\\!\\&@]', '', i['person'])
            if cleaned_name != i['person']:
                print(f"Symbols found in '{i['person']}': {i['person']}")
                print("Flags: Remove symbols and process accordingly")
                dataRequestsPUT(team_id, quote_table, {"person": i['person']},{"$set": {"isPerson": False}})
            if '\n' in i['person']:
                print(f"line break found in '{i['person']}'")
                dataRequestsPUT(team_id, quote_table, {"person": i['person']},{"$set": {"isPerson": False}})
            for word in i['person'].split():
                if word in titles_and_parties:
                    ## WE LOOK UP THE PERSON WITHOUT THE TERM
                    print(i['person'].replace(word, '').strip())
                    data = person_data(team_id, i['person'].replace(word, '').strip())
                    if data:
                    # if data exists for stripped name, change the value with extra word to isPerson False
                        print(data)
                    if 'error' in data:
                        if 'Reader' in i['person']:
                            print("is Reader")
                            dataRequestsPUT(team_id, quote_table, {"person": i['person']},{"$set": {"isPerson": False}})
                        else:
                            print(f"REVIEW {i['person']}")
                    else: 
                        print("They have data")
                        dataRequestsPUT(team_id, quote_table, {"person": i['person']},{"$set": {"isPerson": False}})
                else:
                    print(f"REVIEW {i['person']}") 
    else:
        pass


def plural_people(team_id):
    pipeline = [
        {
            "$match": {
                'isPerson': True, 
                'person': {
                    '$regex': "[’']s", 
                    '$options': 'i'
                }
            }
        }
    ]
    plural_people = dataRequestsGet(team_id, quote_table, pipeline, "aggregate")
    for i in plural_people:
        name = i['person']
        if re.match(r"""^[A-Za-z\s\.\-]+['’"]s$""", name, re.IGNORECASE):
            # Remove the trailing apostrophe and 's'
            cleaned_name = re.sub(r'[’"]s$', '', name)
            #print(f"Cleaned: {cleaned_name}")
            dataRequestsPUT(team_id, quote_table, {"person": i['person']},{"$set": {"isPerson": False}})
            data = person_data(team_id, cleaned_name)
            if data:
                if 'error' in data:
                    print("error")
                    print(cleaned_name)
                else:
                    dataRequestsPUT(team_id, quote_table, {"person": i['person']},{"$set": {"isPerson": False}})
            else:
                print(f"REVIEW {i['person']}")
        else:
            print(f"Skipped: {name}")


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
    try:
        author_other_title_finder(team_id)
    except Exception as e:
        print(f"author_other_title_finder error: {e}")
    try:
        plural_people(team_id)
    except Exception as e:
        print(f"plural_people error: {e}")