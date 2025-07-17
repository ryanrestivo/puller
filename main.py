import re
import json
import requests
import os
from datetime import datetime, timedelta

feed_str = os.getenv("MY_SECRET_JSON")  # Get the environment variable (as a string)
if feed_str:
    try:
        feed = json.loads(feed_str)  # Convert JSON string to dictionary
        token = feed['token']
        endpoint = feed['endpoint']
        link = feed['link']
        validation = feed['validation']
        teamID = feed['teamID']
        database = feed['database']
    except json.JSONDecodeError as e:
        print("Error decoding JSON:", e)
else:
    print("Environment variable MY_SECRET_JSON is not set.")



def dataRequestsGet(database_name, collection_name, mongo_query, mongo_query_type, metric=None):
    mongo_query_str = json.dumps(mongo_query)
    z = requests.get(f'{endpoint}',
                            headers={'Validation': validation, 'Content-Type': 'application/json', 'database_name': database_name, 'collection_name': collection_name, 'mongo_query': mongo_query_str, 'mongo_query_type': mongo_query_type, 'metric': metric})
    if z.status_code == 200:
        data = z.json()
        z.close()
        return data
    else:
        z.close()
        return 'Fail'

def inputDataRequests(database_name, collection_name, data):
    z = requests.post(f'{endpoint}',
                            headers={'Validation': validation, 'Content-Type': 'application/json', 'database_name': database_name, 'collection_name': collection_name},json=data)
    if z.status_code == 200:
        data = z.json()
        z.close()
        return data
    else:
        z.close()
        return 'Fail'

def data_process(data, end_story_id):
    fourteen_days_ago = datetime.now() - timedelta(days=14)
    for i in range(0,len(data['results'])):
      story = {}
      if len(data['results'][i]['bylines']) > 0:
        print(data['results'][i]['bylines'])
        if 'name' in data['results'][i]['bylines'][-1]:
          if ' / ' in data['results'][i]['bylines'][-1]['name']:
            pass
          elif 'Associated Press' in data['results'][i]['bylines'][-1]['name']:
            pass
          elif "Chicago Tribune" in data['results'][i]['bylines'][-1]['name']:
            pass
          elif "(TNS)" in data['results'][i]['bylines'][-1]['name']:
            pass
          elif "The New York Times Editorial Board" in data['results'][i]['bylines'][-1]['name']:
            pass
          else:
            story['author'] = data['results'][i]['bylines'][-1]['name']
            story['title'] = data['results'][i]['headline']
            story['site'] = data['results'][i]['share_url']
            story['publishDate'] = data['results'][i]['pub_date'].split('T')[0]
            story['story_id'] = data['results'][i]['id']
            if data['results'][i]['id'] <= end_story_id:
              print(f"Completes on ID: {data['results'][i]['id']}")
              return False
            story['description'] = data['results'][i]['tease']
            story['text'] = data['results'][i]['story'].strip()
            data_add = {}
            data_add['rows'] = [story]
            inputDataRequests(teamID, database, data_add)
            given_date = datetime.strptime(story['publishDate'], '%Y-%m-%d')
            if given_date < fourteen_days_ago:
                print(f"Completed on story from {story['publishDate']}")
                return False
    return True



def paginate_feed(initial_url):
    url = initial_url
    while url:
        headers = {'Authorization': token, 'Content-Type': 'application/json'}
        q = requests.get(url, headers=headers)
        q.raise_for_status()
        data = q.json()
        yield data
        url = data.get('next')


if __name__ in "__main__":
    pipeline = [ {"$sort": {"story_id": 1}}, {"$group": {"_id": None, "maxStoryId": {"$max": "$story_id"}}},{"$limit": 1}]
    data = dataRequestsGet(teamID, database, pipeline, "aggregate")
    end_story_id = data[-1]['maxStoryId']
    print(end_story_id)
    initial_url = link
    for page in paginate_feed(initial_url):
      if not data_process(page, end_story_id):
          break