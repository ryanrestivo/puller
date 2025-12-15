import requests
from bs4 import BeautifulSoup
import json
import os

service_api = os.getenv("BACKEND_API")

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



def paragraph_text(html_text):
  soup = BeautifulSoup(html_text, "html.parser")
  paragraphs = soup.find_all('p')
  return ' '.join([p.text.strip() for p in paragraphs])

def story_checker(story_id, past_stories):
  if story_id in past_stories:
    raise Exception

def links_author_get(link):
    z = requests.get(link)
    story_feed = z.json()
    if 'author' in story_feed['_links']:
        if 'href' in story_feed['_links']['author'][-1]:
            q = requests.get(story_feed['_links']['author'][-1]['href'])
            feed = q.json()
            if 'name' in feed:
                return feed['name']
            else:
                return None
    else:
      return None

### THE CUSTOM SCRIPT TO BRING IN STORIES

def post_driver(feed, past_stories):
  end_sequence = 0
  total_pages = 10000 // 100 + 1
  name = feed['name']
  endpoint_site = feed['website']
  team_id = feed['team_id']
  for page in range(1,total_pages):
    r = requests.get(f'{endpoint_site}/wp-json/wp/v2/posts?per_page=100&page={page}') # paginate the feed
    data = r.json()
    data_list = []
    for i in range(0,len(data)):
      try:
        story_checker(data[i]['id'], past_stories)
        data_dict = {}
        data_dict['title'] = data[i]['title']['rendered']
        data_dict['site'] = data[i]['link']
        text = paragraph_text(data[i]['content']['rendered'])
        data_dict['paragraphText'] = text
        data_dict['publishDate'] = data[i]['date_gmt'].split('T')[0]
        data_dict['story_id'] = data[i]['id']
        if 'yoast_head_json' in data[i]:
           data_dict['author'] = data[i]['yoast_head_json']['author']
        elif 'coauthors' in data[i]:
           # others use coauthors
           data_dict['author'] = data[i]['coauthors'][-1]['display_name']
        elif '_links' in data[i]:
           # some have neither but _links is useful resource to find the author name
           if 'self' in data[i]['_links']:
              data_dict['author']  = links_author_get(data[i]['_links'][-1]['href'])
        else:
           data_dict['author'] = None
        try:
          data_dict['schemaData'] = data[i]['yoast_head_json']['schema']
        except:
          pass
        try:
          for a in data[i]['yoast_head_json']['schema']['@graph']:
            if a['@type'] == 'NewsArticle':
              data_dict['keywords'] = a['keywords']
              data_dict['articleSection'] = a['articleSection']
            elif a['@type'] == 'NewsArticle':
              data_dict['keywords'] = a['keywords']
              data_dict['articleSection'] = a['articleSection']
        except:
          pass
        data_list.append(data_dict)
      except Exception:
        pass
    input_data = {}
    if len(data_list) == 0:
      end_sequence = end_sequence + 1
      if end_sequence == 5:
         print(f"end_sequence equals {end_sequence}")
         # we havent found any new stories
         break
    else:
      input_data['rows'] = data_list
      inputDataRequests(team_id, "storyData", input_data)


def past_story_run(team_id):
    pipeline = [ {"$sort": {"story_id": -1}}, {'$project': {'story_id': 1}}]
    data_identifiers = dataRequestsGet(team_id, 'storyData', pipeline, "aggregate")
    past_story_ids = [i['story_id'] for i in data_identifiers]
    return past_story_ids


if __name__ in "__main__":
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
    past_story_ids = past_story_run(endpoint_space['team_id'])
    print(f"{len(past_story_ids)} total stories")
    try:
       post_driver(endpoint_space, past_story_ids)
    except:
       pass
    # POST RUN STORY COUNT
    post_run_ids = past_story_run(endpoint_space['team_id'])
    print(f"NOW {len(post_run_ids)} total stories")






