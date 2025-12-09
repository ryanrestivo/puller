# listener for public radio RSS-based pages 
import requests
from bs4 import BeautifulSoup
import json
import os
from datetime import datetime

service_api = os.getenv("BACKEND_API")
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

def paragraph_text(html_text):
  soup = BeautifulSoup(html_text, "html.parser")
  paragraphs = soup.find_all('p')
  return ' '.join([p.text.strip() for p in paragraphs])

def story_checker(story_id, past_stories):
  if story_id in past_stories:
    raise Exception

def scrape_listing(url, previous_sites, end_text=None):
    resp = requests.get(url)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    stories = []
    # stories are in <li class="ListE-items-item">  → <ps-promo class="PromoA">
    items = soup.select("li.ListE-items-item ps-promo.PromoA")
    for item in items:
        title_tag = item.select_one(".PromoA-title a")
        title = title_tag.get_text(strip=True) if title_tag else None
        link = title_tag["href"] if title_tag else None
        try:
            story_checker(link, previous_sites)
            author_tag = item.select_one(".PromoA-authorName a")
            author = author_tag.get_text(strip=True) if author_tag else None
            date_tag = item.select_one(".PromoA-date .PromoA-timestamp")
            if date_tag and date_tag.get("data-timestamp"):
                ts = int(date_tag.get("data-timestamp")) / 1000
                date = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
            else:
                date = None
            story = requests.get(link)
            story_text = paragraph_text(story.text)
            if end_text:
                story_text = story_text.replace(end_text, '')
            stories.append({
                "title": title,
                "site": link,
                "author": author,
                "publishDate": date,
                "paragraphText": story_text
            })
        except: 
            pass
    return stories

def post_driver(team_id, url, previous_sites, end_text=None):
    story_data = []
    for i in range(1,100):
        URL = f'{url}{i}'
        try:
            data = scrape_listing(URL, previous_sites, end_text)
            story_data.extend(data)
        except:
            pass
    input_data = {}
    if len(story_data) == 0:
        print("No stories")
    else:
        input_data['rows'] = story_data
        inputDataRequests(team_id, "storyData", input_data)

def past_story_run(team_id):
    pipeline = [ {"$sort": {"site": -1}}, {'$project': {'site': 1}}]
    data_identifiers = dataRequestsGet(team_id, 'storyData', pipeline, "aggregate")
    past_story_ids = [i['site'] for i in data_identifiers]
    return past_story_ids


if __name__ in "__main__":
    feed_string = os.getenv("NEWSROOM_VARIABLE") 
    if feed_string:
        try:
            endpoint_space = json.loads(feed_string)  # Convert JSON string to dictionary
            #print([a for a in endpoint_space])
        except json.JSONDecodeError as e:
            print("Error decoding JSON:", e)
    else:
        print("Environment variable NEWSROOM_VARIABLE is not set.")
    print(f"Running for {endpoint_space['team_id']}")
    past_story_ids = past_story_run(endpoint_space['team_id'])
    print(f"{len(past_story_ids)} total stories")
    if 'end_text' in endpoint_space:
        end_text = endpoint_space['end_text']
    else:
        end_text = None
    try:
       post_driver(endpoint_space['team_id'], endpoint_space['website'], past_story_ids, end_text)
    except:
       pass
    # POST RUN STORY COUNT
    post_run_ids = past_story_run(endpoint_space['team_id'])
    print(f"NOW {len(post_run_ids)} total stories")