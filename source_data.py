# use requirements.txt
import requests
from bs4    import BeautifulSoup
import json
from datetime import datetime, timedelta
import os
import ast
import re
import urllib.parse
import random



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
    

def get_source_list(team_id):
    pipeline = [
        {
            '$match': {
                'isPerson': {
                    '$eq': True
                },
                'organization': {
                    '$ne': None
                },
                'dead': {
                    '$eq': False
                },
                'search_data': {
                    '$exists': False
                }
            }
        }, {
            '$project': {
                'person': 1,
                '_id': 0,
                'organization': 1,
                'role': 1,
                'total_data': 1
            }
        },{
            '$sort': {
                'total_data': -1
            }
        }
    ]
    people_data = dataRequestsGet(team_id,quote_table, pipeline, "aggregate")
    return people_data

def get_link_data(link):
    try:
        response = requests.get(link, timeout=10)
        response.raise_for_status() 
        soup = BeautifulSoup(response.text, 'html.parser')
        paragraphs = soup.find_all('p')
        text_data = [p.get_text(strip=True) for p in paragraphs]
        return '\n'.join(text_data)
    except requests.exceptions.RequestException as e:
        raise Exception(f"Request error: {str(e)}")
    except Exception as e:
        raise Exception(f"Unexpected error: {str(e)}")



def search_endpoint(query, max_results=10):
    base_url = os.getenv("SEARCH_API")
    params = {
        "q": query
    }
    user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.6.1 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.2048.71",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36 Edg/118.0.2089.115",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36 Edg/117.0.2045.60",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36 Edg/116.0.1938.83",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 Edg/115.0.1907.170",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36 Edg/114.0.1823.83",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36 Edg/113.0.1900.123",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36 Edg/112.0.1722.96",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36 Edg/111.0.1661.62",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36 Edg/110.0.1587.69",]
    headers = {
    "User-Agent": random.choice(user_agents),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Connection": "keep-alive",
    }
    session = requests.Session()
    session.headers.update(headers)
    response = requests.get(base_url, data=params, headers=headers, timeout=10)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    results = []
    result_blocks = soup.find_all("div", class_="result")
    for block in result_blocks:
        title_tag = block.find("a", class_="result__a")
        snippet_tag = block.find("a", class_="result__snippet")
        if title_tag:
            link = title_tag["href"]
            title = title_tag.get_text(" ", strip=True)
            snippet = snippet_tag.get_text(" ", strip=True) if snippet_tag else ""
            results.append({
                "title": title,
                "link": link,
                "snippet": snippet
            })
        if len(results) >= max_results:
            break
    return results

def people_generation(team_id, people_data):
  for people in people_data:
    try:
        invalid_values = ['', 'unknown', 'N/A', 'none', 'not provided', '---']
        if type(people['organization']) is list:
            org = ' '.join(people['organization'])
        elif type(people['organization']) is dict:
            org = str(people['organization'])
        else:
            org = people['organization']
        if org.lower() not in [v.lower() for v in invalid_values]:
            search_string = f"{people['person']} {people['organization']}"
            results = search_endpoint(search_string)
            if len(results) == 0:
                print(people['person'])
            else:
                print(people['person'])
                print(len(results))
                for a in results:
                    parsed_url = urllib.parse.unquote(a['link']).split('uddg=')[-1].split('&rut=')[0]
                    a['link'] = parsed_url
                    try:
                        a['paragraphText'] = get_link_data(parsed_url)
                        print(f"pulled {parsed_url}")
                    except:
                        pass
                bio_data = {}
                bio_data['search_data'] = results
                try:
                    dataRequestsPUT(team_id,quote_table, {'person': people['person']}, { "$set": bio_data })
                except:
                    pass
    except Exception as e:
        print(f"Error processing {people['person']}: {str(e)}")


if __name__ in "__main__":
    feed_string = os.getenv("NEWSROOM_VARIABLE") 
    if feed_string:
        try:
            endpoint_space = json.loads(feed_string)  # Convert JSON string to dictionary
        except json.JSONDecodeError as e:
            print("Error decoding JSON:", e)
    else:
        print("Environment variable NEWSROOM_VARIABLE is not set.")
    print(f"{endpoint_space['team_id']}")
    team_id = endpoint_space['team_id']
    try:
        source_list = get_source_list(team_id)
        people_generation(team_id, source_list)
    except Exception as e:
        print(f"error: {e}")