import requests
from bs4 import BeautifulSoup
import os
import json


feed_str = os.getenv("WK_DATA")  # Get the environment variable (as a string)
if feed_str:
    try:
        # Convert JSON string to dictionary
        feed = json.loads(feed_str)  
        WIKIPEDIA_API_URL = feed['site']
        headers = {feed['name']: feed['description']}

    except json.JSONDecodeError as e:
        print("Error decoding JSON:", e)
else:
    print("Environment variable WK_DATA is not set.")

def search_wikipedia(query):
    params = {
        'action': 'query',
        'list': 'search',
        'srsearch': query,
        'format': 'json'
    }
    response = requests.get(WIKIPEDIA_API_URL, params=params, headers=headers).json()
    return response.get('query', {}).get('search', [])

def get_combined_page_info(pageid):
    params = {
        'action': 'query',
        'pageids': pageid,
        'prop': 'extracts|revisions|references|pageprops|links',
        'rvprop': 'content',  # Full content
        'format': 'json'
    }
    response = requests.get(WIKIPEDIA_API_URL, params=params, headers=headers).json()
    pages = response.get('query', {}).get('pages', {})
    if pages:
        page = next(iter(pages.values()))
        return {
            'title': page.get('title'),
            'pageid': page.get('pageid'),
            'extract': page.get('extract', ''),
            'references': page.get('references', []),
            'pageprops': page.get('pageprops', {}),
            'links': page.get('links', [])
        }
    return {}

def clean_snippets(snippet):
  soup = BeautifulSoup(snippet, 'html.parser')
  for span in soup.find_all('span', class_='searchmatch'):
      span.replace_with('*' + span.text + '*')
  modified_snippet = soup.get_text()
  return modified_snippet

def exact_match_data(page_id):
  data2 = get_combined_page_info(page_id)
  html_content = data2['extract']
  soup = BeautifulSoup(html_content, 'html.parser')
  paragraphs = soup.find_all('p')
  text_data = []
  for p in paragraphs:
      text = p.get_text(strip=True)
      text_data.append(text)
  data2['extract'] = ' '.join(text_data).strip()
  return data2 #data_dict


def searching_person(search_term):
  #print(search_term)
  search_results = search_wikipedia(search_term)
  data = {}
  other_matches = []
  #print(search_results)
  for i in range(0,len(search_results)):
    if ' may refer to: ' in search_results[i]['snippet']:
      # inexact match
      break
    if search_results[i]['title'] == search_term: # if there is an exact match
      data['exact_match'] = exact_match_data(search_results[i]['pageid'])
      #print(data['exact_match'])
    else:
      clean_snippet = clean_snippets(search_results[i]['snippet'])
      if search_term.split(' ')[0] in clean_snippet and search_term.split(' ')[1] in clean_snippet:
        other_matches.append({
            'title': search_results[i]['title'],
            'pageid': search_results[i]['pageid'],
            'snippet': clean_snippet})
  if 'exact_match' in data:
    data['other_matches'] = other_matches
  else:
    data = {}
  #print(data)
  return data
