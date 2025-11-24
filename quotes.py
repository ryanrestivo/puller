import spacy
import requests
import json
from datetime import datetime, timedelta
import pandas as pd
import os
import ast
import re

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


# NLP WORK
nlp = spacy.load("en_core_web_md")

def entsTracking(doc):
    return [{ent.label_: ent.text} for ent in doc.ents]

def person_processor(data):
  people_list = []
  for i in range(0,len(data)):
    doc = nlp(data[i]['paragraphText'])
    ent_data = entsTracking(doc)
    people = [doc['PERSON'] for doc in ent_data if 'PERSON' in doc and len(doc['PERSON']) > 3 and len(doc['PERSON'].split()) > 1 and doc['PERSON'] is not data[i]['author']]
    people_list.extend(people)
  return people_list

def extract_person_data(data, person_name):
  # take the JSON response from REQUEST to turn into data
  data = pd.DataFrame(data)
  data['paragraphText'] = data['paragraphText'].fillna('')
  # Filter by the person we want
  mask = data['paragraphText'].str.contains(person_name)
  result = data[mask]
  return result


def extract_quote(text, attribution_verbs):
    """
    Extract quoted text, handling balanced and unbalanced quotes without duplicates.
    """
    quotes = []

    # Grab all balanced quotes first
    balanced = re.findall(r'["“](.+?)["”]', text, flags=re.DOTALL)
    for q in balanced:
        q = q.strip()
        if q and q not in quotes:
            quotes.append(q)

    # Grab unbalanced quotes anywhere
    verb_group = r'(?:' + '|'.join(attribution_verbs) + r')'
    pattern = r'["“](.+?)(?=(?:\s+' + verb_group + r'|\s$|\n))'
    unbalanced = re.findall(pattern, text, flags=re.IGNORECASE | re.DOTALL)
    for q in unbalanced:
        q = q.strip()
        # only add if not a substring of any existing quote
        if q and not any(q in existing or existing in q for existing in quotes):
            quotes.append(q)

    return quotes


def normalize_name(name):
    """Normalize names for safe comparison (lowercase, strip punctuation)."""
    return re.sub(r'[^\w\s]', '', name.lower()).strip()

def person_in_story(person_name, text):
    text_norm = normalize_name(text)
    name_parts = normalize_name(person_name).split()
    return any(part in text_norm for part in name_parts)


def detect_speaker(sent_text, attribution_verbs, target_person=None):
    text = sent_text
    verb_group = r'(?:' + '|'.join(attribution_verbs) + r')'
    name_pattern = r'(\b[\w\-]+(?:\s+[\w\-]+)?)'
    p1 = rf'{name_pattern}\s+{verb_group}\b'
    p2 = rf'{verb_group}\s+{name_pattern}'
    p3 = rf'{verb_group}\s+[^,]+,\s+{name_pattern}'

    patterns = [p1, p2, p3]

    for pattern in patterns:
        matches = re.findall(pattern, text, flags=re.IGNORECASE)
        for raw_name in matches:
            detected_norm = normalize_name(raw_name)
            if not detected_norm:
                continue
            if not target_person:
                return raw_name.strip()
            target_norm = normalize_name(target_person)
            detected_last = detected_norm.split()[-1]
            target_last = target_norm.split()[-1]
            if detected_last == target_last:
                return target_person
            if detected_norm == target_norm:
                return target_person

    return None

def extract_mentions(dataItem, person_name, attribution_verbs):
    title = dataItem.get('title')
    site = dataItem['site']
    text = dataItem['paragraphText']
    author = dataItem['author']
    doc = nlp(text)
    person_mentions = []
    person_name_lower = person_name.lower()
    person_name_parts = person_name_lower.split()
    sentences = list(doc.sents)

    for i, sent in enumerate(sentences):
        if person_name_lower in sent.text.lower() or any(part in sent.text.lower() for part in person_name_parts):
            mention = {
                "mention": sent.text,
                "site": site,
                "title": title,
            }
            try:
                mention['author'] = author
            except Exception:
                pass
            try:
                mention['publishDate'] = dataItem.get('publishDate')
            except Exception:
                pass
            try:
                if author == person_name:
                    mention['isAuthor'] = True
            except Exception:
                pass

            mention['fullNameMentioned'] = person_in_story(person_name, text)

            # extend mention context like before
            next_sent = ""
            for j in range(i+1, len(sentences)):
              # always extend if the next sentence begins another quote
              if '"' in sentences[j].text or '“' in sentences[j].text:
                  next_sent += " " + sentences[j].text
              else:
                  break
            mention["mention"] += next_sent

            # --- BUILD FULL QUOTE BLOCK (fix for multi-sentence quotes) ---
            quote_source = sent.text

            # Look forward for additional quoted sentences
            j = i + 1
            while j < len(sentences) and ('"' in sentences[j].text or '“' in sentences[j].text):
                quote_source += " " + sentences[j].text
                j += 1

            # Look backward (in case the quote started earlier than this sentence)
            k = i - 1
            while k >= 0 and ('"' in sentences[k].text or '“' in sentences[k].text):
                quote_source = sentences[k].text + " " + quote_source
                k -= 1

            quotes = extract_quote(quote_source, attribution_verbs)
            quote_data = ' '.join(quotes) if quotes else None
            mention['quotes'] = quote_data

            # detect speaker from the narrowed text
            mention['speaker'] = detect_speaker(quote_source, attribution_verbs, person_name) if quote_data else None

            person_mentions.append(mention)

    return person_mentions



### IF DOESNT WORK UNDO
def extract_attributable_quotes(data_item, person_name, attribution_verbs):
    title = data_item.get("title")
    site = data_item.get("site")
    text = data_item.get("paragraphText")
    publish_date = data_item.get("publishDate")
    author = data_item.get("author")
    person_explicit = person_in_story(person_name, text)
    person_name_norm = normalize_name(person_name)
    person_first, person_last = person_name_norm.split()[0], person_name_norm.split()[-1]

    doc = nlp(text)
    sentences = list(doc.sents)

    person_quotes = []
    seen_quotes = set()

    for sent in sentences:
        sent_text = sent.text.strip()
        quotes_in_sent = extract_quote(sent_text, attribution_verbs)
        if not quotes_in_sent:
            continue

        # Look for speaker attribution immediately after quote
        for quote in quotes_in_sent:
            # Pattern: quote followed by something like: "Wamp said" or "John Wamp says"
            pattern = rf'{re.escape(quote)}["”]?\s*,?\s*(\w+\s*\w*)\s+(?:said|says)\b'
            match = re.search(pattern, sent_text, flags=re.IGNORECASE)
            if match:
                speaker_raw = match.group(1)
                speaker_norm = normalize_name(speaker_raw)
                # Check if speaker matches target person (first or last name)
                if not (person_first in speaker_norm or person_last in speaker_norm):
                    continue  # skip quote if speaker is not the target person
            else:
                continue  # no clear speaker attribution

            # Build quote block by looking at consecutive sentences if needed
            quote_block = quote
            # Optional: extend to next sentences if they continue with quotes
            idx = sentences.index(sent)
            j = idx + 1
            while j < len(sentences):
                next_s = sentences[j].text.strip()
                next_quotes = extract_quote(next_s, attribution_verbs)
                if next_quotes:
                    quote_block += " " + " ".join(next_quotes)
                    j += 1
                else:
                    break

            normalized_quote = re.sub(r'\s+', ' ', quote_block.lower())
            if normalized_quote in seen_quotes:
                continue

            person_quotes.append({
                "mention": sent_text,
                "quotes": quote_block.strip(),
                "site": site,
                "title": title,
                "speaker": person_name,
                "publishDate": publish_date,
                "author": author,
                "fullNameMentioned": person_explicit
            })
            seen_quotes.add(normalized_quote)

    return person_quotes


def missingDates(teamID, table):
    pipeline = [
        {
            '$group': {
                '_id': '$publishDate',
                'dates': {'$addToSet': '$publishDate'} # Crucially, use $addToSet
            }
        },
        {
            '$project': {
                '_id': 0,
                'dates': 1
            }
        }
    ]
    story_dates = dataRequestsGet(teamID, 'storyData', pipeline, "aggregate")
    all_dates = sorted(list(set([i['dates'][0] for i in story_dates])), reverse=True)
    existing_dates_agg = [
    {
        "$group": {
            "_id": None,
            "dates": {"$push": "$date"}
        }
    },
    {
        "$unwind": "$dates"
    },
    {
        "$group": {
            "_id": None,
            "dates": {"$push": "$dates"}
        }
    }]
    existing_dates = dataRequestsGet(teamID, table, existing_dates_agg, "aggregate")
    dates_list = existing_dates[0]['dates']
    missing_dates = [date for date in all_dates if date not in dates_list]
    return missing_dates

def produce_expert(person, data):
    seen = set()
    unique_mentions = []
    for item in ['mention', 'quotes']:
        if item in data:
            mention = data[item]
            if mention not in seen:
                seen.add(mention)
                unique_mentions.append(mention)
    mentions_removed_dupes = unique_mentions
    expertises = []
    for a in mentions_removed_dupes:
      text = a
      try:
          readout = flex_llm_point({'training': f'{os.getenv("EXPERT_TRAIN")}', 
                                    'rule': f'{os.getenv("EXPERT_RULE_ONE")} {person} {os.getenv("EXPERT_RULE_TWO")} ', 
                                    'text': text})
          try:
              expertise = ast.literal_eval(readout['choices'][-1]['message']['content'])['expertise']
          except Exception:
              start_index = readout['choices'][-1]['message']['content'].find('{')
              end_index = readout['choices'][-1]['message']['content'].rfind('}')
              if start_index != -1 and end_index != -1:
                  json_string = readout['choices'][-1]['message']['content'][start_index:end_index + 1]
                  try:
                      expertise = ast.literal_eval(json_string)['expertise']
                  except Exception:
                      expertise = []
          expertises.extend(expertise)
      except Exception as e:
          print(e)
    final_expertises = pd.DataFrame(expertises).value_counts().reset_index()
    final_expertises[0] = final_expertises[0].apply(lambda x:x.lower())
    final_expertises = final_expertises.rename(columns={0: 'expertise'})
    final_expertises = final_expertises.set_index('expertise')['count'].to_dict()
    return final_expertises

def relationships(person, data):
    seen = set()
    unique_mentions = []
    for item in ['mention', 'quotes']:
        if item in data:
            mention = data[item]
            if mention not in seen:
                seen.add(mention)
                unique_mentions.append(mention)
    mentions_removed_dupes = unique_mentions
    expertises = []
    for a in mentions_removed_dupes:
        text = a
        try:
            readout = flex_llm_point({'training': f'{os.getenv("RELATIONSHIP_RULE_ONE")} {person} {os.getenv("RELATIONSHIP_RULE_TWO")}', 
                                        'rule': f'{os.getenv("REL_SET_ONE")} {person} {os.getenv("REL_SET_TWO")} {person} {os.getenv("REL_SET_THREE")} ', 
                                        'text': text})
            #print(readout)
            try:
                expertise = ast.literal_eval(readout['choices'][-1]['message']['content'])
            except Exception:
                expertise = []
            expertises.extend(expertise)
        except Exception as e:
            print(e)
    return expertises


def storyWork(team_id, date_num):
    attribution_verbs = ["said", "says", "told", "tells", "writes", "reports"] #, "stated", "echoes", "added","noted", "warns", "warned", "argues", "explained","conceded", "quipped"]
    pipeline = [
          {
              "$match": {
                  "publishDate": {
                      "$eq": date_num
                  }
              }
          },
          {
              "$project": {
                  "paragraphText": 1,
                  "author": 1,
                  "title": 1,
                  "publishDate": 1,
                  "site": 1,
                  "_id": 0
              }
          }
      ]
    data = dataRequestsGet(team_id, 'storyData', pipeline, "aggregate")
    if 'error' in data:
        print(f"No available data for {date_num}")
        raise Exception
    if type(data) == list:
        story_data = pd.DataFrame(data)
    people_by_day = person_processor(data)
    people_trim = list(set(people_by_day))
    for b in people_trim:
        try:
            #print(b)
            person_result = extract_person_data(story_data,b)
            person_data_list = []
            for i in range(0,len(person_result)):
                person_data_list.extend(extract_mentions(person_result.iloc[i],b, attribution_verbs))
                person_data_list.extend(extract_attributable_quotes(person_result.iloc[i],b,attribution_verbs))
                if len(person_data_list) > 0:
                    for a in person_data_list:
                        try:
                            a['expertise'] = produce_expert(b, a)
                        except Exception as e:
                            pass
                        try:
                            a['relationships'] = relationships(b, a)
                        except Exception as e:
                            pass
                        dataRequestsPUT(team_id,quote_table, {"person": b}, {'$push':{ "mentions": a}})
        except Exception as e:
            #print(b, e)
            pass
    dataRequestsPUT(team_id,'quoteDates', {"date": date_num}, {'$set':{"totalPeople": len(people_trim), "complete": True}})




if __name__ == "__main__":
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
    endpoint_space['team_id']
    missing_dates = missingDates(endpoint_space['team_id'], "quoteDates")
    print(missing_dates)
    for i in missing_dates: #[2:4]:
      try:
          print(f"Running {i}")
          storyWork(endpoint_space['team_id'], i)
      except Exception as e:
          print(f"Error on {i}: {e}")
