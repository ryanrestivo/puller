import re
import json
import requests
import os


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
        print("STASHING VARIABLES")
        print(feed)
    except json.JSONDecodeError as e:
        print("Error decoding JSON:", e)
else:
    print("Environment variable MY_SECRET_JSON is not set.")


