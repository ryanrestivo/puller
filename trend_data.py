import time
import re
import urllib.parse
from datetime import datetime, timezone, timedelta
import requests
import feedparser
import os
import json

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

HEADERS = {"User-Agent": "TrendMatchBot/5.7 (Authenticated Pipeline)"}

# Bluesky Credentials for API Search Access
BSKY_HANDLE = os.getenv("BLUESKY_HANDLE")
BSKY_APP_PASSWORD = os.getenv("BLUESKY_PW")

# API Endpoints
BSKY_AUTH_URL = "https://bsky.social/xrpc/com.atproto.server.createSession"
BSKY_API_URL = "https://public.api.bsky.app/xrpc/app.bsky.feed.getAuthorFeed"
BSKY_SEARCH_URL = "https://bsky.social/xrpc/app.bsky.feed.searchPosts"

HANDLE_ONE = os.getenv("HANDLE_ONE")
HANDLE_TWO = os.getenv("HANDLE_TWO")

TARGET_GEOS = ["US", "GB", "CA"]

STATIC_FIREHOSES = {
    "upi_wire": os.getenv("UPI_WIRE"),
    "cbs_sports": os.getenv("SPORTS_WIRE")
}

SIGNAL_MAP = {
    (True, True): {
        "status": "🚨 HIGH VELOCITY (BREAKING)",
        "description": "Spiking on Bluesky conversations & trending hashtags",
        "social": "high",
        "flags": ["social_word", "social_hashtag"]
    },
    (True, False): {
        "status": "🔥 BUILDING",
        "description": "Active on Bluesky social feed",
        "social": "medium",
        "flags": ["social_word"]
    },
    (False, True): {
        "status": "🔥 BUILDING",
        "description": "Trending as a hashtag on Bluesky",
        "social": "medium",
        "flags": ["social_hashtag"]
    },
    (False, False): {
        "status": "📌 NEWS ONLY",
        "description": "No social buzz detected yet",
        "social": None,
        "flags": ["trends_only"]
    }
}

BSKY_SESSION_TOKEN = None

def get_bsky_session_token():
    """Authenticates with Bluesky to acquire a JWT Access Token."""
    global BSKY_SESSION_TOKEN
    if BSKY_SESSION_TOKEN:
        return BSKY_SESSION_TOKEN
    payload = {
        "identifier": BSKY_HANDLE,
        "password": BSKY_APP_PASSWORD
    }
    try:
        res = requests.post(BSKY_AUTH_URL, json=payload, timeout=10)
        if res.status_code == 200:
            BSKY_SESSION_TOKEN = res.json().get("accessJwt")
            return BSKY_SESSION_TOKEN
        else:
            print(f"[Bluesky Auth Error]: Status {res.status_code} - {res.text}")
    except Exception as e:
        print(f"[Bluesky Auth Exception]: {e}")
    return None

def inspect_bluesky_posts(keyword, limit=5, max_age_hours=24):
    """
    Deep-inspects Bluesky search API:
    1. Filters server-side via `since` parameter to only fetch recent posts.
    2. Sorts server-side by engagement (`sort: top`).
    3. Filters client-side for recency and calculates top total engagement.
    """
    token = get_bsky_session_token()
    if not token:
        return []
    headers = {
        "User-Agent": "TrendMatchBot/5.9",
        "Authorization": f"Bearer {token}"
    }
    # 1. Calculate UTC ISO string for server-side `since` filtering
    cutoff_time = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
    since_iso = cutoff_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    clean_kw = keyword.replace("#", "").strip()
    words = clean_kw.split()
    queries_to_try = [clean_kw]
    if len(words) > 2:
        queries_to_try.append(" ".join(words[:2]))
    for query in queries_to_try:
        params = {
            "q": query,
            "limit": 20,          
            "sort": "top",        
            "since": since_iso
        }
        try:
            res = requests.get(BSKY_SEARCH_URL, params=params, headers=headers, timeout=8)
            if res.status_code != 200:
                continue
            posts_data = res.json().get("posts", [])
            if not posts_data:
                continue
            inspected_posts = []
            for p in posts_data:
                record = p.get("record", {})
                created_at_str = record.get("createdAt", "")
                if created_at_str:
                    try:
                        post_dt = datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))
                        if post_dt < cutoff_time:
                            continue
                    except ValueError:
                        pass
                author_handle = p.get("author", {}).get("handle", "unknown")
                uri_parts = p.get("uri", "").split("/")
                rkey = uri_parts[-1] if uri_parts else ""
                like_count = p.get("likeCount", 0)
                repost_count = p.get("repostCount", 0)
                reply_count = p.get("replyCount", 0)
                total_engagement = like_count + repost_count + reply_count
                inspected_posts.append({
                    "author": author_handle,
                    "text": record.get("text", ""),
                    "created_at": created_at_str,
                    "like_count": like_count,
                    "repost_count": repost_count,
                    "reply_count": reply_count,
                    "total_engagement": total_engagement,
                    "post_url": f"https://bsky.app/profile/{author_handle}/post/{rkey}" if rkey else ""
                })
            if inspected_posts:
                # Rank client-side by highest engagement
                inspected_posts.sort(key=lambda x: x["total_engagement"], reverse=True)
                
                # Return the top N most engaged, fresh posts
                return inspected_posts[:limit]
        except Exception as e:
            print(f"[Bluesky Search Exception for '{query}']: {e}")
    return []

def clean_html_tags(raw_html):
    if not raw_html:
        return ""
    clean_text = re.sub(r"<[^>]+>", " ", raw_html)
    return " ".join(clean_text.split())

def extract_entry_data(entry, source_name):
    summary_raw = getattr(entry, "summary", "") or getattr(entry, "description", "")
    return {
        "source": source_name,
        "title": entry.title,
        "summary": clean_html_tags(summary_raw),
        "published": getattr(entry, "published", "N/A"),
        "link": entry.link
    }

def fetch_google_trends_multi_geo(geos):
    all_trends = []
    for geo in geos:
        rss_url = f"https://trends.google.com/trending/rss?geo={geo}"
        try:
            feed = feedparser.parse(rss_url)
            for entry in feed.entries:
                all_trends.append({
                    "title": entry.title,
                    "traffic": getattr(entry, "ht_approx_traffic", "N/A"),
                    "guid": getattr(entry, "id", entry.title),
                    "geo": geo
                })
        except Exception as e:
            print(f"[Google Trends Fetch Error - {geo}]: {e}")
    return all_trends


def fetch_bsky_author_post(handle):
    params = {"actor": handle, "limit": 1}
    try:
        res = requests.get(BSKY_API_URL, params=params, headers=HEADERS, timeout=10)
        if res.status_code == 200:
            feed = res.json().get("feed", [])
            if feed:
                return feed[0]["post"]["record"]["text"]
    except Exception as e:
        print(f"[Bluesky Fetch Error - {handle}]: {e}")
    return ""


def get_social_signals():
    breezing_text = fetch_bsky_author_post(HANDLE_ONE)
    keywords = re.findall(r"(?:💨)?x\d+\*?\s*-\s*([^\n\r*]+)", breezing_text)

    tags_text = fetch_bsky_author_post(HANDLE_TWO)
    hashtags = re.findall(r"#\w+", tags_text)

    return {
        "keywords": [k.strip().lower() for k in keywords],
        "hashtags": [h.strip().lower() for h in hashtags]
    }


def fetch_firehose_entries():
    raw_articles = []
    for source_label, feed_url in STATIC_FIREHOSES.items():
        try:
            feed = feedparser.parse(feed_url)
            for entry in feed.entries:
                raw_articles.append(extract_entry_data(entry, source_label))
        except Exception as e:
            print(f"[{source_label} Fetch Error]: {e}")
    return raw_articles


def search_google_news_multi(keyword, geo="US", max_results=3):
    encoded_query = urllib.parse.quote(keyword)
    hl_param = "es-419" if geo == "MX" else f"en-{geo}"
    ceid_param = f"{geo}:es" if geo == "MX" else f"{geo}:en"
    
    gn_url = f"https://news.google.com/rss/search?q={encoded_query}&hl={hl_param}&gl={geo}&ceid={ceid_param}"
    feed = feedparser.parse(gn_url)
    
    results = []
    for entry in feed.entries[:max_results]:
        results.append(extract_entry_data(entry, f"google_news_{geo.lower()}"))
    return results

# --- Pipeline Execution ---
def run_pipeline():
    data_list = []
    cycle_utc_timestamp = datetime.now(timezone.utc).isoformat()
    print("=" * 65)
    print(f" Multi-Geo Data Pipeline Cycle: {cycle_utc_timestamp}")
    print("=" * 65)
    trends = fetch_google_trends_multi_geo(TARGET_GEOS)
    if not trends:
        print("[Warning]: No Google Trends retrieved.")
        return []
    social_data = get_social_signals()
    bsky_words = social_data["keywords"]
    bsky_tags = social_data["hashtags"]
    firehose_articles = fetch_firehose_entries()
    for trend in trends:
        title = trend["title"]
        geo_origin = trend["geo"]
        title_lower = title.lower().strip()        
        if len(title_lower) <= 1:
            continue
        title_tag = f"#{re.sub(r'[^a-zA-Z0-9]', '', title_lower)}"
        has_word = any(w in title_lower or title_lower in w for w in bsky_words)
        has_tag = any(h in title_tag or title_tag in h for h in bsky_tags)
        signal = SIGNAL_MAP[(has_word, has_tag)]
        inspected_posts = []
        if signal["social"] in ["medium", "high"]:
            inspected_posts = inspect_bluesky_posts(title_lower, limit=5)
        matched_stories = []
        for article in firehose_articles:
            if title_lower in article["title"].lower():
                matched_stories.append(article)
        gn_matches = search_google_news_multi(title, geo=geo_origin, max_results=3)
        existing_links = {s["link"] for s in matched_stories}
        for gn_item in gn_matches:
            if gn_item["link"] not in existing_links:
                matched_stories.append(gn_item)
        trend_document = {
            "keyword": title_lower,
            "keyword_original": title,
            "geo": geo_origin,
            "traffic": trend["traffic"],
            "guid": trend["guid"],
            "fetched_at": cycle_utc_timestamp,
            "signal": {
                "status": signal["status"],
                "description": signal["description"],
                "social": signal["social"],
                "flags": signal["flags"],
                "has_word_match": has_word,
                "has_tag_match": has_tag
            },
            "bsky_inspected_posts": inspected_posts,
            "bsky_post_count": len(inspected_posts),
            "stories": matched_stories,
            "story_count": len(matched_stories)
        }
        data_list.append(trend_document)
    return data_list


if __name__ in "__main__":
    input_data = {}
    input_data['rows'] = run_pipeline()
    inputDataRequests(os.getenv("TREND_BACKEND"), os.getenv("TREND_BACKSET"), input_data)