import os
import sys
import json
import time
import logging
import requests
import csv
import shutil
from datetime import datetime, timezone
from tqdm import tqdm
from collections import Counter
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.feature_extraction import text
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
import numpy as np

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

def safe_get(url, headers=None, params=None, proxies=None, retries=3, delay=2):
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=headers, params=params, proxies=proxies, timeout=20)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logging.warning(f"Attempt {attempt+1}/{retries} failed: {e}")
            time.sleep(delay)
    return None

class VbrickAuthManager:
    def __init__(self, base_url, api_key, api_secret, proxies=None):
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.api_secret = api_secret
        self.proxies = proxies
        self.token = None
        self.token_created = 0
        self.expires_in = 3600

    def get_token(self):
        if not self.token or (time.time() - self.token_created) > (self.expires_in - 60):
            self._refresh_token()
        return self.token

    def _refresh_token(self):
        url = f"{self.base_url}/api/v2/authenticate"
        payload = {"apiKey": self.api_key, "apiSecret": self.api_secret}
        headers = {"accept": "application/json", "content-type": "application/json"}
        resp = requests.post(url, headers=headers, json=payload, proxies=self.proxies)
        resp.raise_for_status()
        data = resp.json()
        self.token = data["token"]
        self.expires_in = data.get("expiresIn", 3600)
        self.token_created = time.time()

def fetch_webcasts(auth_mgr, start_date, end_date):
    url = f"{auth_mgr.base_url}/api/v2/scheduled-events"
    headers = {"Authorization": f"Bearer {auth_mgr.get_token()}"}
    params = {
        "after": start_date,
        "before": end_date,
        "sortField": "startDate",
        "sortDirection": "asc"
    }
    data = safe_get(url, headers=headers, params=params, proxies=auth_mgr.proxies)
    return data if isinstance(data, list) else []

def fetch_attendance(auth_mgr, event_id):
    base_url = f"{auth_mgr.base_url}/api/v2/scheduled-events/{event_id}/post-event-report"
    headers = {"Authorization": f"VBrick {auth_mgr.get_token()}"}
    all_sessions = []
    scroll_id = None
    page_count = 0
    max_pages = 40  # safety cap
    null_scroll_count = 0  # track how many times scrollid is None
    
    while True:
        params = {"scrollId": scroll_id} if scroll_id else {}
        data = safe_get(base_url, headers=headers, params=params, proxies=auth_mgr.proxies)
        if not data:
            return None
        # logging.warning(f"No data returned for event {event_id}.")
        
        sessions = data.get("sessions", [])
        if not sessions:
            # logging.info(f"No more sessions returned for event {event_id}.")
            break
        
        all_sessions.extend(sessions)
        # logging.info(f"Page {page_count + 1}: Fetched {len(sessions)} sessions for event {event_id} (scrollId: {scroll_id})")
        
        scroll_id = data.get("scrollId")
        if scroll_id is None:
            null_scroll_count += 1
            if null_scroll_count >= 1:
                # logging.info(f"Stopping pagination for event {event_id}: scrollId was None 1 time.")
                break
        else:
            null_scroll_count = 0  # reset if a valid scrollid is received
        
        page_count += 1
        if page_count >= max_pages:
            # logging.warning(f"Reached max page limit ({max_pages}) for event {event_id}.")
            break
    
    data["sessions"] = all_sessions
    return data


def parse_duration_to_seconds(duration_str):
    try:
        h, m, s = map(int, duration_str.split(":"))
        return h * 3600 + m * 60 + s
    except Exception:
        return 0

def parse_numeric(value):
    try:
        return int(value)
    except (ValueError, TypeError):
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0

def parse_float(value):
    try:
        return float(value)
    except (ValueError, TypeError):
        return 0.0



def assign_categories_to_webcasts(webcast_data):
    logging.info("Starting AI-based categorization of webcast titles...")
    
    # Extract titles from webcast data
    titles = [item["title"] for item in webcast_data if "title" in item]
    logging.info(f"Extracted {len(titles)} titles for clustering.")
    
    # Convert titles into TF-IDF vectors to capture term importance
    custom_stop_words = list(text.ENGLISH_STOP_WORDS.union(['ubs', '2024', '2025']))
    vectorizer = TfidfVectorizer(stop_words=custom_stop_words)
    X = vectorizer.fit_transform(titles)
    logging.info("TF-IDF vectorization complete.")
    
    # Determine the optimal number of clusters using silhouette score
    best_k = 2
    best_score = -1
    logging.info("Evaluating optimal number of clusters using silhouette score...")
    for k in range(2, min(11, len(titles))):  # Try cluster sizes from 2 to 10
        kmeans = KMeans(n_clusters=k, random_state=42)
        labels = kmeans.fit_predict(X)
        score = silhouette_score(X, labels)
        logging.debug(f"Silhouette score for k={k}: {score:.4f}")
        if score > best_score:
            best_k = k
            best_score = score
    logging.info(f"Optimal number of clusters determined: k={best_k} with silhouette score={best_score:.4f}")
    
    # Fit KMeans clustering with the best number of clusters
    kmeans = KMeans(n_clusters=best_k, random_state=42)
    clusters = kmeans.fit_predict(X)
    logging.info("KMeans clustering complete.")
    
    # Helper function to extract top terms for each cluster
    def get_top_terms_per_cluster(tfidf_matrix, labels, vectorizer, top_n=3):
        logging.info("Extracting top terms for each cluster...")
        # Compute average TF-IDF scores per cluster
        df = pd.DataFrame(tfidf_matrix.todense()).groupby(labels).mean()
        terms = vectorizer.get_feature_names_out()
        top_terms = {}
        for i, row in df.iterrows():
            # Get indices of top N terms with highest average TF-IDF scores
            top_indices = np.argsort(row)[-top_n:][::-1]
            top_terms[i] = [terms[ind] for ind in top_indices]
            logging.debug(f"Cluster {i} top terms: {top_terms[i]}")
        return top_terms
    
    # Get top descriptive terms for each cluster
    top_terms = get_top_terms_per_cluster(X, clusters, vectorizer)
    # Create readable category names by joining top terms
    category_names = {i: " / ".join(terms).title() for i, terms in top_terms.items()}
    logging.info("Descriptive category names generated.")
    
    # Assign category names to each webcast item based on cluster label
    for item, label in zip(webcast_data, clusters):
        item["category_full"] = category_names[label]
        logging.info("Webcast items updated with category labels.")

def split_category_and_subcategory(webcast_data):
    for item in webcast_data:
        full_category = item.get("category_full", "")
        terms = full_category.split(" / ")
        item["category"] = terms[0] if terms else ""
        item["subcategory"] = full_category
    logging.info("Split category into 'category' (top term) and 'subcategory' (top 3 terms).")

def main():
    with open("secrets.json") as f:
        cfg = json.load(f)
    
    base_url = cfg.get("base_url")
    api_key = cfg.get("api_key")
    api_secret = cfg.get("api_secret")
    proxy_url = cfg.get("proxies")
    proxies = proxy_url if proxy_url else None
    
    auth_mgr = VbrickAuthManager(base_url, api_key, api_secret, proxies)
    start_date = "2025-07-01T00:00:00Z"
    end_date = datetime.now(timezone.utc).isoformat()
    
    logging.info("Fetching webcast metadata...")
    webcast_data = fetch_webcasts(auth_mgr, start_date, end_date)
    if not webcast_data:
        logging.error("No webcast data retrieved.")
        return
    
    assign_categories_to_webcasts(webcast_data)
    split_category_and_subcategory(webcast_data)
    

    with open("webcast_metadata_categorized.json", "w", encoding="utf-8") as jf:
        json.dump(webcast_data, jf, indent=2)
    logging.info("Webcast metadata written to webcast_metadata_categorized.json")
    
    logging.info(f"Fetched {len(webcast_data)} webcasts. Enriching with attendance data...")
    rows = []
    failed_events = []
    dynamic_fields = set()
    
    zone_mapping = {
        "APAC": "APAC",
        "APAC CS": "APAC",
        "APAC Cloud VDI's & Surface Device's": "APAC",
        "America": "America",
        "America CS": "America",
        "America Cloud VDI's & Surface Device's": "America",
        "Core HLS(Connect Me / Remote User)": "Other",
        "DefaultZone": "Other",
        "EMEA": "EMEA",
        "EMEA CS": "EMEA",
        "EMEA Cloud VDI's & Surface Device's": "EMEA",
        "None": "Other",
        "Secure Web Gateway Zone for Surface Device(Direct)": "Other",
        "Secure Web Gateway Zone for Surface Device(Direct).1": "Other",
        "Swiss": "Swiss",
        "Swiss CS": "Swiss",
        "Swiss Cloud VDI's & Surface Device's": "Swiss",
        "UBS Card Center": "Other",
        "Z - Fallback": "Other"
    }

    browser_mapping = {
        "Chrome": "Chrome",
        "Chrome mobile": "Chrome",
        "Microsoft Edge": "Edge",
        "Microsoft Edge mobile": "Edge",
        "Android WebView": "Other",
        "Apple Mail": "Other",
        "Chrome Mobile": "Chrome",
        "Firefox": "Other",
        "Mozilla": "Other",
        "None": "Other",
        "Opera": "Other",
        "Safari": "Other",
        "Safari mobile": "Other",
        "Unknown": "Other"
    }

    device_mapping = {
        "PC": "PC",
        "Mobile Device": "Mobile",
        "None": "Other",
        "Unknown": "Other"
    }
    
    # filtered_data = [w for w in webcast_data if w.get("id") == "1e5ed26b-4080-4813-8cba-870e6051a743"]
    # for webcast in tqdm(filtered_data, desc="Processing Webcasts", unit="webcast"):

    for webcast in tqdm(webcast_data, desc="Processing Webcasts", unit="webcast"):
        event_id = webcast.get("id")
        title = webcast.get("title")
        vodId = webcast.get("linkedVideoId")
        category = webcast.get("category", "")
        subcategory = webcast.get("subcategory", "")
        attendance = fetch_attendance(auth_mgr, event_id)
        if attendance is None:
            failed_events.append({"id": event_id, "title": title})
            continue
        sessions = attendance.get("sessions", [])
        # attendee_sessions = [s for s in sessions if s.get("attendeeType") == "Attendee"]
        attendee_sessions = [s for s in sessions]
        

        browser_counter = Counter()
        device_counter = Counter()
        zone_counter = Counter()
        viewing_time = 0
        
        # Inside your session processing loop
        for session in sessions:
            # Normalize and map browser
            raw_browser = session.get("browser")
            browser = str(raw_browser).strip() if raw_browser else "Other"
            grouped_browser = browser_mapping.get(browser, "Other")
            browser_counter[grouped_browser] += 1
            
            # Normalize and map device type
            raw_device = session.get("deviceType")
            device = str(raw_device).strip() if raw_device else "Other"
            grouped_device = device_mapping.get(device, "Other")
            device_counter[grouped_device] += 1

            # Normalize and map zone
            raw_zone = session.get("zone")
            zone = str(raw_zone).strip() if raw_zone else "Other"
            grouped_zone = zone_mapping.get(zone, "Other")
            zone_counter[grouped_zone] += 1
            
            # Viewing time
            viewing_time += parse_duration_to_seconds(session.get("viewingTime", "00:00:00"))
        
        attendee_total = sum(browser_counter.values())
        
        base_row = {
            "id": event_id,
            "title": title,
            "vodId": vodId,
            "eventUrl": webcast.get("eventUrl"),
            "attendeeCount": attendance.get("attendeeCount"),
            "attendeeTotal": attendee_total,
            "startDate": webcast.get("startDate"),
            "endDate": webcast.get("endDate"),
            "total_viewingTime": viewing_time,
            "category": category,
            "subcategory": subcategory
        }
        
        for key, count in browser_counter.items():
            col = f"browser_{key}"
            base_row[col] = count
            dynamic_fields.add(col)
        for key, count in device_counter.items():
            col = f"deviceType_{key}"
            base_row[col] = count
            dynamic_fields.add(col)
        for key, count in zone_counter.items():
            col = f"zone_{key}"
            base_row[col] = count
            dynamic_fields.add(col)
        
        rows.append(base_row)
    
    if rows:
    
        # Define static fields
        static_fields = ["id", "title", "vodId", "eventUrl", "attendeeCount", "attendeeTotal", "startDate", "endDate", "total_viewingTime", "category", "subcategory"]
        
        fieldnames = static_fields + sorted(dynamic_fields)
        with open("webcast_summary.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow({field: row.get(field, "") for field in fieldnames})
        logging.info("Webcast summary exported to webcast_summary.csv")
    
    if failed_events:
        with open("failed_webcasts.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["id", "title"])
            writer.writeheader()
            writer.writerows(failed_events)
        logging.info(f"{len(failed_events)} webcasts failed and were logged in failed_webcasts.csv")
    

    # Define source and destination paths
    source_path = f"P:/IMPORTANT/Projects/Vbrick/webcast_summary.csv"
    destination_path = f"Q:/Vbrick/webcast_summary.csv"

    # Move the file
    try:
        shutil.move(source_path, destination_path)
        print("File moved successfully.")
    except FileNotFoundError:
        print("The source file was not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()