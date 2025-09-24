import os
import sys
import json
import time
import logging
import requests
import shutil
from requests.exceptions import ProxyError, ConnectionError
from datetime import datetime, date, timedelta, timezone
from tqdm import tqdm
import csv

"""
This script authenticates with the Vbrick API, retrieves video metadata and daily view statistics 
for all active videos uploaded in the past two years, and exports the results to JSON and CSV files.
It includes robust error handling, token management, and supports proxy configuration.
The final CSV is moved to a designated network location for further use.
"""

# Enable debug logging
logging.basicConfig(
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
def safe_get(url, headers=None, params=None, proxies=None, retries=3, delay=2):
    """Safe HTTP GET with retry logic and error handling"""
    for attempt in range(1, retries + 1):
        try:
            logging.debug(f"GET {url} with headers={headers} and params={params}")
            resp = requests.get(url, headers=headers, params=params, proxies=proxies, timeout=20)
            resp.raise_for_status()
            return resp.json()
        except (ProxyError, ConnectionError) as e:
            logging.warning(f"Attempt {attempt}/{retries} network error: {e}")
        except requests.HTTPError as e:
            logging.error(f"HTTP {e.response.status_code} on GET {url}: {e.response.text}")
            if e.response.status_code == 429:
                delay *= 2
        time.sleep(delay)
    logging.error(f"Unexpected error on GET {url}: {e}")
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
        now = time.time()
        if not self.token or (now - self.token_created) > (self.expires_in - 60):
            self.refresh_token()
        return self.token
    
    def refresh_token(self):
        url = f"{self.base_url}/api/v2/authenticate"
        logging.info(f"Requesting new access token via {url}")
        
        headers = {
            "accept": "application/json",
            "content-type": "application/json"
        }
        payload = {
            "apiKey": self.api_key,
            "apiSecret": self.api_secret
        }
        
        try:
            resp = requests.post(
                url, 
                headers=headers, 
                json=payload, 
                proxies=self.proxies, 
                timeout=30
            )
            resp.raise_for_status()
            data = resp.json()
        except requests.HTTPError as e:
            logging.error("Authentication failed %s: %s", e.response.status_code, e.response.text)
            sys.exit(1)
        except Exception as ex:
            logging.error("Unexpected error fetching token: %s", ex)
            sys.exit(1)
        
        token = data.get("token")
        if not token:
            logging.error("No 'token' field in response: %s", data)
            sys.exit(1)
            
        self.token = token
        self.expires_in = data.get("expiresIn", self.expires_in)
        self.token_created = time.time()
        logging.info("obtained token; expires in %d seconds", self.expires_in)

def fetch_all_active_videos(auth_manager, proxies=None, count=100):
    videos = []
    scroll_id = None
    
    # Calculate the date 730 days ago in UTC ISO 8601 format
    two_year_ago = (datetime.now(timezone.utc) - timedelta(days=730)).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    logging.debug(f"Fetching videos from date: {two_year_ago}")
    
    # First request to get total count
    url = f"{auth_manager.base_url}/api/v2/videos/search"
    headers = {"Authorization": f"Bearer {auth_manager.get_token()}"}
    params = {
        "count": count,
        "status": "Active",
        "fromUploadDate": two_year_ago
    }
    
    data = safe_get(url, headers=headers, params=params, proxies=proxies)
    if not data:
        logging.error("Initial request failed, cannot fetch videos.")
        return []
    
    total = data.get("totalVideos", 0)
    pbar = tqdm(total=total, desc="Fetching Active Videos", unit="video", dynamic_ncols=True)
    
    items = data.get("videos", [])
    videos.extend(items)
    pbar.update(len(items))
    scroll_id = data.get("scrollId")
    
    while scroll_id:
        params[scroll_id] = scroll_id
        data = safe_get(url, headers=headers, params=params, proxies=proxies)
        if not data:
            break 
        
        items = data.get("videos", [])
        if not items:
            break
            
        videos.extend(items)
        pbar.update(len(items))
        scroll_id = data.get("scrollId")
        time.sleep(0.1)
    
    pbar.close()
    logging.info(f"Fetched {len(videos)} videos total")
    return videos

# def fetch_all_active_videos(auth_manager, proxies=None, count=100):
#     videos = []
#     scroll_id = None
#     pbar = tqdm(desc="Fetching Active Videos", unit="video", leave=False)
    
#     # Calculate the date 730 days ago in UTC ISO 8601 format
#     two_year_ago = (datetime.now(timezone.utc) - timedelta(days=730)).isoformat(timespec="milliseconds").replace("+00:00", "Z")
#     logging.debug(f"Fetching videos from date: {two_year_ago}")
    
#     while True:
#         url = f"{auth_manager.base_url}/api/v2/videos/search"
#         headers = {"Authorization": f"Bearer {auth_manager.get_token()}"}
#         params = {
#             "count": count,
#             "status": "Active",
#             "fromUploadDate": two_year_ago
#         }
#         if scroll_id:
#             params["scrollId"] = scroll_id
        
#         logging.debug(f"Requesting videos with scrollId={scroll_id}")
#         data = safe_get(url, headers=headers, params=params, proxies=proxies)
#         if not data:
#             break

#         items = data.get("videos", [])
#         logging.debug(f"Fetched {len(items)} videos")
#         if not items:
#             break
    
#         videos.extend(items)
#         pbar.update(len(items))
        
#         scroll_id = data.get("scrollId")
#         if not scroll_id:
#             break
        
#         time.sleep(0.1)
    
#     pbar.close()
#     logging.info(f"Total active videos fetched: %d", len(videos))
#     return videos

def get_video_summary(video_id, auth_manager, start_date=None, end_date=None, proxies=None):
    url = f"{auth_manager.base_url}/api/v2/videos/{video_id}/summary-statistics"
    headers = {
        "Authorization": f"Bearer {auth_manager.get_token()}",
        "Accept": "application/json"
    }
    params = {}
    if start_date:
        params["after"] = start_date
    if end_date:
        params["before"] = end_date
    
    data = safe_get(url, headers=headers, params=params, proxies=proxies)
    return data if data else {}

def main():
    cfg_path = os.getenv("VBRICK_CONFIG_JSON", "secrets.json")
    if not os.path.exists(cfg_path):
        logging.error("Config file not found: %s", cfg_path)
        sys.exit(1)
    with open(cfg_path) as f:
        cfg = json.load(f)
    
    base_url = cfg.get("base_url")
    api_key = cfg.get("api_key")
    api_secret = cfg.get("api_secret")
    proxy_url = cfg.get("proxies")
    suffix = date.today().isoformat()
    metadata_json = cfg.get("metadata_output", f"video_metadata_{suffix}.json")
    summary_json = cfg.get("analytics_json", f"video_summary_{suffix}.json")
    summary_csv = cfg.get("analytics_csv", f"UBS_TV_{suffix}.csv")
    
    if not all([base_url, api_key, api_secret]):
        logging.error("base_url, api_key, api_secret required in secrets.json")
        sys.exit(1)
    
    proxies = proxy_url if proxy_url else None
    auth_mgr = VbrickAuthManager(base_url, api_key, api_secret, proxies)
    
    # Fetch all videos from past 2 years
    end_date = date.today().isoformat()

    videos = fetch_all_active_videos(auth_mgr, proxies)
    
    with open(metadata_json, "w") as mf:
        json.dump(videos, mf, indent=2)
    logging.info("Wrote metadata JSON to %s", metadata_json)
    
    summary_dict = {}       # Fetch analytics for each video
    for v in tqdm(videos, desc="Summarizing Videos", unit="video"):
        vid = v.get("id")
        when_uploaded = v.get("whenUploaded", "")[:10]
        # Get video summary statistics
        stats = get_video_summary(vid, auth_mgr, when_uploaded, end_date, proxies)
        summary_dict[vid] = {"metadata": v,"dailySummary": stats}
    
    with open(summary_json, "w") as jf:                     # Save summary data to JSON
        json.dump(summary_dict, jf, indent=2)
    logging.info("Wrote summary JSON to %s", summary_json)
    
    # Convert to CSV format
    rows = []
    for vid, block in summary_dict.items():
        meta = block["metadata"]
        summary = block["dailySummary"]
        metadata_fields = {
            "video_id": meta.get("id"),
            "title": meta.get("title"),
            "playbackUrl": meta.get("playbackUrl"),
            "duration": meta.get("duration"),
            "whenUploaded": meta.get("whenUploaded"),
            "lastViewed": meta.get("lastViewed"),
            "whenPublished": meta.get("whenPublished"),
            "commentCount": meta.get("commentCount"),
            "score": meta.get("score"),
            "uploadedBy": meta.get("uploadedBy"),
            "tags": ", ".join(meta.get("tags", [])) if isinstance(meta.get("tags"), list) else meta.get("tags", "")
        }
        
        # Grouping functions
        def group_device_type(device_key):
            if device_key == 'PC':
                return 'Desktop'
            elif device_key == "Mobile Device":
                return "Mobile"
            else:
                return "Other Device"
        
        def group_browser_type(browser_key):
            if browser_key in ['Chrome', "Chrome Mobile"]:
                return "Chrome"
            elif browser_key in ['Microsoft Edge', 'Microsoft Edge mobile']:
                return "Microsoft Edge"
            else:
                return "Other Browser"
        
        # Apply grouping for device and browser statistics
        device_grouped = {}
        for d in summary.get('deviceCounts', []):
            group = group_device_type(d.get('key'))
            device_grouped[group] = device_grouped.get(group, 0) + d.get('value', 0)
        
        browser_grouped = {}
        for b in summary.get('browserCounts', []):
            group = group_browser_type(b.get('key'))
            browser_grouped[group] = browser_grouped.get(group, 0) + b.get('value', 0)
        
        # Process daily view data
        for day in summary.get('totalViewsByDay', []):
            row = metadata_fields.copy()
            row['date'] = day.get('key')
            row['views'] = day.get('value')
            row.update(device_grouped)
            row.update(browser_grouped)
            rows.append(row)
    



    # Update header to include all possible keys
    all_keys = set(k for r in rows for k in r.keys())
    header = ['video_id', 'title', 'playbackUrl', 'duration', 'whenUploaded', 'lastViewed', 'whenPublished', 'commentCount', 'score', 'uploadedBy', 'tags', 'date', 'views']
    extra_cols = sorted(k for k in all_keys if k not in header)
    full_header = header + extra_cols
    

    # Write to CSV
    with open(summary_csv, 'w', newline='', encoding='utf-8') as cf:
        writer = csv.DictWriter(cf, fieldnames=full_header)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)
    logging.info("Wrote summary CSV to %s", summary_csv)
    
    # Define source and destination paths
    source_path = f"P:/IMPORTANT/Projects/Vbrick/UBS_TV_{suffix}.csv"
    destination_path = f"Q:/Vbrick/UBS_TV.csv"
    
    # Move the file
    try:
        shutil.move(source_path, destination_path)
        logging.info("File moved successfully from %s to %s", source_path, destination_path)
    except FileNotFoundError:
        logging.warning("The source file was not found: %s", source_path)
    except Exception as e:
        logging.error("An error occurred while moving file: %s", e)

if __name__ == "__main__":
    main()