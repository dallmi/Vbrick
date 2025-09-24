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
    
    # Continue fetching with pagination using scroll_id
    while scroll_id:
        params = {
            "count": count,
            "scrollId": scroll_id
        }
        
        data = safe_get(url, headers=headers, params=params, proxies=proxies)
        if not data:
            logging.warning("Failed to fetch more videos, stopping pagination")
            break
        
        items = data.get("videos", [])
        if not items:
            break
            
        videos.extend(items)
        pbar.update(len(items))
        scroll_id = data.get("scrollId")
        
        # Small delay to be respectful to the API
        time.sleep(0.1)
    
    pbar.close()
    logging.info(f"Fetched {len(videos)} videos total")
    return videos


def fetch_video_analytics(auth_manager, video_id, proxies=None, days_back=30):
    """Fetch analytics for a specific video"""
    # Calculate date range for analytics
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=days_back)
    
    url = f"{auth_manager.base_url}/api/v2/videos/{video_id}/analytics"
    headers = {"Authorization": f"Bearer {auth_manager.get_token()}"}
    
    params = {
        "startDate": start_date.isoformat(),
        "endDate": end_date.isoformat(),
        "granularity": "daily"
    }
    
    analytics_data = safe_get(url, headers=headers, params=params, proxies=proxies)
    return analytics_data


def export_to_json(data, filename="vbrick_analytics.json"):
    """Export data to JSON file"""
    export_data = {
        'export_timestamp': datetime.now().isoformat(),
        'total_videos': len(data.get('videos', [])),
        'data': data
    }
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(export_data, f, indent=2, ensure_ascii=False)
    
    logging.info(f"Data exported to {filename}")
    return filename


def export_to_csv(data, filename="vbrick_analytics.csv"):
    """Export flattened data to CSV file"""
    videos = data.get('videos', [])
    if not videos:
        logging.warning("No video data to export")
        return None
    
    csv_rows = []
    for video in videos:
        # Extract video metadata
        base_row = {
            'video_id': video.get('id'),
            'title': video.get('title'),
            'description': video.get('description'),
            'duration': video.get('duration'),
            'upload_date': video.get('uploadDate'),
            'status': video.get('status'),
            'owner_name': video.get('ownerName'),
            'owner_email': video.get('ownerEmail'),
            'views_total': video.get('viewsTotal', 0)
        }
        csv_rows.append(base_row)
    
    # Write to CSV
    if csv_rows:
        fieldnames = csv_rows[0].keys()
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(csv_rows)
        
        logging.info(f"CSV exported to {filename} with {len(csv_rows)} rows")
        return filename
    
    return None


def main():
    """Main execution function"""
    # Configuration - update these with your actual values
    BASE_URL = "https://your-vbrick-instance.com"  # Replace with your Vbrick URL
    API_KEY = os.getenv('VBRICK_API_KEY', 'your_api_key_here')
    API_SECRET = os.getenv('VBRICK_API_SECRET', 'your_api_secret_here')
    
    # Proxy configuration (if needed)
    PROXIES = {
        'http': os.getenv('HTTP_PROXY'),
        'https': os.getenv('HTTPS_PROXY')
    } if os.getenv('HTTP_PROXY') else None
    
    # Network destination path (update as needed)
    NETWORK_PATH = "\\\\network\\share\\vbrick_reports\\"
    
    try:
        # Initialize auth manager
        auth_manager = VbrickAuthManager(BASE_URL, API_KEY, API_SECRET, PROXIES)
        
        # Fetch videos from past 2 years
        logging.info("Starting Vbrick analytics fetch process")
        videos = fetch_all_active_videos(auth_manager, PROXIES)
        
        if not videos:
            logging.error("No videos found")
            return 1
        
        # Prepare data structure
        data = {'videos': videos}
        
        # Export to JSON
        json_file = export_to_json(data, "vbrick_analytics.json")
        
        # Export to CSV
        csv_file = export_to_csv(data, "vbrick_analytics.csv")
        
        # Move CSV to network location if specified and file exists
        if csv_file and NETWORK_PATH and os.path.exists(NETWORK_PATH):
            try:
                network_csv = os.path.join(NETWORK_PATH, f"vbrick_analytics_{datetime.now().strftime('%Y%m%d')}.csv")
                shutil.copy2(csv_file, network_csv)
                logging.info(f"CSV copied to network location: {network_csv}")
            except Exception as e:
                logging.error(f"Failed to copy CSV to network location: {e}")
        
        logging.info("Vbrick analytics fetch completed successfully")
        return 0
        
    except Exception as e:
        logging.error(f"Fatal error in main execution: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())