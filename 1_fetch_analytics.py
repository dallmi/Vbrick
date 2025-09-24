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
            response = requests.post(
                url, 
                headers=headers, 
                json=payload, 
                proxies=self.proxies, 
                timeout=20
            )
            response.raise_for_status()
            
            token_data = response.json()
            self.token = token_data.get('accessToken')
            self.expires_in = token_data.get('expiresIn', 3600)
            self.token_created = time.time()
            
            logging.info(f"Token refreshed successfully, expires in {self.expires_in} seconds")
            
        except Exception as e:
            logging.error(f"Failed to refresh token: {e}")
            raise
    
    def get_auth_headers(self):
        """Get headers with current authorization token"""
        return {
            "Authorization": f"Bearer {self.get_token()}",
            "accept": "application/json",
            "content-type": "application/json"
        }


class VbrickAnalyticsFetcher:
    """Main class for fetching Vbrick analytics data"""
    
    def __init__(self, base_url, api_key, api_secret, proxies=None):
        self.auth_manager = VbrickAuthManager(base_url, api_key, api_secret, proxies)
        self.base_url = base_url.rstrip('/')
        self.proxies = proxies
        self.videos_data = []
        self.analytics_data = []
    
    def fetch_videos(self, days_back=730):
        """Fetch all videos uploaded in the past specified days"""
        logging.info(f"Fetching videos from the past {days_back} days")
        
        # Calculate date range
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=days_back)
        
        url = f"{self.base_url}/api/v2/videos"
        headers = self.auth_manager.get_auth_headers()
        
        params = {
            "uploadedAfter": start_date.isoformat(),
            "uploadedBefore": end_date.isoformat(),
            "status": "active",
            "limit": 100,  # Max per page
            "offset": 0
        }
        
        all_videos = []
        
        while True:
            logging.info(f"Fetching videos, offset: {params['offset']}")
            
            response_data = safe_get(
                url, 
                headers=headers, 
                params=params, 
                proxies=self.proxies
            )
            
            if not response_data:
                break
            
            videos = response_data.get('videos', [])
            if not videos:
                break
            
            all_videos.extend(videos)
            logging.info(f"Retrieved {len(videos)} videos, total: {len(all_videos)}")
            
            # Check if we have more pages
            if len(videos) < params['limit']:
                break
            
            params['offset'] += params['limit']
            time.sleep(0.5)  # Rate limiting
        
        self.videos_data = all_videos
        logging.info(f"Total videos retrieved: {len(all_videos)}")
        return all_videos
    
    def fetch_video_analytics(self, video_id, days_back=30):
        """Fetch analytics for a specific video"""
        url = f"{self.base_url}/api/v2/videos/{video_id}/analytics"
        headers = self.auth_manager.get_auth_headers()
        
        # Calculate date range for analytics
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=days_back)
        
        params = {
            "startDate": start_date.isoformat(),
            "endDate": end_date.isoformat(),
            "granularity": "daily"
        }
        
        analytics_data = safe_get(
            url, 
            headers=headers, 
            params=params, 
            proxies=self.proxies
        )
        
        return analytics_data
    
    def fetch_all_analytics(self, days_back=30):
        """Fetch analytics for all videos"""
        logging.info(f"Fetching analytics for {len(self.videos_data)} videos")
        
        analytics_results = []
        
        for video in tqdm(self.videos_data, desc="Fetching analytics"):
            video_id = video.get('id')
            if not video_id:
                continue
            
            analytics = self.fetch_video_analytics(video_id, days_back)
            if analytics:
                # Combine video metadata with analytics
                combined_data = {
                    'video_metadata': video,
                    'analytics': analytics
                }
                analytics_results.append(combined_data)
            
            time.sleep(0.1)  # Rate limiting
        
        self.analytics_data = analytics_results
        logging.info(f"Analytics retrieved for {len(analytics_results)} videos")
        return analytics_results
    
    def export_to_json(self, filename="vbrick_analytics.json"):
        """Export all data to JSON file"""
        export_data = {
            'export_timestamp': datetime.now().isoformat(),
            'total_videos': len(self.videos_data),
            'videos_with_analytics': len(self.analytics_data),
            'data': self.analytics_data
        }
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(export_data, f, indent=2, ensure_ascii=False)
        
        logging.info(f"Data exported to {filename}")
        return filename
    
    def export_to_csv(self, filename="vbrick_analytics.csv"):
        """Export flattened data to CSV file"""
        if not self.analytics_data:
            logging.warning("No analytics data to export")
            return None
        
        csv_rows = []
        
        for item in self.analytics_data:
            video = item.get('video_metadata', {})
            analytics = item.get('analytics', {})
            
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
                'views_total': video.get('viewsTotal', 0),
                'likes': video.get('likes', 0),
                'dislikes': video.get('dislikes', 0)
            }
            
            # Extract daily analytics if available
            daily_stats = analytics.get('dailyStats', [])
            if daily_stats:
                for day_stat in daily_stats:
                    row = base_row.copy()
                    row.update({
                        'date': day_stat.get('date'),
                        'daily_views': day_stat.get('views', 0),
                        'daily_unique_viewers': day_stat.get('uniqueViewers', 0),
                        'daily_minutes_watched': day_stat.get('minutesWatched', 0)
                    })
                    csv_rows.append(row)
            else:
                # No daily stats, just add the base video info
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
        # Initialize fetcher
        fetcher = VbrickAnalyticsFetcher(BASE_URL, API_KEY, API_SECRET, PROXIES)
        
        # Fetch videos from past 2 years
        logging.info("Starting Vbrick analytics fetch process")
        videos = fetcher.fetch_videos(days_back=730)
        
        if not videos:
            logging.error("No videos found")
            return 1
        
        # Fetch analytics for past 30 days
        analytics = fetcher.fetch_all_analytics(days_back=30)
        
        # Export to JSON
        json_file = fetcher.export_to_json("vbrick_analytics.json")
        
        # Export to CSV
        csv_file = fetcher.export_to_csv("vbrick_analytics.csv")
        
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