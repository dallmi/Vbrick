# Vbrick Analytics Fetcher

This script fetches video analytics data from the Vbrick API and exports it to JSON and CSV formats.

## Features

- **Authentication Management**: Automatic token refresh and management
- **Video Fetching**: Retrieves all active videos uploaded in the past 2 years
- **Analytics Collection**: Fetches daily view statistics for each video
- **Export Options**: Outputs data to both JSON and CSV formats
- **Error Handling**: Robust retry logic and error handling
- **Proxy Support**: Configurable proxy settings for corporate environments
- **Network Integration**: Automatic copying of results to network shares

## Setup

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Configure your API credentials:
   ```bash
   cp .env.template .env
   # Edit .env with your actual Vbrick API credentials
   ```

3. Update the configuration in the script:
   - `BASE_URL`: Your Vbrick instance URL
   - `API_KEY`: Your Vbrick API key
   - `API_SECRET`: Your Vbrick API secret
   - `NETWORK_PATH`: Network destination path (optional)

## Usage

Run the script:
```bash
python 1_fetch_analytics.py
```

## Output Files

- `vbrick_analytics.json`: Complete data export in JSON format
- `vbrick_analytics.csv`: Flattened data suitable for analysis
- Network copy: Timestamped CSV file copied to network share (if configured)

## CSV Structure

The CSV export includes:
- Video metadata (ID, title, description, duration, upload date, owner)
- Video statistics (total views, likes, dislikes)
- Daily analytics (date, daily views, unique viewers, minutes watched)

## Error Handling

The script includes comprehensive error handling:
- Automatic token refresh
- HTTP request retries with exponential backoff
- Rate limiting to prevent API throttling
- Detailed logging for troubleshooting

## Configuration Options

### Environment Variables

- `VBRICK_API_KEY`: API key for authentication
- `VBRICK_API_SECRET`: API secret for authentication
- `HTTP_PROXY`: HTTP proxy URL (optional)
- `HTTPS_PROXY`: HTTPS proxy URL (optional)

### Script Parameters

- `days_back` for video fetching (default: 730 days = 2 years)
- `days_back` for analytics (default: 30 days)
- Retry attempts and delays are configurable in the `safe_get` function

## Troubleshooting

1. **Authentication Issues**: Verify your API key and secret are correct
2. **Network Errors**: Check proxy configuration if behind corporate firewall
3. **Rate Limiting**: The script includes automatic rate limiting, but you can adjust delays if needed
4. **Large Datasets**: For instances with many videos, consider running during off-peak hours

## Requirements

- Python 3.7+
- requests library
- tqdm library (for progress bars)
- Access to Vbrick API with appropriate permissions
