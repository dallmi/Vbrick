# Vbrick Analytics Pipeline

A comprehensive data processing pipeline that extracts, enriches, merges, and normalizes video analytics data from Vbrick. This suite combines video usage statistics with webcast attendance data to provide detailed insights into content performance across different dimensions such as geography, browsers, and devices.

## Overview

The Vbrick Analytics Pipeline is designed to help organizations understand how their video content is being consumed across different audiences and platforms. It pulls data from multiple Vbrick APIs, applies AI-driven categorization, and produces analysis-ready datasets in two distinct formats depending on your reporting needs.

### What This Pipeline Does

1. **Fetches Video Analytics**: Collects detailed viewing statistics for all active videos
2. **Processes Webcast Data**: Retrieves live webcast attendance and engagement metrics  
3. **AI-Powered Categorization**: Automatically categorizes content using machine learning
4. **Data Integration**: Merges video and webcast data into unified datasets
5. **Flexible Output**: Produces both aggregated summaries and normalized data for different analysis needs

## Pipeline Components

### 1. Video Analytics Collection (`01_fetch_analytics.py`)
**Purpose**: Extracts comprehensive video viewing data from Vbrick API

**Key Functions**:
- Authenticates with Vbrick API using secure token management
- Fetches all active videos from the past 2 years
- Collects daily analytics including views, unique viewers, and watch time
- Groups data by device type (Desktop, Mobile) and browser (Chrome, Edge, etc.)

**Output**: `vbrick_analytics.csv`

| videoId | title | description | duration | views_total | browser_Chrome | browser_Edge | device_Desktop | device_Mobile |
|---------|-------|-------------|----------|-------------|----------------|--------------|----------------|---------------|
| v123 | Product Demo | Demo of new features | 1800 | 450 | 300 | 150 | 400 | 50 |
| v124 | Training Video | Employee onboarding | 3600 | 820 | 500 | 320 | 700 | 120 |

### 2. Webcast Data Processing (`02_Webcast.py`)
**Purpose**: Retrieves and enriches live webcast attendance data with AI categorization

**Key Functions**:
- Fetches webcast events and attendee sessions
- Maps attendee locations to geographical zones (APAC, Americas, EMEA, Swiss)
- Categorizes content using TF-IDF vectorization and K-means clustering
- Groups attendance by browser, device, and geographical zone

**Output**: `webcast_summary.csv`

| eventId | title | startDate | attendeeTotal | zone_APAC | zone_Americas | zone_EMEA | category | subcategory |
|---------|-------|-----------|---------------|-----------|---------------|-----------|----------|-------------|
| w789 | Global All-Hands | 2024-09-15 | 1250 | 300 | 600 | 350 | Corporate | Company Updates |
| w790 | Product Launch | 2024-09-20 | 850 | 200 | 400 | 250 | Product | Announcements |

### 3. Data Integration (`03_MergeWebcastVideo.py`)
**Purpose**: Combines video and webcast data into a unified dataset for comprehensive analysis

**Key Functions**:
- Matches webcast events with their corresponding recorded videos
- Merges attendance data with video viewing statistics
- Preserves both live event metrics and on-demand video consumption
- Creates comprehensive content performance overview

**Output**: `merged_webcast_video_summary.csv`

| id | title | eventURL | attendeeTotal | zone_APAC | zone_Americas | v_views | v_Chrome | v_Desktop | category |
|----|-------|----------|---------------|-----------|---------------|---------|----------|-----------|----------|
| w789 | Global All-Hands | https://vbrick.com/event/w789 | 1250 | 300 | 600 | 450 | 300 | 400 | Corporate |
| w790 | Product Launch | https://vbrick.com/event/w790 | 850 | 200 | 400 | 820 | 500 | 700 | Product |

### 4. Data Normalization (`04_NormalizedMergedWebcastVideo.py`)
**Purpose**: Transforms merged data into a normalized format optimized for dimensional analysis

**Key Functions**:
- Flattens multi-dimensional data into individual records per dimension
- Creates separate rows for each zone, browser, and device combination
- Enables easy filtering and analysis by specific dimensions
- Maintains data relationships while optimizing for analytics tools

**Output**: `normalized_webcast_video_summary.csv`

| id | title | zone | webcast_browser | video_device | attendeeTotal | v_views | category |
|----|-------|------|-----------------|--------------|---------------|---------|----------|
| w789 | Global All-Hands | APAC | NULL | NULL | 300 | NULL | Corporate |
| w789 | Global All-Hands | Americas | NULL | NULL | 600 | NULL | Corporate |
| w789 | Global All-Hands | NULL | NULL | Desktop | NULL | 400 | Corporate |
| w790 | Product Launch | APAC | NULL | NULL | 200 | NULL | Product |

## Real-World Use Cases

### Use Case 1: Regional Content Performance Analysis
**Scenario**: Understanding which content resonates in different global regions

**Data Source**: Normalized output
```sql
SELECT zone, category, SUM(attendeeTotal) as total_attendance
FROM normalized_data 
WHERE zone IS NOT NULL
GROUP BY zone, category
ORDER BY total_attendance DESC
```

**Business Insight**: "Product announcements have 3x higher attendance in APAC region compared to EMEA"

### Use Case 2: Technology Adoption Tracking
**Scenario**: Monitoring browser and device usage trends for technical planning

**Data Source**: Merged output  
```python
# Calculate mobile adoption rate
mobile_rate = df['device_Mobile'].sum() / (df['device_Desktop'].sum() + df['device_Mobile'].sum())
print(f"Mobile viewing accounts for {mobile_rate:.1%} of total consumption")
```

**Business Insight**: "Mobile viewing has increased to 25% of total consumption, indicating need for mobile-optimized content"

### Use Case 3: Content Lifecycle Analysis
**Scenario**: Comparing live webcast engagement vs. on-demand video consumption

**Data Source**: Merged output
```python
# Compare live vs. recorded engagement
live_engagement = df['attendeeTotal'].mean()
recorded_engagement = df['v_views'].mean()
engagement_ratio = recorded_engagement / live_engagement

print(f"On-demand videos receive {engagement_ratio:.1f}x more views than live attendance")
```

**Business Insight**: "Recorded content extends reach by 2.3x, justifying investment in high-quality recording infrastructure"

## Setup and Configuration

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure API Access
```bash
cp .env.template .env
# Edit .env with your Vbrick credentials:
# VBRICK_API_KEY=your_api_key
# VBRICK_API_SECRET=your_api_secret
```

### 3. Run the Pipeline
Execute scripts in sequence for full pipeline:
```bash
python 01_fetch_analytics.py    # Collect video data
python 02_Webcast.py            # Process webcast data  
python 03_MergeWebcastVideo.py  # Combine datasets
python 04_NormalizedMergedWebcastVideo.py  # Create normalized output
```

## Output File Selection Guide

**Use Merged Output (`merged_webcast_video_summary.csv`) when**:
- Creating executive dashboards with aggregated metrics
- Analyzing overall content performance trends
- Building reports that need totals and summaries
- Working with BI tools that prefer wide-format data

**Use Normalized Output (`normalized_webcast_video_summary.csv`) when**:
- Performing dimensional analysis (by zone, browser, device)
- Creating detailed breakdowns and drill-down reports
- Using analytics tools that prefer long-format data
- Building visualizations that show distribution across categories

## System Requirements

- Python 3.7+
- Libraries: pandas, requests, scikit-learn, tqdm
- Access to Vbrick API with analytics permissions
- Network access to Q:/ drive (or modify paths as needed)

## Troubleshooting

**Authentication Issues**: Verify API credentials in `.env` file
**Large Datasets**: Scripts include progress bars and can handle thousands of records
**Network Paths**: Modify file paths in scripts if not using Q:/ drive structure
**AI Categorization**: Categories are automatically generated; review and adjust clustering parameters if needed
