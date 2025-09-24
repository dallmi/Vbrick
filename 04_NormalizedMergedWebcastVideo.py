"""
Script: Normalize Webcast Video Summary Data

Purpose:
This script processes a merged webcast video summary CSV file and transforms it into a normalized format
suitable for analysis by dimension (e.g., zone, browser, device).

Step-by-Step Process:

1. ** Import Libraries**:
   - Import pandas for data manipulation.

2. **Load Data**:
   - Read the input CSV file from the path: Q:/Vbrick/merged_webcast_video_summary.csv

3. **Define Metadata Columns**:
    - Specify the key metadata fields to retain for each webcast record.

4. **Define Dimension Configurations**:
   - Set up configurations for each dimension (zone, browser, device) including:
        - The output column name.
        - The source columns to check.
        - The labels to assign.
        - The metric column to populate (e.g., attendeeTotal, v_views).

5. **Transform Data**:
   - Iterate through each row of the dataset.
   - For each dimension, check if the value is non-zero.
   - If so, create a new record with:
     - Metadata fields.
     - One-hot encoded dimension label.
     - Corresponding metric value.

Example 1: Webcast Zone Breakdown
---------------------------------
Original Row:
id | title | zone_APAC | zone_America | zone_Swiss | attendeeTotal
1  | AI Talk | 5         | 10           | 3          | 18

Transformed Rows:
id | title   | zone    | attendeeTotal
1  | AI Talk | APAC    | 5
1  | AI Talk | America | 10
1  | AI Talk | Swiss   | 3

Example 2: Video Browser Views
Original Row:
id | title          | v_browser_Chrome | v_browser_Edge | v_browser_Other | v_views
1  | Data Deep Dive | 100              | 50             | 25              | 175

Transformed Rows:
id | title          | browser      | v_views
1  | Data Deep Dive | Chrome       | 100
1  | Data Deep Dive | Edge         | 50
1  | Data Deep Dive | Other        | 25

6. **Normalize Output**:
   - Collect all transformed records into a new DataFrame.

7. **Export Result**:
   - Save the normalized data to normalized_webcast_video_summary.csv.

Output: A flattened CSV file where each row represents a single dimension-metric combination for a webcast.
"""



import pandas as pd

# Load the merged webcast dataset
df = pd.read_csv("Q:/Vbrick/merged_webcast_video_summary.csv")

# Define metadata columns to retain
metadata_cols = [
    "id", "title", "description", "date", "duration_minutes", "speaker", "category"
]

# Define dimension configurations
dimension_configs = [
    {
        "dimension_column": "zone",
        "columns": ["zone_APAC", "zone_America", "zone_Swiss"],
        "labels": ["APAC", "America", "Swiss"],
        "metric_column": "attendeeTotal"
    },
    {
        "dimension_column": "webcast_browser", 
        "columns": ["wc_browser_Chrome", "wc_browser_Edge", "wc_browser_Other"],
        "labels": ["Chrome", "Edge", "Other"],
        "metric_column": "attendeeTotal"
    },
    {
        "dimension_column": "webcast_device",
        "columns": ["wc_device_Desktop", "wc_device_Mobile", "wc_device_Other"],
        "labels": ["Desktop", "Mobile", "Other"],
        "metric_column": "attendeeTotal"
    },
    {
        "dimension_column": "video_browser",
        "columns": ["v_browser_Chrome", "v_browser_Edge", "v_browser_Other"],
        "labels": ["Chrome", "Edge", "Other"],
        "metric_column": "v_views"
    },
    {
        "dimension_column": "video_device",
        "columns": ["v_device_Desktop", "v_device_Mobile", "v_device_Other"],
        "labels": ["Desktop", "Mobile", "Other"],
        "metric_column": "v_views"
    }
]

# Collect transformed rows
records = []

for index, row in df.iterrows():
    # Create base record with metadata
    base = row[metadata_cols].to_dict()
    
    for config in dimension_configs:
        for col, label in zip(config["columns"], config["labels"]):
            # Check if this dimension has non-zero values
            if pd.notna(row[col]) and row[col] > 0:
                record = base.copy()
                
                # Initialize all dimension columns to None
                record.update({
                    "zone": None,
                    "webcast_browser": None,
                    "webcast_device": None,
                    "video_browser": None,
                    "video_device": None,
                    "attendeeTotal": None,
                    "v_views": None
                })
                
                # Set the current dimension value
                record[config["dimension_column"]] = label
                record[config["metric_column"]] = row[col]
                
                records.append(record)

# Create normalized DataFrame
normalized_df = pd.DataFrame(records)

# Export to CSV
normalized_df.to_csv("Q:/Vbrick/normalized_webcast_video_summary.csv", index=False)

print(f"Normalized data exported with {len(normalized_df)} records")
print(f"Original dataset had {len(df)} webcasts")
print("\nSample of normalized data:")
print(normalized_df.head(10))