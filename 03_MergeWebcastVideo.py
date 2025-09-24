import pandas as pd
import shutil

"""
This script merges video analytics data from UBS_TV.csv with webcast metadata from webcast_summary.csv 
to create a unified summary report. It performs the following steps:

1. Aggregates daily video view data by video_id, summing total views and retaining metadata 
   to have only one row per video.
2. Filters and renames relevant video columns for clarity and consistency.
3. Merges the video data with webcast data using a left join on webcast 'vodId' and video 'video_id'.
4. Replaces missing values with 0 and formats numeric values for regional display (e.g., using commas).
5. Exports the merged dataset to merged_webcast_video_summary.csv and moves it to a shared network location.

This script is used to enrich webcast reporting with detailed video performance metrics.
"""


# Load the UBS_TV.csv file
df_video = pd.read_csv('Q:/Vbrick/UBS_TV.csv')

# Drop the "date" column since it's used only for daily breakdowns
df_video = df_video.drop(columns=['date'])

# Dynamically identify all columns except 'video_id' and 'views'
non_summed_columns = [col for col in df_video.columns if col not in ['video_id', 'views']]

# Aggregate: sum views, take first occurrence of all other columns
aggregated_df = df_video.groupby("video_id").agg({
    "views": 'sum',
    **{col: "first" for col in non_summed_columns}
}).reset_index()

# Load the webcast_summary.csv file
df_webcast = pd.read_csv('Q:/Vbrick/webcast_summary.csv')

# Identify numeric columns to keep, excluding specific ones
excluded_columns = {'video_id', 'commentCount', 'score'}
numeric_columns = aggregated_df.select_dtypes(include='number').columns
columns_to_keep = ['video_id', 'duration','lastViewed','whenPublished', 'views'] 
columns_to_keep += [col for col in numeric_columns if col not in excluded_columns and col not in columns_to_keep]

# Filter and rename video columns
aggregated_df = aggregated_df[columns_to_keep]
aggregated_df = aggregated_df.rename(columns={col: f'v_{col}' for col in aggregated_df.columns if col != 'video_id'})

# Merge using a left join
merged_df = df_webcast.merge(aggregated_df, how='left', left_on='vodId', right_on='video_id')

# Drop the redundant join key from video data
merged_df = merged_df.drop(columns=['video_id'])

# Replace NaN values with 0
merged_df = merged_df.fillna(0)

# Format numeric values: convert float to int where appropriate and replace '.' with ',' for regional formatting
def format_number(val):
    if isinstance(val, float) and val.is_integer():
        return str(int(val)).replace('.', ',')
    elif isinstance(val, (float, int)):
        return str(val).replace('.', ',')
    return val

# Apply formatting to merged dataframe
merged_df = merged_df.apply(lambda col: col.map(format_number) if col.dtype != "object" else col)


# Save the result
merged_df.to_csv('merged_webcast_video_summary.csv', index=False)

# Define source and destination paths
source_path = "P:/IMPORTANT/Projects/Vbrick/merged_webcast_video_summary.csv"
destination_path = "Q:/Vbrick/merged_webcast_video_summary.csv"

# Move the file
try:
    shutil.move(source_path, destination_path)
    print("File moved successfully to Q:/Vbrick/merged_webcast_video_summary.csv")
except FileNotFoundError:
    print("The source file was not found.")
except Exception as e:
    print(f"An error occurred: {e}")

print("Join complete. Output saved to 'merged_webcast_video_summary.csv'.")