import pandas as pd
import os
import numpy as np

# --- Configuration ---
INPUT_CSV_PATH = 'usa_map/usa_fitzpatrick_trials_dataset.csv'
OUTPUT_CSV_PATH = 'usa_map/facilities_geocoding_input.csv'

# --- Load the Dataset ---
print(f"Loading data from '{INPUT_CSV_PATH}'...")
try:
    df = pd.read_csv(INPUT_CSV_PATH)
    df.columns = df.columns.str.lower().str.strip()
    print("Dataset loaded successfully.")
except FileNotFoundError:
    print(f"Error: The file '{INPUT_CSV_PATH}' was not found.")
    exit()

# --- Handle nctid Column ---
if 'nctid' not in df.columns:
    print("Warning: 'nctid' column not found in source file. Adding it as a placeholder.")
    df['nctid'] = 'N/A'

# --- Prepare Search Query Column ---
print("\nPreparing search queries and filtering by name suffix...")

# Ensure key columns are strings for consistent checks
for col in ['facility', 'city', 'state', 'zip']:
    if col in df.columns:
        df[col] = df[col].astype(str)

# --- NEW: Define masks for names ending in 'site' or a number ---
# `.str.lower()` makes the 'site' check case-insensitive.
ends_with_site_mask = df['facility'].str.lower().str.endswith('site', na=False)
# `\d+$` is a regular expression that means "ends with one or more digits".
ends_with_number_mask = df['facility'].str.contains(r'\d+$', regex=True, na=False)


# 1. Define conditions that are "fatal" and should always be skipped
fatal_flaw_mask = (
    df[['facility', 'city', 'state']].isnull().any(axis=1) |
    df['facility'].isin(['N/A', 'nan']) |
    df['facility'].str.startswith('Call Suneva', na=False) |
    df['zip'].isin(['00000']) |
    # --- MODIFIED: Use the new suffix masks ---
    ends_with_site_mask |
    ends_with_number_mask
)

# 2. Define condition for a "bad" (but not fatal) zip code
bad_zip_mask = df['zip'].isnull() | df['zip'].isin(['N/A', 'nan'])

# 3. Initialize the search_query column
df['search_query'] = 'SKIP'

# 4. Build queries for rows that are NOT fatally flawed
workable_rows_mask = ~fatal_flaw_mask

# 4a. Build query for workable rows WITH a good zip code
good_zip_mask = workable_rows_mask & ~bad_zip_mask
df.loc[good_zip_mask, 'search_query'] = (
    df.loc[good_zip_mask, 'facility'] + ', ' +
    df.loc[good_zip_mask, 'city'] + ', ' +
    df.loc[good_zip_mask, 'state'] + ' ' +
    df.loc[good_zip_mask, 'zip']
)

# 4b. Build query for workable rows WITHOUT a good zip code
missing_zip_mask = workable_rows_mask & bad_zip_mask
df.loc[missing_zip_mask, 'search_query'] = (
    df.loc[missing_zip_mask, 'facility'] + ', ' +
    df.loc[missing_zip_mask, 'city'] + ', ' +
    df.loc[missing_zip_mask, 'state']
)

# --- Summarize the Results ---
ready_count = (df['search_query'] != 'SKIP').sum()
print(f"Rows ready for API call: {ready_count}")
print(f"Rows marked to SKIP:      {len(df) - ready_count}")

# --- Format the Final DataFrame ---
print("\nFormatting final output file...")

df['latitude'] = np.nan
df['longitude'] = np.nan

final_column_order = [
    'nctid', 'search_query', 'facility', 'state', 'city', 'zip', 'latitude', 'longitude'
]
output_df = df[final_column_order]

# --- Save the Final Data ---
output_dir = os.path.dirname(OUTPUT_CSV_PATH)
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

output_df.to_csv(OUTPUT_CSV_PATH, index=False)
print(f"\nFormatted data ready for geocoding saved to '{OUTPUT_CSV_PATH}'")