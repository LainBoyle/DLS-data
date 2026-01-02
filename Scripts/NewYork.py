#import
import pandas as pd
import re
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parent
DATA_ROOT = BASE_DIR.parent

#paths - New York data is in a different location
NEWYORK_DATA_DIR = Path(r"C:\Users\elain\.vscode\DLS Project\DLS Project\DLS data\Too Big\New York")
OUTPUT_CSV = DATA_ROOT / "Outputs" / "NewYork.csv"

def parse_newyork_date(date_str):
    """Parse New York date format (M/D/YYYY or MM/DD/YYYY)"""
    if pd.isna(date_str) or date_str == '' or str(date_str).strip() == '':
        return None
    
    date_str = str(date_str).strip()
    
    try:
        # Try parsing as M/D/YYYY or MM/DD/YYYY
        if '/' in date_str:
            parts = date_str.split('/')
            if len(parts) == 3:
                month = int(parts[0])
                day = int(parts[1])
                year = int(parts[2])
                
                # Validate
                if month < 1 or month > 12 or day < 1 or day > 31:
                    return None
                
                return pd.Timestamp(year, month, day)
    except (ValueError, IndexError):
        return None
    
    return None

def infer_category_for_newyork_reason(reason):
    """Categorize New York suspension reasons into FTP, FTA, road_safety, Other"""
    if pd.isna(reason):
        return "Other"
    
    text = str(reason).strip().upper()
    
    # Failure to appear (FTA)
    if any(kw in text for kw in ["FAILURE TO ANSWER", "FAIL TO ANSWER", "FAILURE TO APPEAR", 
                                  "FAIL TO APPEAR", "APPEARANCE", "SUMMONS"]):
        return "FTA"
    
    # Failure to pay/comply (FTP)
    if any(kw in text for kw in ["FAILURE TO PAY", "FAIL TO PAY", "FINE", "DISHONORED CHECK",
                                  "POST BOND"]):
        return "FTP"
    
    # Road safety - would need specific codes/descriptions
    # New York data appears to be mostly FTP/FTA based on the sample
    # If there are DUI or other road safety reasons, they would go here
    
    # Default to Other
    return "Other"

# Find all CSV files in New York folder
csv_files = sorted([f for f in NEWYORK_DATA_DIR.glob("DMV_SANCTIONS_*.csv") if f.is_file()])

if not csv_files:
    raise FileNotFoundError(f"No DMV_SANCTIONS CSV files found in {NEWYORK_DATA_DIR}")

print(f"Found {len(csv_files)} CSV file(s) to process")

# Process files
all_data = []

for csv_file in csv_files:
    print(f"Processing {csv_file.name}...")
    print(f"  This is a large file, reading in chunks...")
    
    # Read the file in chunks to handle large size
    chunk_size = 1000000
    chunk_num = 0
    
    for chunk in pd.read_csv(csv_file, chunksize=chunk_size, low_memory=False):
        chunk_num += 1
        if chunk_num % 5 == 0:
            print(f"  Processing chunk {chunk_num}...")
        
        # Check for required columns
        if 'REASON' not in chunk.columns or 'EFFECTIVE' not in chunk.columns:
            print(f"    Warning: Missing required columns in {csv_file.name}")
            continue
        
        # Parse dates
        chunk['effective_date'] = chunk['EFFECTIVE'].apply(parse_newyork_date)
        
        # Filter out invalid dates
        chunk = chunk[chunk['effective_date'].notna()]
        chunk = chunk[chunk['effective_date'] < pd.Timestamp('2025-01-01')]
        chunk = chunk[chunk['effective_date'] >= pd.Timestamp('1970-01-01')]
        
        # Extract year and month
        chunk['year'] = chunk['effective_date'].dt.year
        chunk['month'] = chunk['effective_date'].dt.month
        
        # Filter out invalid years
        chunk = chunk[(chunk['year'] >= 1970) & (chunk['year'] <= 2025)]
        
        # Categorize each record
        chunk['category'] = chunk['REASON'].apply(infer_category_for_newyork_reason)
        
        # Create time column (YYYY-MM format)
        chunk['time'] = chunk.apply(lambda row: f"{int(row['year']):04d}-{int(row['month']):02d}", axis=1)
        
        # Select only needed columns
        chunk_subset = chunk[['time', 'category']].copy()
        all_data.append(chunk_subset)
    
    print(f"  Processed {chunk_num} chunks")

if not all_data:
    raise ValueError("No data was extracted from any files")

# Combine all data
print("\nCombining all data...")
combined_df = pd.concat(all_data, ignore_index=True)

print(f"Total records: {len(combined_df)}")

# Group by time and category, counting records
agg_df = combined_df.groupby(['time', 'category'], dropna=False).size().reset_index(name='count')

# Pivot to wide format
pivot_df = agg_df.pivot(index='time', columns='category', values='count').fillna(0)

# Ensure all categories are present
categories = ["FTP", "FTA", "road_safety", "Other"]
for cat in categories:
    if cat not in pivot_df.columns:
        pivot_df[cat] = 0

# Reorder columns
pivot_df = pivot_df[categories]

# Add total column
pivot_df['total'] = pivot_df[categories].sum(axis=1)

# Sort by time
pivot_df = pivot_df.sort_index()

# Add totals row
totals_row = pivot_df[categories + ['total']].sum().to_frame().T
totals_row.index = ['total']

# Combine
output_df = pd.concat([pivot_df, totals_row])
output_df.insert(0, 'time', output_df.index)
output_df = output_df.reset_index(drop=True)

# Convert to integers
for col in categories + ['total']:
    output_df[col] = pd.to_numeric(output_df[col], errors='coerce').fillna(0).astype(int)

# Ensure output directory exists
OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)

# Save to CSV
output_df.to_csv(OUTPUT_CSV, index=False, float_format='%.0f')
print(f"\nOutput saved to {OUTPUT_CSV}")
if len(pivot_df) > 0:
    print(f"Date range: {pivot_df.index.min()} to {pivot_df.index.max()}")


