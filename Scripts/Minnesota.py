#import
import pandas as pd
import re
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parent
DATA_ROOT = BASE_DIR.parent

#paths - Minnesota data is in a different location
MINNESOTA_DATA_DIR = Path(r"C:\Users\elain\.vscode\DLS Project\DLS Project\DLS data\Too Big\Minnesota")
OUTPUT_CSV = DATA_ROOT / "Outputs" / "Minnesota.csv"

def parse_minnesota_date(date_str):
    """Parse Minnesota date format (YYYY-MM-DD HH:MM:SS or YYYY-MM-DD)"""
    if pd.isna(date_str) or date_str == '' or str(date_str).strip() == '':
        return None
    
    date_str = str(date_str).strip()
    
    # Handle invalid dates
    if date_str == '9999-12-31' or date_str == '0000-00-00' or date_str == '0':
        return None
    
    try:
        # Try parsing as YYYY-MM-DD or YYYY-MM-DD HH:MM:SS
        if '-' in date_str:
            # Extract just the date part
            date_part = date_str.split()[0] if ' ' in date_str else date_str
            parts = date_part.split('-')
            if len(parts) >= 3:
                year = int(parts[0])
                month = int(parts[1])
                day = int(parts[2])
                
                # Validate
                if year < 1970 or year > 2025 or month < 1 or month > 12 or day < 1 or day > 31:
                    return None
                
                return pd.Timestamp(year, month, day)
    except (ValueError, IndexError):
        return None
    
    return None

def infer_category_for_minnesota_code(sanction_code):
    """Categorize Minnesota sanction codes into FTP, FTA, road_safety, Other"""
    if pd.isna(sanction_code):
        return "Other"
    
    code = str(sanction_code).strip().upper()
    
    # Remove "Fast." prefix if present
    if code.startswith('FAST.'):
        code = code[5:]
    elif code.startswith('FAST'):
        code = code[4:]
    
    # Failure to appear (FTA)
    # SD45 = Failure to Appear (most common FTA code)
    if code in ['SD45', 'SA12']:  # SA12 might also be FTA related
        return "FTA"
    
    # Failure to pay/comply (FTP)
    # SD51 = Failure to Pay Fine
    # SD53 = Failure to Pay Child Support
    # SD56 = Failure to Pay
    if code in ['SD51', 'SD53', 'SD56']:
        return "FTP"
    
    # Road safety - DUI/alcohol related
    # SA90, SA98, SA21, SA22, SA33, SA91, SA95, SA11, SA61 = DUI/alcohol related
    # SB20, SB25, SB26, SB51, SB22, SB74 = DUI related
    if code in ['SA90', 'SA98', 'SA21', 'SA22', 'SA33', 'SA91', 'SA95', 'SA11', 'SA61',
                'SB20', 'SB25', 'SB26', 'SB51', 'SB22', 'SB74']:
        return "road_safety"
    
    # Road safety - other violations
    # SD35, SD36, SD39, SD27, SD29, SD16 = Various traffic violations
    # SW00, SW01, SW72 = Traffic violations
    # SU01, SU03, SU04, SU06 = Traffic violations
    if code in ['SD35', 'SD36', 'SD39', 'SD27', 'SD29', 'SD16',
                'SW00', 'SW01', 'SW72',
                'SU01', 'SU03', 'SU04', 'SU06']:
        return "road_safety"
    
    # Conversion codes - likely administrative
    if code.startswith('CONVERSION'):
        return "Other"
    
    # Default to Other
    return "Other"

# Find CSV files in Minnesota folder
csv_files = sorted([f for f in MINNESOTA_DATA_DIR.glob("*.csv") if f.is_file()])

if not csv_files:
    raise FileNotFoundError(f"No .csv files found in {MINNESOTA_DATA_DIR}")

print(f"Found {len(csv_files)} CSV file(s) to process")

# Process files
all_data = []

for csv_file in csv_files:
    print(f"Processing {csv_file.name}...")
    print(f"  This is a large file, reading in chunks...")
    
    # Read the file in chunks to handle large size
    chunk_size = 1000000
    chunk_num = 0
    
    for chunk in pd.read_csv(csv_file, encoding='utf-16', sep=',', quotechar='"', 
                             chunksize=chunk_size, low_memory=False):
        chunk_num += 1
        if chunk_num % 5 == 0:
            print(f"  Processing chunk {chunk_num}...")
        
        # Check for required columns
        if 'Sanction Code' not in chunk.columns or 'fdtmRestraintCommence' not in chunk.columns:
            print(f"    Warning: Missing required columns in {csv_file.name}")
            print(f"    Available columns: {list(chunk.columns)}")
            continue
        
        # Parse dates
        chunk['restraint_start'] = chunk['fdtmRestraintCommence'].apply(parse_minnesota_date)
        
        # Filter out invalid dates
        chunk = chunk[chunk['restraint_start'].notna()]
        chunk = chunk[chunk['restraint_start'] < pd.Timestamp('2025-01-01')]
        chunk = chunk[chunk['restraint_start'] >= pd.Timestamp('1970-01-01')]
        
        # Extract year and month
        chunk['year'] = chunk['restraint_start'].dt.year
        chunk['month'] = chunk['restraint_start'].dt.month
        
        # Filter out invalid years
        chunk = chunk[(chunk['year'] >= 1970) & (chunk['year'] <= 2025)]
        
        # Categorize each record
        chunk['category'] = chunk['Sanction Code'].apply(infer_category_for_minnesota_code)
        
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


