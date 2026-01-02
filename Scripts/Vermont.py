#import
import pandas as pd
import re
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parent
DATA_ROOT = BASE_DIR.parent

#paths - Vermont data is in a different location
VERMONT_DATA_DIR = Path(r"C:\Users\elain\.vscode\DLS Project\DLS Project\DLS data\Too Big\Vermont")
OUTPUT_CSV = DATA_ROOT / "Outputs" / "Vermont.csv"

def parse_vermont_date(date_str):
    """Parse Vermont date format (YYMMDD or YYYYMMDD)"""
    if pd.isna(date_str) or date_str == '' or str(date_str).strip() == '':
        return None
    
    date_str = str(date_str).strip()
    
    # Handle invalid dates
    if date_str == '000000' or date_str == '0' or len(date_str) < 6:
        return None
    
    try:
        # Try YYMMDD format (6 digits)
        if len(date_str) == 6:
            year = int(date_str[:2])
            month = int(date_str[2:4])
            day = int(date_str[4:6])
            
            # Convert 2-digit year to 4-digit (assume 1900s for years > 50, 2000s for <= 50)
            if year > 50:
                year += 1900
            else:
                year += 2000
            
            # Validate month and day
            if month < 1 or month > 12 or day < 1 or day > 31:
                return None
            
            return pd.Timestamp(year, month, day)
        
        # Try YYYYMMDD format (8 digits)
        elif len(date_str) == 8:
            year = int(date_str[:4])
            month = int(date_str[2:4])
            day = int(date_str[4:8])
            
            # Validate month and day
            if month < 1 or month > 12 or day < 1 or day > 31:
                return None
            
            return pd.Timestamp(year, month, day)
    except (ValueError, IndexError):
        return None
    
    return None

def infer_category_for_vermont_code(suspension_code):
    """Categorize Vermont suspension codes into FTP, FTA, road_safety, Other"""
    if pd.isna(suspension_code):
        return "Other"
    
    code = str(suspension_code).strip().upper()
    
    # Failure to appear (FTA)
    # FAF = Failure to Appear (most common FTA code)
    # FAM = Failure to Appear (another variant)
    # FAD = Failure to Appear (another variant)
    if code in ['FAF', 'FAM', 'FAD']:
        return "FTA"
    
    # Failure to pay/comply (FTP)
    # FAP = Failure to Pay
    # UJ = Unsatisfied Judgment
    # MFC = likely related to financial compliance
    if code in ['FAP', 'UJ', 'MFC']:
        return "FTP"
    
    # Road safety - DUI/alcohol related
    # DW1, DW2, DW3 = Driving While... (likely DUI related)
    # CA1, CA2 = likely alcohol-related
    # DA1, DA2 = likely drug/alcohol related
    # CT1 = Chemical Test refusal
    # 16C = likely related to alcohol (16C is a common DUI code)
    # 21A = likely alcohol-related
    if code in ['DW1', 'DW2', 'DW3', 'CA1', 'CA2', 'DA1', 'DA2', 'CT1', '16C', '21A']:
        return "road_safety"
    
    # Road safety - points and traffic violations
    # PTS = Points (point accumulation)
    # PTC = Points (another variant)
    if code in ['PTS', 'PTC']:
        return "road_safety"
    
    # Road safety - other violations
    # CNC = likely criminal/negligent
    # CIV = likely civil violation
    # CIG = likely related to violations
    # CM1 = likely related to violations
    # CX1 = likely related to violations
    if code in ['CNC', 'CIV', 'CIG', 'CM1', 'CX1']:
        return "road_safety"
    
    # Other categories
    # IP = Instruction Permit (administrative)
    # PC = likely administrative
    # PD = likely administrative
    # CI2 = likely administrative
    # JRP = likely administrative
    # NNY = likely administrative
    # PU = likely administrative
    # DLS = Driving License Suspended (could be road_safety, but might be administrative)
    # ESL = likely administrative
    # Default to Other
    return "Other"

# Find the text file
txt_files = sorted([f for f in VERMONT_DATA_DIR.glob("*.txt") if f.is_file()])

if not txt_files:
    raise FileNotFoundError(f"No .txt files found in {VERMONT_DATA_DIR}")

print(f"Found {len(txt_files)} text file(s) to process")

# Process files
all_data = []

for txt_file in txt_files:
    print(f"Processing {txt_file.name}...")
    print(f"  This is a large file, reading in chunks...")
    
    # Read the file in chunks to handle large size
    chunk_size = 1000000
    chunk_num = 0
    
    for chunk in pd.read_csv(txt_file, sep='|', chunksize=chunk_size, low_memory=False):
        chunk_num += 1
        if chunk_num % 10 == 0:
            print(f"  Processing chunk {chunk_num}...")
        
        # Check for required columns
        if 'SUSPENSION_CODE' not in chunk.columns or 'EFFECTIVE_DATE' not in chunk.columns:
            print(f"    Warning: Missing required columns")
            continue
        
        # Parse dates
        chunk['effective_date'] = chunk['EFFECTIVE_DATE'].apply(parse_vermont_date)
        
        # Filter out invalid dates
        chunk = chunk[chunk['effective_date'].notna()]
        chunk = chunk[chunk['effective_date'] < pd.Timestamp('2025-01-01')]
        chunk = chunk[chunk['effective_date'] >= pd.Timestamp('1980-01-01')]
        
        # Extract year and month
        chunk['year'] = chunk['effective_date'].dt.year
        chunk['month'] = chunk['effective_date'].dt.month
        
        # Filter out invalid years
        chunk = chunk[(chunk['year'] >= 1980) & (chunk['year'] <= 2025)]
        
        # Categorize each record
        chunk['category'] = chunk['SUSPENSION_CODE'].apply(infer_category_for_vermont_code)
        
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

