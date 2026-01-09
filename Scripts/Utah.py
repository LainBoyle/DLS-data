#import
import pandas as pd
import re
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parent
DATA_ROOT = BASE_DIR.parent

#paths - Utah data is in a different location
UTAH_DATA_DIR = Path(r"C:\Users\elain\.vscode\DLS Project\DLS Project\DLS data\Too Big\Utah")
OUTPUT_CSV = DATA_ROOT / "Outputs" / "Utah.csv"

def parse_utah_date(date_str):
    """Parse Utah date format (M/D/YYYY or MM/DD/YYYY)"""
    if pd.isna(date_str) or date_str == '' or str(date_str).strip() == '':
        return None
    
    date_str = str(date_str).strip()
    
    try:
        # Try parsing as M/D/YYYY or MM/DD/YYYY
        # Handle various formats
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

def infer_category_for_utah_description(description):
    """Categorize Utah action descriptions into FTP, FTA, road_safety, Child_Support, Other"""
    if pd.isna(description):
        return "Other"
    
    text = str(description).strip().upper()
    
    # Child support - check BEFORE FTP to separate from other fees
    # Note: "NO CHILD SUPPORT" might mean failure to pay, but we'll include it
    if "CHILD SUPPORT" in text:
        return "Child_Support"
    
    # Failure to appear (FTA)
    # Note: After July 2012, Utah merged FTA cases into "FAIL TO COMPLY"
    # We can't distinguish them, so FTA is undercounted after that date
    if any(kw in text for kw in ["FAIL APPEAR", "FAILURE TO APPEAR", "FAIL TO APPEAR", "FTA"]):
        return "FTA"
    
    # Failure to pay/comply (FTP) - exclude child support
    # Note: "FAIL TO COMPLY" after July 2012 includes both FTP and FTA cases,
    # but we categorize it as FTP since we can't distinguish them
    if any(kw in text for kw in ["FAIL TO COMPLY", "FAILURE TO PAY", "UNSATISFIED DAMAGES", 
                                  "UNSATISFIED JUDGEMENT", "UNSATISFIED JUDGMENT"]):
        return "FTP"
    
    # Insurance-related - FTP (financial responsibility)
    if any(kw in text for kw in ["NO VEHICLE INSURANCE", "DRIVING W/O INSURANCE", "NO PROOF OF INSUR",
                                  "PROOF OF INS", "INSURANCE", "INS-SR22"]):
        return "FTP"
    
    # Road safety - DUI/alcohol related
    if any(kw in text for kw in ["DUI", "PERSE ARREST", "PER SE", "REFUSAL TO SUBMIT", 
                                  "JUVENILE ALCOHOL", "METABOLITE", "DRINKING AND DRIVING",
                                  "ALCOHOL", "BAC"]):
        return "road_safety"
    
    # Road safety - drug related
    if any(kw in text for kw in ["CTRL SUBSTANCE", "CONTROLLED SUBSTANCE", "DRUG"]):
        return "road_safety"
    
    # Road safety - serious violations
    if any(kw in text for kw in ["DRIVING ON REVOCATION", "DRIVING WHILE SUSPENDED", "DRIVE WHILE DENIED",
                                  "POINTS ACCUMULATION", "RECKLESS DRIVING", "FLEEING", "EVADE ARREST",
                                  "HIT & RUN", "HIT AND RUN", "LEAVE ACCID SCENE", "AUTO HOMICIDE",
                                  "SPEEDING", "ALC/DRUG RECKLESS"]):
        return "road_safety"
    
    # Other categories
    # Administrative, medical, etc.
    if any(kw in text for kw in ["DL TESTS REQUIRED", "ALTERED LICENSE", "FALSE DL APPLICATION",
                                  "NOT A DROP", "NON-ACD WITHDRAWAL", "COURT ORDERED SUSPENSION",
                                  "MISREPRESENTATION", "SHOW/USE IMPRPR DL", "PHYSCL/MENTL DISABILITY",
                                  "EXP/NO REGISTRATION", "PARENTAL WITHDRAWAL", "REHAB REQUIRED",
                                  "VIOL LIMITED LICENSE", "FAIL FILE MEDICAL", "LIQUOR TO MINOR"]):
        return "Other"
    
    # Default to Other
    return "Other"

# Find the text file
txt_files = sorted([f for f in UTAH_DATA_DIR.glob("*.txt") if f.is_file()])

if not txt_files:
    raise FileNotFoundError(f"No .txt files found in {UTAH_DATA_DIR}")

print(f"Found {len(txt_files)} text file(s) to process")

# Process files
all_data = []

for txt_file in txt_files:
    print(f"Processing {txt_file.name}...")
    print(f"  This is a large file, reading in chunks...")
    
    # Read the file in chunks to handle large size
    chunk_size = 1000000
    chunk_num = 0
    
    for chunk in pd.read_csv(txt_file, chunksize=chunk_size, low_memory=False):
        chunk_num += 1
        if chunk_num % 5 == 0:
            print(f"  Processing chunk {chunk_num}...")
        
        # Check for required columns
        if 'DESCRIPTION' not in chunk.columns or 'ACTION_DATE' not in chunk.columns:
            print(f"    Warning: Missing required columns")
            continue
        
        # Parse dates
        chunk['action_date'] = chunk['ACTION_DATE'].apply(parse_utah_date)
        
        # Filter out invalid dates
        chunk = chunk[chunk['action_date'].notna()]
        chunk = chunk[chunk['action_date'] < pd.Timestamp('2025-01-01')]
        chunk = chunk[chunk['action_date'] >= pd.Timestamp('1970-01-01')]
        
        # Extract year and month
        chunk['year'] = chunk['action_date'].dt.year
        chunk['month'] = chunk['action_date'].dt.month
        
        # Filter out invalid years
        chunk = chunk[(chunk['year'] >= 1970) & (chunk['year'] <= 2025)]
        
        # Categorize each record
        chunk['category'] = chunk['DESCRIPTION'].apply(infer_category_for_utah_description)
        
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
categories = ["FTP", "FTA", "road_safety", "Child_Support", "Other"]
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

