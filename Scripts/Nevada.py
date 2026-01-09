#import
import pandas as pd
import re
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_ROOT = BASE_DIR.parent

#paths
DATA_DIR = DATA_ROOT / "Nevada"
OUTPUT_CSV = DATA_ROOT / "Outputs" / "Nevada.csv"

def parse_nevada_report(file_path):
    """Parse the Nevada fixed-width formatted report file"""
    print(f"Processing {file_path.name}...")
    
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        content = f.read()
    
    # Extract the as-of date
    date_match = re.search(r'AS-OF DATE\s*:\s*(\d{4})-(\d{2})-(\d{2})', content)
    if date_match:
        year, month, day = date_match.groups()
        as_of_date = f"{year}-{month}"
    else:
        # Default to file date if not found
        as_of_date = "2023-06"
    
    print(f"Report date: {as_of_date}")
    print("WARNING: This is a snapshot report, not time-series data.")
    print("The file only contains license status counts by class, not suspension reasons.")
    print("Cannot categorize into FTP/FTA/road_safety/Other without detailed suspension reason data.")
    
    # Extract suspended and revoked counts
    lines = content.split('\n')
    suspended_count = 0
    revoked_count = 0
    
    in_suspended_section = False
    in_revoked_section = False
    
    for line in lines:
        line_upper = line.upper()
        
        # Check if we're entering a section
        if 'SUSPENDED' in line_upper and 'LICENSE STATUS' not in line_upper:
            in_suspended_section = True
            in_revoked_section = False
            continue
        elif 'REVOKED' in line_upper and 'LICENSE STATUS' not in line_upper:
            in_revoked_section = True
            in_suspended_section = False
            continue
        elif any(status in line_upper for status in ['VALID', 'EXPIRED', 'SURRENDER', 'CLEARED', 'DECEASED', 'CANCELLED', 'DENIED', 'OTHER', 'PENDING']):
            in_suspended_section = False
            in_revoked_section = False
            continue
        
        # Extract numbers from lines in suspended/revoked sections
        if in_suspended_section or in_revoked_section:
            # Look for the TOTAL column (last number in the line)
            numbers = re.findall(r'\d+', line)
            if numbers:
                try:
                    # The last number is usually the total
                    total = int(numbers[-1])
                    if in_suspended_section:
                        suspended_count += total
                    elif in_revoked_section:
                        revoked_count += total
                except ValueError:
                    pass
    
    # Since we don't have suspension reasons, we can't categorize properly
    # We'll create a single row with the as-of date and put all suspensions in "Other"
    # This is not ideal but reflects the data limitation
    
    data = {
        'time': [as_of_date],
        'FTP': [0],  # No data available
        'FTA': [0],  # No data available
        'road_safety': [0],  # No data available
        'Child_Support': [0],  # No data available
        'Other': [suspended_count + revoked_count],  # All suspensions/revocations
        'total': [suspended_count + revoked_count]
    }
    
    df = pd.DataFrame(data)
    
    # Add totals row
    totals_row = pd.DataFrame([{
        'time': 'total',
        'FTP': 0,
        'FTA': 0,
        'road_safety': 0,
        'Child_Support': 0,
        'Other': suspended_count + revoked_count,
        'total': suspended_count + revoked_count
    }])
    
    df = pd.concat([df, totals_row], ignore_index=True)
    
    print(f"Extracted {suspended_count} suspended and {revoked_count} revoked licenses")
    print("NOTE: Without suspension reason data, all counts are in 'Other' category")
    
    return df

# Find all txt files in Nevada folder
txt_files = sorted([f for f in DATA_DIR.glob("*.txt") if f.is_file()])

if not txt_files:
    raise FileNotFoundError(f"No .txt files found in {DATA_DIR}")

print(f"Found {len(txt_files)} text file(s) to process")

# Process files
all_data = []

for txt_file in txt_files:
    df = parse_nevada_report(txt_file)
    all_data.append(df)

if not all_data:
    raise ValueError("No data was extracted from any files")

# Combine all data
combined_df = pd.concat(all_data, ignore_index=True)

# Ensure all categories are present
categories = ["FTP", "FTA", "road_safety", "Child_Support", "Other"]
for cat in categories:
    if cat not in combined_df.columns:
        combined_df[cat] = 0

# Convert to integers
for col in categories + ['total']:
    combined_df[col] = pd.to_numeric(combined_df[col], errors='coerce').fillna(0).astype(int)

# Ensure output directory exists
OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)

# Save to CSV
combined_df.to_csv(OUTPUT_CSV, index=False, float_format='%.0f')
print(f"\nOutput saved to {OUTPUT_CSV}")
print("\nIMPORTANT LIMITATION:")
print("The Nevada data file is a snapshot report without time-series data or suspension reasons.")
print("All suspensions/revocations are categorized as 'Other' since detailed reason data is not available.")
print("This output does not match the format of other states' data due to data limitations.")





