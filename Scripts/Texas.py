#import
import pandas as pd
import re
from pathlib import Path
from calendar import month_name

BASE_DIR = Path(__file__).resolve().parent
DATA_ROOT = BASE_DIR.parent

#paths
DATA_DIR = DATA_ROOT / "Texas"
OUTPUT_CSV = DATA_ROOT / "Outputs" / "Texas.csv"

def month_name_to_number(month_str):
    """Convert month name to number (1-12)"""
    if pd.isna(month_str):
        return None
    month_str = str(month_str).strip().lower()
    for i in range(1, 13):
        if month_name[i].lower() == month_str or month_str.startswith(month_name[i].lower()[:3]):
            return i
    return None

def infer_category_for_texas_action(action_str):
    """Categorize Texas enforcement actions into FTP, FTA, road_safety, Other"""
    if pd.isna(action_str):
        return "Other"
    
    action = str(action_str).strip().upper()
    
    # Failure to appear (FTA)
    if any(kw in action for kw in ["FAILURE TO APPEAR", "FAIL TO APPEAR", "FTA", "OUT-OF-STATE FTA"]):
        return "FTA"
    
    # Failure to pay/comply (FTP)
    if any(kw in action for kw in ["FAILURE TO COMPLY", "FAIL TO COMPLY", "FTC", "OUT-OF STATE FTC",
                                    "NO LIABILITY INSURANCE", "INSURANCE", "FINANCIAL RESPONSIBILITY"]):
        return "FTP"
    
    # Road safety - DUI/DWI and alcohol related
    if any(kw in action for kw in ["ALR", "ADMINISTRATIVE LICENSE REVOCATION", "DWI", "DUI", 
                                    "DRIVING WHILE INTOXICATED", "INTOXICATED", "ALCOHOL",
                                    "BAC", "CHEMICAL TEST", "REFUSAL", "UNDER 21"]):
        return "road_safety"
    
    # Road safety - drug related
    if any(kw in action for kw in ["DRUG", "DWI EDUCATION PROGRAM", "DRUG EDUCATION PROGRAM"]):
        return "road_safety"
    
    # Road safety - serious traffic violations
    if any(kw in action for kw in ["SERIOUS TRAFFIC VIOLATIONS", "HABITUAL VIOLATOR", 
                                    "REPEAT OFFENDER", "CRASH"]):
        return "road_safety"
    
    # Road safety - other violations
    if any(kw in action for kw in ["VIOLATE RESTRICTION", "RESTRICTION", "PROHIBITION"]):
        return "road_safety"
    
    # Medical - Other
    if any(kw in action for kw in ["MEDICAL", "INCAPABLE", "TEST REQUIRED"]):
        return "Other"
    
    # Default to Other
    return "Other"

# Find all xlsx files in Texas folder
xlsx_files = sorted([f for f in DATA_DIR.glob("*.xlsx") if f.is_file()])

if not xlsx_files:
    raise FileNotFoundError(f"No .xlsx files found in {DATA_DIR}")

print(f"Found {len(xlsx_files)} Excel file(s) to process")

# Process the enforcement actions file
all_data = []

for xlsx_file in xlsx_files:
    print(f"Processing {xlsx_file.name}...")
    
    # Read the 'EAs & EA Status' sheet which has time-based data
    try:
        df = pd.read_excel(xlsx_file, sheet_name='EAs & EA Status')
    except:
        # Try first sheet if name doesn't match
        df = pd.read_excel(xlsx_file, sheet_name=0)
    
    # Check for required columns
    required_cols = ['Month', 'Year of Enforcement Action', 'Enforcement Action', 'Count']
    if not all(col in df.columns for col in required_cols):
        print(f"Warning: Missing required columns in {xlsx_file.name}")
        print(f"Available columns: {list(df.columns)}")
        continue
    
    # Filter out invalid years (like 9999 or future dates beyond 2025)
    df = df[df['Year of Enforcement Action'].between(2010, 2025)].copy()
    
    # Convert month names to numbers
    df['month_num'] = df['Month'].apply(month_name_to_number)
    
    # Remove rows with invalid months
    df = df[df['month_num'].notna()].copy()
    
    # Create time column (YYYY-MM format)
    df['time'] = df.apply(
        lambda row: f"{int(row['Year of Enforcement Action']):04d}-{int(row['month_num']):02d}",
        axis=1
    )
    
    # Categorize each enforcement action
    df['category'] = df['Enforcement Action'].apply(infer_category_for_texas_action)
    
    # Group by time and category, summing counts
    grouped = df.groupby(['time', 'category'], dropna=False)['Count'].sum().reset_index()
    
    all_data.append(grouped)

if not all_data:
    raise ValueError("No data was extracted from any files")

# Combine all data
combined_df = pd.concat(all_data, ignore_index=True)

# Group by time and category, summing counts (in case of multiple files)
agg_df = combined_df.groupby(['time', 'category'], dropna=False)['Count'].sum().reset_index()

# Pivot to wide format
pivot_df = agg_df.pivot(index='time', columns='category', values='Count').fillna(0)

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
print(f"Output saved to {OUTPUT_CSV}")

