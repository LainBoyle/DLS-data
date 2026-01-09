#import
import pandas as pd
import re
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_ROOT = BASE_DIR.parent

#paths
DATA_DIR = DATA_ROOT / "Maryland"
OUTPUT_CSV = DATA_ROOT / "Outputs" / "Maryland.csv"

def infer_category_for_maryland_code(sanction_type, decode2):
    """Categorize Maryland sanction codes into FTP, FTA, road_safety, Child_Support, Other"""
    # Combine both fields for categorization
    text = ""
    if pd.notna(sanction_type):
        text += " " + str(sanction_type).upper()
    if pd.notna(decode2):
        text += " " + str(decode2).upper()
    
    text = text.upper()
    
    # Child support - check BEFORE FTP to separate from other fees
    if "CHILD SUPPORT" in text:
        return "Child_Support"
    
    # Failure to appear (FTA)
    if any(kw in text for kw in ["FAIL TO APPEAR", "FAILURE TO APPEAR", "BENCH WARRANT", "WARRANT"]):
        return "FTA"
    
    # Failure to pay/comply (FTP) - exclude child support
    if any(kw in text for kw in ["FAILURE TO PAY", "FAILED TO PAY", "FTP", 
                                  "INSURANCE", "FINANCIAL", "UNSATISFIED JUDGMENT",
                                  "NON-RESIDENT VIOLATORS", "RECIPROCITY", "VIOLATED RECIPROCITY"]):
        return "FTP"
    
    # Road safety - alcohol/DUI related
    if any(kw in text for kw in ["ALCOHOL", "DUI", "BAC", "CHEMICAL TEST", "ADMIN PER SE", 
                                  "INTERLOCK", "A/R", "ALCOHOL CONTENT"]):
        return "road_safety"
    
    # Road safety - point system (all point accumulation is road safety related)
    # Point accumulation results from traffic violations, so it's road safety
    if "POINT" in text:
        return "road_safety"
    
    # Road safety - other violations
    if any(kw in text for kw in ["RECKLESS", "SPEEDING", "ACCIDENT", "FATAL", "VEHICULAR"]):
        return "road_safety"
    
    # Medical, graduated license, etc. - Other
    if any(kw in text for kw in ["MEDICAL", "GRADUATED LICENSE", "GLS", "PROVISIONAL"]):
        return "Other"
    
    # Default to Other
    return "Other"

# Find all xlsx files in Maryland folder
xlsx_files = sorted([f for f in DATA_DIR.glob("*.xlsx") if f.is_file()])

if not xlsx_files:
    raise FileNotFoundError(f"No .xlsx files found in {DATA_DIR}")

print(f"Found {len(xlsx_files)} Excel file(s) to process")

# Process the raw data file
all_data = []

for xlsx_file in xlsx_files:
    file_name_lower = xlsx_file.name.lower()
    
    # Process the raw data file
    if "raw data" in file_name_lower:
        print(f"Processing {xlsx_file.name}...")
        df = pd.read_excel(xlsx_file)
        
        # Check for required columns
        required_cols = ['Year_Posted', 'Month_Posted']
        if not all(col in df.columns for col in required_cols):
            print(f"Warning: Missing required columns in {xlsx_file.name}")
            continue
        
        # Get sanction type columns
        sanction_col = None
        decode_col = None
        
        if 'SanctionType_Decode' in df.columns:
            sanction_col = 'SanctionType_Decode'
        if 'fstrDecode2' in df.columns:
            decode_col = 'fstrDecode2'
        
        if sanction_col is None and decode_col is None:
            print(f"Warning: No sanction type columns found in {xlsx_file.name}")
            continue
        
        # Create time column (YYYY-MM format)
        df['time'] = df.apply(
            lambda row: f"{int(row['Year_Posted']):04d}-{int(row['Month_Posted']):02d}" 
            if pd.notna(row['Year_Posted']) and pd.notna(row['Month_Posted']) 
            else None, 
            axis=1
        )
        
        # Remove rows with invalid time
        df = df[df['time'].notna()].copy()
        
        # Categorize each row
        df['category'] = df.apply(
            lambda row: infer_category_for_maryland_code(
                row[sanction_col] if sanction_col else None,
                row[decode_col] if decode_col else None
            ),
            axis=1
        )
        
        # Group by time and category, count occurrences
        grouped = df.groupby(['time', 'category'], dropna=False).size().reset_index(name='count')
        
        all_data.append(grouped)

if not all_data:
    raise ValueError("No data was extracted from any files")

# Combine all data
combined_df = pd.concat(all_data, ignore_index=True)

# Group by time and category, summing counts (in case of multiple files)
agg_df = combined_df.groupby(['time', 'category'], dropna=False)['count'].sum().reset_index()

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
print(f"Output saved to {OUTPUT_CSV}")

