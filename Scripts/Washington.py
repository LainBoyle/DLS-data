#import
import pandas as pd
import re
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_ROOT = BASE_DIR.parent

#paths - Washington data is in a different location
WASHINGTON_DATA_DIR = Path(r"C:\Users\elain\.vscode\DLS Project\DLS Project\DLS data\Too Big\Washington")
OUTPUT_CSV = DATA_ROOT / "Outputs" / "Washington.csv"

def infer_category_for_washington_reason(reason):
    """Categorize Washington suspension reasons into FTP, FTA, road_safety, Child_Support, Other"""
    if pd.isna(reason):
        return "Other"
    
    text = str(reason).strip().upper()
    
    # Child support - check BEFORE FTP to separate from other fees
    if "CHILD SUPPORT" in text:
        return "Child_Support"
    
    # Failure to appear (FTA)
    if any(kw in text for kw in ["FAILURE TO APPEAR", "FAIL TO APPEAR", "FAILURE TO ANSWER", "FAIL TO ANSWER", "BENCH WARRANT", "WARRANT", "FTA"]):
        return "FTA"
    
    # Failure to pay/comply (FTP) - exclude child support
    if any(kw in text for kw in ["FAILURE TO MAKE REQUIRED PAYMENT", "FAILED TO PAY", "FTP", 
                                  "UNSATISFIED JUDGMENT", "FINANCIAL RESPONSIBILITY",
                                  "FAILURE TO COMPLY WITH FINANCIAL", "FAILURE TO PAY FOR DAMAGES",
                                  "INSTALLMENT PAYMENT", "FINE AND COSTS"]):
        return "FTP"
    
    # Road safety - alcohol/DUI related
    if any(kw in text for kw in ["ALCOHOL", "DUI", "UNDER THE INFLUENCE", "ADMINISTRATIVE PER SE",
                                  "BAC", "CHEMICAL TEST", "REFUSED TO SUBMIT TO TEST",
                                  "IMPLIED CONSENT", "UNDERAGE ADMINISTRATIVE PER SE", ".02 OR HIGHER"]):
        return "road_safety"
    
    # Road safety - drug related
    if any(kw in text for kw in ["DRUG", "CONTROLLED SUBSTANCE", "UNDER THE INFLUENCE OF DRUGS"]):
        return "road_safety"
    
    # Road safety - serious violations
    if any(kw in text for kw in ["RECKLESS DRIVING", "HABITUAL TRAFFIC OFFENDER", "HABITUAL OFFENDER",
                                  "DRIVING WHILE LICENSE SUSPENDED", "DRIVING WHILE LICENSE REVOKED",
                                  "HIT AND RUN", "FAILURE TO STOP AND RENDER AID", "VEHICULAR ASSAULT",
                                  "VEHICULAR HOMICIDE", "FLEEING OR EVADING POLICE", "ROADBLOCK",
                                  "SPEED CONTEST", "RACING", "ACCUMULATION OF CONVICTIONS", "POINT"]):
        return "road_safety"
    
    # Road safety - using vehicle in felony
    if "USING A MOTOR VEHICLE IN CONNECTION WITH A FELONY" in text:
        return "road_safety"
    
    # Medical, administrative, other - Other
    if any(kw in text for kw in ["MEDICAL", "PHYSICAL OR MENTAL DISABILITY", "RE-EXAM REQUIREMENT",
                                  "ALC/DRUG ASSESSMENT REQUIREMENT", "MEDICAL CERTIFICATION",
                                  "MISREPRESENTATION", "VIOLATE RESTRICTIONS", "IGNITION INTERLOCK REQUIREMENT",
                                  "VIOLATION OF PROBATION", "MINOR IN POSSESSION", "WITHDRAWAL"]):
        return "Other"
    
    # Default to Other
    return "Other"

# Find all xlsx files in Washington folder
xlsx_files = sorted([f for f in WASHINGTON_DATA_DIR.glob("*.xlsx") if f.is_file()])

if not xlsx_files:
    raise FileNotFoundError(f"No .xlsx files found in {WASHINGTON_DATA_DIR}")

print(f"Found {len(xlsx_files)} Excel file(s) to process")

# Process all data sets
all_data = []

for xlsx_file in xlsx_files:
    print(f"Processing {xlsx_file.name}...")
    
    # Read all data sheets (Data Set 1 through Data Set 5)
    xl = pd.ExcelFile(xlsx_file)
    data_sheets = [sheet for sheet in xl.sheet_names if sheet.startswith('Data Set')]
    
    print(f"  Found {len(data_sheets)} data sheet(s)")
    
    for sheet_name in data_sheets:
        print(f"  Processing {sheet_name}...")
        df = pd.read_excel(xlsx_file, sheet_name=sheet_name)
        
        # Check for required columns
        if 'Suspension_Reason' not in df.columns or 'Suspension_Start' not in df.columns:
            print(f"    Warning: Missing required columns in {sheet_name}")
            continue
        
        # Filter out invalid dates (like 9999-12-31 mentioned in notes)
        df = df[df['Suspension_Start'].notna()]
        df = df[df['Suspension_Start'] < pd.Timestamp('2025-01-01')]  # Filter out future dates
        
        # Filter out records where Suspension_Reason looks like a date (data quality issue)
        # These are misaligned records where the reason field contains dates instead of actual reasons
        date_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}')
        if 'Suspension_Reason' in df.columns:
            df = df[~df['Suspension_Reason'].astype(str).str.match(date_pattern, na=False)]
        
        # Extract year and month from Suspension_Start
        df['year'] = df['Suspension_Start'].dt.year
        df['month'] = df['Suspension_Start'].dt.month
        
        # Filter out invalid years
        df = df[(df['year'] >= 1980) & (df['year'] <= 2025)]
        
        # Categorize each record
        df['category'] = df['Suspension_Reason'].apply(infer_category_for_washington_reason)
        
        # Create time column (YYYY-MM format)
        df['time'] = df.apply(lambda row: f"{int(row['year']):04d}-{int(row['month']):02d}", axis=1)
        
        # Select only needed columns
        df_subset = df[['time', 'category']].copy()
        all_data.append(df_subset)
        
        print(f"    Processed {len(df_subset)} records from {sheet_name}")

if not all_data:
    raise ValueError("No data was extracted from any files")

# Combine all data
print("\nCombining all data sets...")
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
print(f"Date range: {pivot_df.index.min()} to {pivot_df.index.max()}")


