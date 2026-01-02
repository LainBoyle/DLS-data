#import
import pandas as pd
import re
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parent
DATA_ROOT = BASE_DIR.parent

#paths - Oregon data is in a different location
OREGON_DATA_DIR = Path(r"C:\Users\elain\.vscode\DLS Project\DLS Project\DLS data\Too Big\Oregon")
OUTPUT_CSV = DATA_ROOT / "Outputs" / "Oregon.csv"

def parse_oregon_date(date_str):
    """Parse Oregon date format (YYYY-MM-DD)"""
    if pd.isna(date_str) or date_str == '' or str(date_str).strip() == '':
        return None
    
    date_str = str(date_str).strip()
    
    # Handle invalid dates
    if date_str == '9999-12-31' or date_str == '0000-00-00' or date_str == '0':
        return None
    
    try:
        # Try parsing as YYYY-MM-DD
        if '-' in date_str and len(date_str) >= 10:
            parts = date_str.split('-')
            if len(parts) == 3:
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

def infer_category_for_oregon(sanction_type, native_code_literal):
    """Categorize Oregon sanctions into FTP, FTA, road_safety, Other"""
    # Combine both fields for categorization
    text = ""
    if pd.notna(sanction_type):
        text += " " + str(sanction_type).strip().upper()
    if pd.notna(native_code_literal):
        text += " " + str(native_code_literal).strip().upper()
    
    text = text.upper()
    
    # Failure to appear (FTA)
    # FTAFTC = Failure to Appear/Failure to Comply (combined category)
    # But we'll check the native code literal for more specific info
    if "F APPEAR" in text or "FAILURE TO APPEAR" in text or "FAIL TO APPEAR" in text:
        return "FTA"
    
    # If FTAFTC but no specific FTA indicator, it might be FTP (see below)
    
    # Failure to pay/comply (FTP)
    # FTAFTC with "F COMPLY" = Failure to Comply (FTP)
    if "F COMPLY" in text or "FAILURE TO PAY" in text or "FAIL PAY" in text or "FPAYTAX" in text:
        return "FTP"
    
    # Child support
    if "CHILDSUPPORT" in text or "CHLD SPRT" in text or "CHILD SUPPORT" in text:
        return "FTP"
    
    # Unsatisfied judgment
    if "UJUDGMNT" in text or "UNSATISFIED JUDGMENT" in text or "UNSATISFIED JUDGMENT" in text:
        return "FTP"
    
    # Insurance-related - FTP (financial responsibility)
    if any(kw in text for kw in ["SR22V", "SR22I", "SR22", "SR22A", "SR22H", "INSURE", 
                                  "MANDATORY INSURANCE", "DR UNINSUR", "DR UNINS", 
                                  "OWNER UNINSURED", "ACCIDENT UNINSURED"]):
        return "FTP"
    
    # Road safety - DUI/alcohol related
    if any(kw in text for kw in ["DUII", "IMPLIEDCONSENT", "IMPLIED CONSENT", "FAILED BREATH TEST",
                                  "REFUSED BREATH TEST", "REFUSED URINE TEST", "FAILED BLOOD TEST",
                                  "DRVIMP", "DRIVING IMPAIRED"]):
        return "road_safety"
    
    # Road safety - accidents and traffic crimes
    if any(kw in text for kw in ["ACCIDENT", "TRAFFICCRIME", "TRAFFIC CRIME", "RECK DR", "RECKLESS",
                                  "RECK END MV", "SERIOUSACCIM", "DR F RPT AC"]):
        return "road_safety"
    
    # Road safety - habitual offenders and major violations
    if any(kw in text for kw in ["HABIT", "HABITUAL OFFENDER", "MAJOR", "MAJORSUS", "CDLMJR"]):
        return "road_safety"
    
    # Road safety - fleeing/eluding
    if any(kw in text for kw in ["FL/AT ELUDE", "ELUDE", "FLEEING"]):
        return "road_safety"
    
    # Road safety - other violations
    if any(kw in text for kw in ["ASSAULT MV", "CRIM MIS MV", "CO/MV FLNY", "UNAUTH USE"]):
        return "road_safety"
    
    # FTAFTC without specific indicator - default to FTP (more common interpretation)
    # Note: FTAFTC is a combined category, but "F COMPLY" is more common than "F APPEAR"
    if "FTAFTC" in text and "F APPEAR" not in text:
        return "FTP"
    
    # Other categories
    # Administrative, medical, etc.
    if any(kw in text for kw in ["CANCEL", "FALSEAPPSUSP", "FALSEAPPCANC", "FALSE APPLICATION",
                                  "FRAUD", "ATRISK", "ATRISKTEST", "ATRISKDNY", "AT-RISK",
                                  "PERMIT", "DISPAYMENT", "DISPMT", "STATEHOSPITAL", "HSPVIO",
                                  "HSPBAR", "CDLMEDQUAL", "CDLSRS", "CDLOOSO", "CDLRRGC",
                                  "CDPFRD", "IIDVIOLATE", "IIDINDEF", "IIDSUSP", "INTERLOCK",
                                  "IID DEINSTALL", "OOSSUS", "OOSICINDEF", "OOS CONVICTION",
                                  "OOS IMPLIED CONSENT", "CRTODR", "COURT ORDER", "ADMIN",
                                  "LEGACY", "N/ENT DL", "N/ENT CDP", "PRF REQ UNTL",
                                  "ADLTCONVAR", "ADLTACCAPA", "CO/FPDD", "CO/UN USE",
                                  "CO/RECK DR", "CO/DUII", "1 CO/DUII", "* CO/DUII",
                                  "FLS INFO PLC", "F PLC DL/C", "C/FL/A/ELD", "HARDSHIP VIOLATION",
                                  "FAIL BIOMETRIC CHECK", "INCIDENT"]):
        return "Other"
    
    # Default to Other
    return "Other"

# Find data files
txt_files = sorted([f for f in OREGON_DATA_DIR.glob("*.txt") if f.is_file()])
xlsx_files = sorted([f for f in OREGON_DATA_DIR.glob("*.xlsx") if f.is_file()])

if not txt_files and not xlsx_files:
    raise FileNotFoundError(f"No data files found in {OREGON_DATA_DIR}")

print(f"Found {len(txt_files)} text file(s) and {len(xlsx_files)} Excel file(s) to process")

# Process files
all_data = []

# Process text files
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
        if 'Restraint Start' not in chunk.columns or 'Sanction Type' not in chunk.columns:
            print(f"    Warning: Missing required columns")
            continue
        
        # Parse dates
        chunk['restraint_start'] = chunk['Restraint Start'].apply(parse_oregon_date)
        
        # Filter out invalid dates
        chunk = chunk[chunk['restraint_start'].notna()]
        chunk = chunk[chunk['restraint_start'] < pd.Timestamp('2025-01-01')]
        chunk = chunk[chunk['restraint_start'] >= pd.Timestamp('1970-01-01')]
        
        # Extract year and month
        chunk['year'] = chunk['restraint_start'].dt.year
        chunk['month'] = chunk['restraint_start'].dt.month
        
        # Filter out invalid years
        chunk = chunk[(chunk['year'] >= 1970) & (chunk['year'] <= 2025)]
        
        # Get native code literal if available
        native_code_col = '(Native Code) Literal'
        if native_code_col not in chunk.columns:
            native_code_col = None
        
        # Categorize each record
        if native_code_col:
            chunk['category'] = chunk.apply(
                lambda row: infer_category_for_oregon(
                    row.get('Sanction Type'),
                    row.get(native_code_col)
                ),
                axis=1
            )
        else:
            chunk['category'] = chunk.apply(
                lambda row: infer_category_for_oregon(
                    row.get('Sanction Type'),
                    None
                ),
                axis=1
            )
        
        # Create time column (YYYY-MM format)
        chunk['time'] = chunk.apply(lambda row: f"{int(row['year']):04d}-{int(row['month']):02d}", axis=1)
        
        # Select only needed columns
        chunk_subset = chunk[['time', 'category']].copy()
        all_data.append(chunk_subset)
    
    print(f"  Processed {chunk_num} chunks")

# Process Excel files (if any)
for xlsx_file in xlsx_files:
    print(f"Processing {xlsx_file.name}...")
    
    # Read Excel file
    xl = pd.ExcelFile(xlsx_file)
    for sheet_name in xl.sheet_names:
        print(f"  Processing sheet: {sheet_name}")
        df = pd.read_excel(xlsx_file, sheet_name=sheet_name)
        
        # Check if it's pipe-delimited in a single column
        if len(df.columns) == 1:
            # Split by pipe delimiter
            first_col = df.columns[0]
            if '^' in str(df[first_col].iloc[0]):
                # Split the column
                df_split = df[first_col].str.split('^', expand=True)
                # Try to infer headers from first row
                if len(df_split) > 0:
                    # Skip first row if it's headers
                    headers = df_split.iloc[0].tolist()
                    df_split = df_split.iloc[1:]
                    df_split.columns = headers[:len(df_split.columns)]
                    df = df_split
        
        # Check for required columns (might be named differently)
        date_col = None
        sanction_type_col = None
        native_code_col = None
        
        for col in df.columns:
            col_lower = str(col).lower()
            if 'restraint' in col_lower and 'start' in col_lower:
                date_col = col
            if 'sanction' in col_lower and 'type' in col_lower:
                sanction_type_col = col
            if 'native' in col_lower or 'literal' in col_lower:
                native_code_col = col
        
        if not date_col or not sanction_type_col:
            print(f"    Warning: Missing required columns in {sheet_name}")
            continue
        
        # Parse dates
        df['restraint_start'] = df[date_col].apply(parse_oregon_date)
        
        # Filter out invalid dates
        df = df[df['restraint_start'].notna()]
        if len(df) == 0:
            print(f"    No valid dates found in {sheet_name}")
            continue
        
        df = df[df['restraint_start'] < pd.Timestamp('2025-01-01')]
        df = df[df['restraint_start'] >= pd.Timestamp('1970-01-01')]
        
        if len(df) == 0:
            print(f"    No dates in valid range in {sheet_name}")
            continue
        
        # Extract year and month
        df['year'] = df['restraint_start'].dt.year
        df['month'] = df['restraint_start'].dt.month
        
        # Filter out invalid years
        df = df[(df['year'] >= 1970) & (df['year'] <= 2025)]
        
        # Categorize each record
        if native_code_col:
            df['category'] = df.apply(
                lambda row: infer_category_for_oregon(
                    row.get(sanction_type_col),
                    row.get(native_code_col)
                ),
                axis=1
            )
        else:
            df['category'] = df.apply(
                lambda row: infer_category_for_oregon(
                    row.get(sanction_type_col),
                    None
                ),
                axis=1
            )
        
        # Create time column (YYYY-MM format)
        df['time'] = df.apply(lambda row: f"{int(row['year']):04d}-{int(row['month']):02d}", axis=1)
        
        # Select only needed columns
        df_subset = df[['time', 'category']].copy()
        all_data.append(df_subset)
        
        print(f"    Processed {len(df_subset)} records from {sheet_name}")

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

