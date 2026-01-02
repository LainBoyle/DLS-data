#import
import pandas as pd
import re
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_ROOT = BASE_DIR.parent

#paths
DATA_DIR = DATA_ROOT / "Illinois"
OUTPUT_CSV = DATA_ROOT / "Outputs" / "Illinois.csv"

def parse_month_year(month_year_str):
    """Parse MM/YYYY or YYYY-MM format and return (year, month) tuple"""
    if pd.isna(month_year_str):
        return None, None
    s = str(month_year_str).strip()
    # Try MM/YYYY format first
    m = re.match(r"(\d{1,2})/(\d{4})", s)
    if m:
        month, year = int(m.group(1)), int(m.group(2))
        if 1 <= month <= 12:
            return year, month
    # Try YYYY-MM format
    m = re.match(r"(\d{4})-(\d{1,2})", s)
    if m:
        year, month = int(m.group(1)), int(m.group(2))
        if 1 <= month <= 12:
            return year, month
    # Try just year
    m = re.match(r"(\d{4})", s)
    if m:
        return int(m.group(1)), None
    return None, None

def infer_category_for_illinois_code(code_str):
    """Categorize Illinois authority codes into FTP, FTA, road_safety, Other"""
    if pd.isna(code_str):
        return "Other"
    
    code = str(code_str).strip().upper()
    
    # Illinois Vehicle Code section mappings:
    # 6-206 = Failure to pay/appear (FTP/FTA)
    # 6-205 = DUI and alcohol-related (road_safety)
    # 6-113, 6-119, etc. = Various other violations
    
    # Check for 6-206 codes (Failure to Pay/Appear - FTP/FTA)
    # Format can be: 6206, 6-206, 6206A1, 6206A2, etc.
    if "6206" in code or "6-206" in code.replace(" ", ""):
        # 6-206(a)1 through 6-206(a)5 are typically FTA
        if any(x in code for x in ["A1", "A2", "A3", "A4", "A5", "(A)1", "(A)2", "(A)3", "(A)4", "(A)5"]):
            return "FTA"
        # Other 6-206 codes are typically FTP
        return "FTP"
    
    # Check for 6-205 codes (DUI - road_safety)
    if "6205" in code or "6-205" in code.replace(" ", ""):
        return "road_safety"
    
    # Check for 11-501 (DUI - road_safety)
    # Format can be: 11501, 11-501, 1150A1, etc.
    if "11501" in code or "11-501" in code.replace(" ", "") or code.startswith("1150"):
        return "road_safety"
    
    # Explicit text-based checks
    if "DUI" in code or "ALCOHOL" in code or "BAC" in code:
        return "road_safety"
    
    if "FTA" in code or "FAILURE TO APPEAR" in code or "FAIL TO APPEAR" in code:
        return "FTA"
    
    if "FTP" in code or "FAILURE TO PAY" in code or "FAILED TO PAY" in code:
        return "FTP"
    
    if "CHILD SUPPORT" in code or "INSURANCE" in code or "FINANCIAL" in code:
        return "FTP"
    
    if any(x in code for x in ["ACCIDENT", "RECKLESS", "SPEEDING", "HIT AND RUN", 
                                "VEHICULAR", "DRUGS", "CONTROLLED SUBSTANCE"]):
        return "road_safety"
    
    # Default to Other for unknown codes
    return "Other"

def process_monthly_data(file_path):
    """Process the monthly data file (FOIA 9-19-2023)"""
    print(f"Processing monthly data from {file_path.name}...")
    # Read normally - pandas will use first row as column names
    df = pd.read_excel(file_path, sheet_name='Sheet2')
    
    # Row 0 (first data row) actually contains the authority codes
    # Row 1+ contains the actual monthly data
    if len(df) < 2:
        print(f"Warning: Not enough rows in {file_path.name}")
        return pd.DataFrame()
    
    # Extract authority codes from row 0
    authority_codes = {}
    for col_idx, col_name in enumerate(df.columns):
        code_value = df.iloc[0, col_idx]
        if pd.notna(code_value) and str(code_value).strip() != '':
            authority_codes[col_name] = str(code_value).strip()
    
    # Remove row 0 (which has the codes) and reset index
    df = df.iloc[1:].reset_index(drop=True)
    
    # Find the Month/Year column (should be in first column position)
    month_year_col = None
    for col in df.columns:
        col_str = str(col).strip().lower()
        if 'month' in col_str and 'year' in col_str:
            month_year_col = col
            break
    
    # If not found, try first column
    if month_year_col is None and len(df.columns) > 0:
        month_year_col = df.columns[0]
    
    if month_year_col is None:
        print(f"Warning: Could not find Month/Year column in {file_path.name}")
        return pd.DataFrame()
    
    # Get all authority code columns (columns that have codes mapped)
    authority_cols = []
    for col in df.columns:
        if col == month_year_col:
            continue
        # Check if this column has an authority code mapped
        if col in authority_codes:
            code = authority_codes[col]
            if 'total' not in code.lower() and code != 'nan':
                authority_cols.append((col, code))
    
    # Parse month/year and create time column
    time_data = []
    for idx, row in df.iterrows():
        year, month = parse_month_year(row[month_year_col])
        if year and month:
            time_str = f"{year:04d}-{month:02d}"
            time_data.append(time_str)
        elif year:
            # If only year, use first month as placeholder
            time_str = f"{year:04d}-01"
            time_data.append(time_str)
        else:
            time_data.append(None)
    
    # Melt the dataframe to long format
    melted_data = []
    for idx, row in df.iterrows():
        if idx >= len(time_data) or time_data[idx] is None:
            continue
        time_str = time_data[idx]
        for col_name, auth_code in authority_cols:
            count = row[col_name]
            if pd.notna(count) and count != 0:
                try:
                    count_val = float(count)
                    if count_val > 0:
                        melted_data.append({
                            'time': time_str,
                            'authority': auth_code,
                            'count': count_val
                        })
                except (ValueError, TypeError):
                    pass
    
    if not melted_data:
        return pd.DataFrame()
    
    result_df = pd.DataFrame(melted_data)
    return result_df

def process_yearly_data(file_path, exclude_years=None):
    """Process the yearly data file (FOIA Results)
    
    Args:
        file_path: Path to the Excel file
        exclude_years: List of years to exclude (e.g., years with monthly data)
    """
    print(f"Processing yearly data from {file_path.name}...")
    
    if exclude_years is None:
        exclude_years = []
    
    all_data = []
    
    # Process each sheet
    xls = pd.ExcelFile(file_path)
    for sheet_name in xls.sheet_names:
        if 'total' in sheet_name.lower():
            continue
        
        df = pd.read_excel(file_path, sheet_name=sheet_name)
        
        # Find Authority column
        auth_col = None
        for col in df.columns:
            if 'authority' in str(col).lower():
                auth_col = col
                break
        
        if auth_col is None:
            continue
        
        # Get year columns (4-digit years)
        year_cols = []
        for col in df.columns:
            if col == auth_col:
                continue
            try:
                year = int(col)
                if 2000 <= year <= 2030 and year not in exclude_years:
                    year_cols.append((col, year))
            except (ValueError, TypeError):
                pass
        
        # Process each row
        for idx, row in df.iterrows():
            auth_code = row[auth_col]
            if pd.isna(auth_code) or str(auth_code).strip() == '':
                continue
            
            for col, year in year_cols:
                count = row[col]
                if pd.notna(count) and count != 0:
                    try:
                        count_val = float(count)
                        if count_val > 0:
                            # For yearly data, distribute evenly across all 12 months
                            monthly_count = count_val / 12
                            for month in range(1, 13):
                                time_str = f"{year:04d}-{month:02d}"
                                all_data.append({
                                    'time': time_str,
                                    'authority': str(auth_code).strip(),
                                    'count': monthly_count
                                })
                    except (ValueError, TypeError):
                        pass
    
    if not all_data:
        return pd.DataFrame()
    
    result_df = pd.DataFrame(all_data)
    return result_df

# Find all xlsx files in Illinois folder
xlsx_files = sorted([f for f in DATA_DIR.glob("*.xlsx") if f.is_file()])

if not xlsx_files:
    raise FileNotFoundError(f"No .xlsx files found in {DATA_DIR}")

print(f"Found {len(xlsx_files)} Excel file(s) to process")

# Process files - prioritize monthly data
all_data = []
monthly_years = set()

# First pass: process monthly data and identify which years it covers
for xlsx_file in xlsx_files:
    file_name_lower = xlsx_file.name.lower()
    
    # Check if this is the monthly data file
    if "revised" in file_name_lower or "9-19" in file_name_lower:
        monthly_df = process_monthly_data(xlsx_file)
        if not monthly_df.empty:
            # Extract years from monthly data
            monthly_years.update([int(t[:4]) for t in monthly_df['time'].unique() if len(t) >= 4])
            all_data.append(monthly_df)

# Second pass: process yearly data, excluding years with monthly data
for xlsx_file in xlsx_files:
    file_name_lower = xlsx_file.name.lower()
    
    # Check if this is the yearly stats file
    if "sanction stats" in file_name_lower or "2000 to 2023" in file_name_lower:
        yearly_df = process_yearly_data(xlsx_file, exclude_years=list(monthly_years))
        if not yearly_df.empty:
            all_data.append(yearly_df)

if not all_data:
    raise ValueError("No data was extracted from any files")

# Combine all data
combined_df = pd.concat(all_data, ignore_index=True)

# Group by time and authority, summing counts
grouped = combined_df.groupby(['time', 'authority'], dropna=False)['count'].sum().reset_index()

# Categorize each authority code
grouped['category'] = grouped['authority'].apply(infer_category_for_illinois_code)

# Aggregate by time and category
agg_df = grouped.groupby(['time', 'category'], dropna=False)['count'].sum().reset_index()

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
print(f"Output saved to {OUTPUT_CSV}")

