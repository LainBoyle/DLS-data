#import
import pandas as pd
import re
from pathlib import Path
from datetime import datetime, timedelta
import pdfplumber

BASE_DIR = Path(__file__).resolve().parent
DATA_ROOT = BASE_DIR.parent

#paths
DATA_DIR = DATA_ROOT / "Virginia"
OUTPUT_CSV = DATA_ROOT / "Outputs" / "Virginia.csv"

def infer_category_for_virginia_code(order_code, description):
    """Categorize Virginia order codes into FTP, FTA, road_safety, Other"""
    code = str(order_code).strip().upper()
    desc = str(description).strip().upper() if pd.notna(description) else ""
    text = f"{code} {desc}"
    
    # Failure to appear (FTA)
    if any(kw in text for kw in ["FAIL TO APPEAR", "FAILURE TO APPEAR", "FTA", "CE02", "JG02", "DEFAULT JUDGMENT"]):
        return "FTA"
    
    # Failure to pay/comply (FTP)
    if any(kw in text for kw in ["FAILURE TO PAY", "FAILED TO PAY", "FAIL TO PAY", "FTP", 
                                  "FAIL PAY", "JA01", "CV91", "FM01", "FM03", "JG01",
                                  "UNSATISFIED JUDGMENT", "JUDGMENT", "FINE", "COST", "FEE"]):
        return "FTP"
    
    # Road safety - DUI/alcohol related
    if any(kw in text for kw in ["DUI", "ADMIN PER SE", "AP01", "AP55", "INTOX", "ALCOHOL",
                                  "DRIVE INFLU", "DR AFTER CONSUME", "DR CONSUME", "CV12",
                                  "CV57", "CV58", "CV59", "CV61", "BLOOD TEST", "CV25", "CV29"]):
        return "road_safety"
    
    # Road safety - drug related
    if any(kw in text for kw in ["DRUG", "CONTROLLED SUBSTANCE"]):
        return "road_safety"
    
    # Road safety - serious violations
    if any(kw in text for kw in ["MANSLAUGHTER", "CV62", "MAIMING", "CV61", "FELONY",
                                  "EXCESSIVE PT", "DI04", "POINT ACCUMULATION"]):
        return "road_safety"
    
    # Road safety - other violations
    if any(kw in text for kw in ["RECKLESS", "SPEEDING", "ACCIDENT", "CRASH", "VIOLATION"]):
        return "road_safety"
    
    # Medical - Other
    if any(kw in text for kw in ["MEDICAL", "MD", "CD40", "CD41", "CD42", "CD43", "CD44", "CD45", "CD48"]):
        return "Other"
    
    # Insurance - FTP
    if any(kw in text for kw in ["INSURANCE", "UNINS", "IM01", "IM02", "IM03", "IM04", "CV01"]):
        return "FTP"
    
    # Default to Other
    return "Other"

def parse_virginia_pdf(file_path):
    """Extract data from Virginia PDF"""
    path_obj = Path(file_path) if isinstance(file_path, str) else file_path
    print(f"Processing {path_obj.name}...")
    
    pdf = pdfplumber.open(str(path_obj))
    all_data = []
    
    # Extract date range from first page
    first_page_text = pdf.pages[0].extract_text()
    date_match = re.search(r'FROM:\s*(\d{2})/(\d{2})/(\d{2})\s*TO:\s*(\d{2})/(\d{2})/(\d{2})', first_page_text)
    
    if not date_match:
        print("Warning: Could not find date range")
        pdf.close()
        return []
    
    from_month, from_day, from_year = date_match.groups()[:3]
    to_month, to_day, to_year = date_match.groups()[3:]
    from_year_int = 2000 + int(from_year)
    to_year_int = 2000 + int(to_year)
    
    from_date = datetime(from_year_int, int(from_month), int(from_day))
    to_date = datetime(to_year_int, int(to_month), int(to_day))
    
    # Extract text from all pages and parse
    full_text = ""
    for page in pdf.pages:
        full_text += page.extract_text() + "\n"
    
    # Parse lines that look like order records
    lines = full_text.split('\n')
    
    for line in lines:
        line = line.strip()
        if not line or len(line) < 10:
            continue
        
        # Look for lines that start with order codes
        # Pattern: CODE DESCRIPTION ISSUED COMPLIED PERCENT OUTSTANDING
        match = re.match(r'^([A-Z0-9]{2,5})\s+([A-Z][A-Z\s/-]+?)\s+(\d{1,3}(?:,\d{3})*|\d+)\s+(\d{1,3}(?:,\d{3})*|\d+)\s+([\d.]+)\s+(\d{1,3}(?:,\d{3})*|\d+)', line)
        if match:
            order_code, description, issued, complied, percent, outstanding = match.groups()
            try:
                count = int(issued.replace(',', ''))
                if count > 0:
                    # Categorize
                    category = infer_category_for_virginia_code(order_code, description)
                    
                    # Calculate number of months in the date range
                    months = []
                    current = from_date.replace(day=1)  # Start from first of month
                    while current <= to_date:
                        months.append(current.strftime("%Y-%m"))
                        # Move to next month
                        if current.month == 12:
                            current = current.replace(year=current.year + 1, month=1)
                        else:
                            current = current.replace(month=current.month + 1)
                    
                    # Distribute count evenly across months
                    if months:
                        monthly_count = count / len(months)
                        for month in months:
                            all_data.append({
                                'time': month,
                                'order_code': order_code.strip(),
                                'description': description.strip(),
                                'category': category,
                                'count': monthly_count
                            })
            except ValueError:
                pass
    
    pdf.close()
    print(f"Extracted {len(all_data)} order records")
    return all_data

# Find all PDF files in Virginia folder
pdf_files = sorted([f for f in DATA_DIR.glob("*.pdf") if f.is_file()])

if not pdf_files:
    raise FileNotFoundError(f"No .pdf files found in {DATA_DIR}")

print(f"Found {len(pdf_files)} PDF file(s) to process")

# Process files
all_data = []

for pdf_file in pdf_files:
    data = parse_virginia_pdf(pdf_file)
    if data:
        all_data.extend(data)

if not all_data:
    raise ValueError("No data was extracted from any files")

# Combine all data
combined_df = pd.DataFrame(all_data)

# Group by time and category, summing counts
agg_df = combined_df.groupby(['time', 'category'], dropna=False)['count'].sum().reset_index()

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





