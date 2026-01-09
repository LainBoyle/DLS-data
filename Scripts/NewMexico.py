#import
import pandas as pd
import re
from pathlib import Path
import pdfplumber

BASE_DIR = Path(__file__).resolve().parent
DATA_ROOT = BASE_DIR.parent

#paths
DATA_DIR = DATA_ROOT / "New Mexico"
OUTPUT_CSV = DATA_ROOT / "Outputs" / "NewMexico.csv"

def infer_category_for_newmexico_action(action_code, description):
    """Categorize New Mexico action codes into FTP, FTA, road_safety, Child_Support, Other"""
    code = str(action_code).strip().upper()
    desc = str(description).strip().upper() if pd.notna(description) else ""
    text = f"{code} {desc}"
    
    # Child support - check BEFORE FTP to separate from other fees
    # Check for various formats: "CHILD SUPPORT", "CHILDSUPPORT", "CHILD-SUPPORT", etc.
    if any(kw in text for kw in ['CHILD SUPPORT', 'CHILDSUPPORT', 'CHILD-SUPPORT', 'CHILD_SUPPORT', 
                                  'CHLD SUPPORT', 'CHLDSUPPORT', 'CHLD SPRT']):
        return "Child_Support"
    
    # Failure to appear (FTA)
    if code == 'D45' or 'FAIL APPEAR' in desc or 'FAILURE TO APPEAR' in desc or 'FTA' in text:
        return "FTA"
    
    # Failure to pay/comply (FTP) - exclude child support
    if code in ['D53', 'D51', 'D56'] or ('FAIL TO PAY' in desc and 'CHILD SUPPORT' not in desc) or ('FAILURE TO PAY' in desc and 'CHILD SUPPORT' not in desc) or ('FTP' in text and 'CHILD SUPPORT' not in text):
        return "FTP"
    
    # DUI/alcohol related - road_safety
    if code in ['A21', 'A12', 'A98', 'A20', 'A11', 'A22', 'A23'] or 'DUI' in desc or 'INTOX' in desc or 'ALCOHOL' in desc or 'BAC' in desc:
        return "road_safety"
    
    # Drug related - road_safety
    if 'DRUG' in desc or 'CONTROLLED SUBSTANCE' in desc:
        return "road_safety"
    
    # Serious violations - road_safety
    if code in ['B25', 'B26', 'B05'] or 'DRIVING WHILE' in desc or 'LEAVE SCENE' in desc or 'HIT AND RUN' in desc:
        return "road_safety"
    
    # Reckless/careless driving - road_safety
    if 'RECKLESS' in desc or 'CARELESS' in desc:
        return "road_safety"
    
    # Default to Other
    return "Other"

def parse_newmexico_suspensions(file_path):
    """Extract suspension/revocation data from New Mexico PDF starting at page 1786"""
    path_obj = Path(file_path) if isinstance(file_path, str) else file_path
    print(f"Processing {path_obj.name}...")
    
    pdf = pdfplumber.open(str(path_obj))
    print(f"  Total pages: {len(pdf.pages)}")
    print(f"  Extracting data from pages 1786-{len(pdf.pages)}...")
    
    # Extract text from pages 1786 onwards
    text = ""
    for i in range(1785, len(pdf.pages)):  # Page 1786 is index 1785
        if (i - 1785 + 1) % 200 == 0:
            print(f"  Processing page {i+1}/{len(pdf.pages)}...")
        text += pdf.pages[i].extract_text() + "\n"
    
    pdf.close()
    
    print("  Parsing suspension/revocation records...")
    lines = text.split('\n')
    
    records = []
    current_description = ""
    
    for line in lines:
        line = line.strip()
        if not line or len(line) < 5:
            continue
        
        # Skip header lines
        if 'Dimensions:' in line or 'AccountType' in line or 'Credential' in line or 'Activity Type' in line:
            continue
        
        # Check if line contains Activity Type
        activity_type = None
        for activity in ['Suspension', 'Revoked', 'Disqualified', 'Cancel', 'Other']:
            if activity in line:
                activity_type = activity
                break
        
        if not activity_type:
            # Might be a continuation line with description
            if line and not line[0].isdigit() and '-' in line:
                # This might be a description line
                current_description = line
            continue
        
        # Parse the line - structure appears to be:
        # Open [Jurisdiction] none [Activity Type] [Action Code] [Description/Conviction] [numbers...] [Count]
        parts = line.split()
        
        # Find Action Code (usually 2-3 characters, alphanumeric)
        action_code = None
        action_code_idx = -1
        for i, part in enumerate(parts):
            # Action codes are typically like A21, D45, B25, etc.
            if re.match(r'^[A-Z][0-9A-Z]{1,2}$', part) and part not in ['AK', 'AL', 'AR', 'AZ', 'none', 'Open']:
                # Make sure it's not a jurisdiction code (2-letter state codes)
                if len(part) == 2 and part in ['AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME', 'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY']:
                    continue
                action_code = part
                action_code_idx = i
                break
        
        if not action_code:
            continue
        
        # Extract description - might be on same line or previous line
        description = current_description
        if action_code_idx + 1 < len(parts):
            # Check if next part starts a description
            remaining = ' '.join(parts[action_code_idx + 1:])
            if '-' in remaining:
                desc_match = re.search(r'-\s*([^-]+?)(?:\s+\d+\s*$|$)', remaining)
                if desc_match:
                    description = desc_match.group(1).strip()
        
        # Find count - last number in the line
        count_match = re.search(r'(\d+)\s*$', line)
        count = int(count_match.group(1)) if count_match else 0
        
        if count > 0:
            records.append({
                'activity_type': activity_type,
                'action_code': action_code,
                'description': description,
                'count': count
            })
        
        # Reset description for next record
        current_description = ""
    
    print(f"  Found {len(records)} suspension/revocation records")
    return records

# Find all PDF files in New Mexico folder
pdf_files = sorted([f for f in DATA_DIR.glob("*.pdf") if f.is_file()])

if not pdf_files:
    raise FileNotFoundError(f"No .pdf files found in {DATA_DIR}")

print(f"Found {len(pdf_files)} PDF file(s) to process")

# Process files
all_records = []

for pdf_file in pdf_files:
    records = parse_newmexico_suspensions(pdf_file)
    if records:
        all_records.extend(records)

if not all_records:
    raise ValueError("No suspension/revocation data was extracted from any files")

# Create DataFrame
df = pd.DataFrame(all_records)

# Categorize each record
df['category'] = df.apply(lambda row: infer_category_for_newmexico_action(row['action_code'], row['description']), axis=1)

# Group by category, summing counts
agg_df = df.groupby('category', dropna=False)['count'].sum().reset_index()

print(f"\nCategory breakdown:")
print(agg_df)

# Since there's no time-series data, create a single totals row
# But first, let's check if we can infer time from the data structure
# The data appears to be aggregated without time information

# Create output with single totals row
output_data = {
    'time': ['total'],
    'FTP': [0],
    'FTA': [0],
    'road_safety': [0],
    'Child_Support': [0],
    'Other': [0],
    'total': [0]
}

# Fill in the categories
for _, row in agg_df.iterrows():
    category = row['category']
    count = int(row['count'])
    if category in output_data:
        output_data[category][0] = count
        output_data['total'][0] += count

output_df = pd.DataFrame(output_data)

# Convert to integers
for col in ['FTP', 'FTA', 'road_safety', 'Child_Support', 'Other', 'total']:
    output_df[col] = pd.to_numeric(output_df[col], errors='coerce').fillna(0).astype(int)

# Ensure output directory exists
OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)

# Save to CSV
output_df.to_csv(OUTPUT_CSV, index=False, float_format='%.0f')
print(f"\nOutput saved to {OUTPUT_CSV}")
print("\nNOTE: The New Mexico data does not contain time-series information.")
print("The output contains a single totals row with aggregated suspension/revocation counts.")
