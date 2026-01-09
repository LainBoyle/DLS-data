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
    """Categorize Texas enforcement actions into FTP, FTA, road_safety, Child_Support, Other"""
    if pd.isna(action_str):
        return "Other"
    
    action = str(action_str).strip().upper()
    
    # Child support - check FIRST to separate from other fees
    if "CHILD SUPPORT" in action or "DELINQUENT CHILD SUPPORT" in action:
        return "Child_Support"
    
    # Failure to appear (FTA) - check before FTP
    if any(kw in action for kw in ["FAILURE TO APPEAR", "FAIL TO APPEAR", "FTA", 
                                    "OUT-OF-STATE FTA", "OUT OF STATE FTA"]):
        return "FTA"
    
    # Failure to pay/comply (FTP) - insurance, surcharges, judgments, installment agreements
    if any(kw in action for kw in [
        # Insurance related
        "NO LIABILITY INSURANCE", "CANCELLED INSURANCE", "INSURANCE", 
        "FINANCIAL RESPONSIBILITY", "SR SUSPENSION",
        # Financial obligations
        "SURCHARGE DUE", "DEFAULT INSTALLMENT AGREEMENT", "DEFAULTED INSTALLMENT",
        "LIABILITY JUDGMENT", "OUT OF STATE JUDGMENT", "OUT-OF STATE JUDGMENT",
        # Failure to comply/pay
        "FAILURE TO COMPLY", "FAIL TO COMPLY", "FTC", "OUT-OF STATE FTC", "OUT OF STATE FTC",
        "OUT-OF-STATE FTP", "OUT OF STATE FTP",
        # DHS overpayment
        "DHS OVERPAYMENT",
        # Denied renewal for financial reasons
        "DENIED RENEWAL OUT-OF STATE FTC", "DENIED RENEWAL OUT-OF-STATE FTP"
    ]):
        return "FTP"
    
    # Road safety - ALR (Administrative License Revocation) - all alcohol/drug related
    if "ALR" in action:
        return "road_safety"
    
    # Road safety - DUI/DWI and intoxication related
    if any(kw in action for kw in [
        "DWI", "DUI", "DRIVING WHILE INTOXICATED", "INTOXICATED", "INTOXICATION",
        "ALCOHOL", "BAC", "CHEMICAL TEST", "REFUSAL", "UNDER 21",
        "BOATING WHILE INTOXICATED", "BOATING REFUSAL", "BOATING FAILURE",
        "FLYING WHILE INTOXICATED", "AMUSEMENT RIDE INTOXICATION",
        "INTOXICATION ASSAULT", "INTOXICATION MANSLAUGHTER",
        "ADMINISTRATIVE PER SE", "IMPLIED CONSENT"
    ]):
        return "road_safety"
    
    # Road safety - drug related
    if any(kw in action for kw in [
        "DRUG", "CONTROLLED SUBSTANCE", "DANGEROUS DRUG", "DRUG OFFENSE",
        "DWI EDUCATION PROGRAM", "DRUG EDUCATION PROGRAM"
    ]):
        return "road_safety"
    
    # Road safety - serious traffic violations and dangerous driving
    if any(kw in action for kw in [
        "SERIOUS TRAFFIC VIOLATIONS", "HABITUAL VIOLATOR", "HARDSHIP VIOLATOR",
        "REPEAT OFFENDER", "REPEATED", "SUBSEQUENT",
        "CRASH SERIOUS", "CRASH", "FATAL ACC", "INJ ACC", "PDO ACC",
        "FSRA", "LVSC", "VEHICLE MANSLAUGHTER", "CRIMINAL NEGLIGENT HOMICIDE",
        "MURDER WITH MOTOR VEHICLE"
    ]):
        return "road_safety"
    
    # Road safety - fleeing and evasion
    if any(kw in action for kw in [
        "FLEE POLICE", "EVADE ARREST", "EVADE DETENTION"
    ]):
        return "road_safety"
    
    # Road safety - failure to stop/render aid
    if any(kw in action for kw in [
        "FAILURE TO STOP AND RENDER AID", "FAIL TO STOP", "FAIL TO SLOW",
        "FAILED TO OBEY", "FAIL TO STOP FOR SCHOOL BUS"
    ]):
        return "road_safety"
    
    # Road safety - racing
    if any(kw in action for kw in [
        "RACING", "PROHIBITION RACING"
    ]):
        return "road_safety"
    
    # Road safety - violations of restrictions and prohibitions
    if any(kw in action for kw in [
        "VIOLATE RESTRICTION", "RESTRICTION", "PROHIBITION", "ORDER OF PROHIBITION"
    ]):
        return "road_safety"
    
    # Administrative cancellations - Other (check before CMV/CDL safety checks)
    if "CANCELLED - CDL ONLY" in action or "CANCELLED - CLP ONLY" in action:
        return "Other"
    
    # Road safety - CMV/CDL violations (commercial vehicle safety)
    if any(kw in action for kw in [
        "CMV", "CDL", "CLP", "COMMERCIAL", "HAZMAT", "OUT OF SERVICE",
        "RAILROAD VIOLATION", "RR XING", "RR GATE", "INSUFFICIENT SPACE"
    ]):
        return "road_safety"
    
    # Road safety - driving while license invalid/suspended/revoked
    if any(kw in action for kw in [
        "DWLI", "DWLD", "DRIVING WHILE LICENSE", "DWL", "DRIVING WHILE LICENSE INVALID",
        "DRIVING WHILE LICENSE SUSPENDED", "DRIVING WHILE LICENSE REVOKED",
        "DRIVING WHILE LICENSE CANCELED", "DRIVING WHILE LICENSE DISQUALIFIED",
        "DRIVING WHILE LICENSE WITHDRAWN"
    ]):
        return "road_safety"
    
    # Road safety - out of state convictions (traffic safety related)
    if "OUT OF STATE CONVICTION" in action or "OUT-OF STATE CONVICTION" in action:
        return "road_safety"
    
    # Road safety - out of state crash (safety related)
    if "OUT OF STATE CRASH" in action or "OUT-OF STATE CRASH" in action:
        return "road_safety"
    
    # Medical/incapable - Other (not road safety)
    if any(kw in action for kw in [
        "MEDICAL", "INCAPABLE", "TEST REQUIRED", "MEDICAL ADVISORY"
    ]):
        return "Other"
    
    # Cancellations and denials - Other (administrative)
    if any(kw in action for kw in [
        "CANCELLED - CDL ONLY", "CANCELLED - CLP ONLY",  # Administrative CDL/CLP cancellations
        "CANCELLED", "DENY ISSUANCE", "DENIED RENEWAL"  # (but not FTA/FTP which are handled above)
    ]):
        return "Other"
    
    # Juvenile suspensions - Other (non-traffic)
    if "JUVENILE" in action:
        return "Other"
    
    # Minor violations (non-alcohol) - Other
    if any(kw in action for kw in [
        "MINOR LICENSE VIOLATION", "TOBACCO MINOR EDUCATION COURSE"  # (but not alcohol-related which is road_safety)
    ]):
        return "Other"
    
    # License/ID violations (non-safety) - Other
    if any(kw in action for kw in [
        "FALSIFICATION", "FICTITIOUS", "MISREPRESENTATION", "POSSESS DECEPTIVE",
        "POSSESS MORE THAN ONE", "UNLAWFUL DISPLAY", "LEND/PERMIT USE"
    ]):
        return "Other"
    
    # Contempt - Other
    if "CONTEMPT" in action:
        return "Other"
    
    # Sex offender - Other (not traffic safety)
    if "SEX OFFENDER" in action:
        return "Other"
    
    # Section 521.319 - Other (administrative)
    if "SECTION 521.319" in action:
        return "Other"
    
    # NRVC - Other
    if "NRVC" in action:
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

