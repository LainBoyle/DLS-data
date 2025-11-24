#import
import pandas as pd
import re
from pathlib import Path
from calendar import month_name
import os


BASE_DIR = Path(__file__).resolve().parent
DATA_ROOT = BASE_DIR.parent

#paths
DATA_DIR = DATA_ROOT / "Colorado" / "Data"
OUTPUT_CSV = DATA_ROOT / "Outputs" / "Colorado.csv"

def _clean_cols(df):
    #normalizes column names
    df=df.copy()
    df.columns=[str(c).strip().lower() for c in df.columns]
    return df

def _infer_year_from_filename(path:str):
    #pulls a 4 digit year from file name
    stem=Path(path).stem
    for token in re.split(r"[ _\-\.]+", stem):
        if token.isdigit() and len(token)==4:
            return int(token)
    return pd.Timestamp.today().year

def _month_from_sheet_name(name:str):
    #extracts month number from sheet name
    s=str(name).strip().lower()
    for i in range(1,13):
        if month_name[i].lower()==s or s.startswith(month_name[i].lower()[:3]):
            return i
    m=re.search(r"(?:^|[^0-9])([1-9]|1[0-2])(?:[^0-9]|$)", s)
    return int(m.group(1)) if m else None

def _pick_reason_and_count(df):
    #selects reason and count columns
    cols=list(df.columns)
    reason_cands=[c for c in cols if any(k in c for k in ["action","reason","category","type","disposition"])]
    count_cands=[c for c in cols if c in ["count","counts","n","num","number","total","qty","quantity"]]
    reason_col=reason_cands[0] if reason_cands else cols[0]
    if count_cands:
        count_col=count_cands[0]
    else:
        df["_count"]=1
        count_col="_count"
    return reason_col,count_col

def build_month_reason_pivot(input_excel:str)->pd.DataFrame:
    #builds month by reason matrix with totals
    yr=_infer_year_from_filename(input_excel)
    sheets=pd.read_excel(input_excel, sheet_name=None)
    frames=[]
    for sheet_name, df in sheets.items():
        #cleans column names
        df=_clean_cols(df)
        #figures out the month number from the tab name
        mon=_month_from_sheet_name(sheet_name)
        #skips tabs that dont match a month pattern
        if mon is None:
            continue
        #adds a helper month column for grouping later
        df["_month_num"]=mon
        frames.append(df)
    #combines all months into one dataframe
    src=pd.concat(frames, ignore_index=True)
    #detects which columns hold reason text and count data
    reason_col,count_col=_pick_reason_and_count(src)
    #creates a formatted month label using the inferred year
    src["_time"]=src["_month_num"].apply(lambda m:f"{int(yr):04d}-{int(m):02d}")
    #groups by month and reason to calculate counts
    agg=src.groupby(["_time",reason_col], dropna=False)[count_col].sum().reset_index()
    #reshapes data to have reasons as columns and months as rows
    wide=agg.pivot(index="_time", columns=reason_col, values=count_col).fillna(0).astype(int).sort_index()
    #adds a total column that sums all reasons per month
    wide["total"]=wide.sum(axis=1)
    #creates one more row that sums everything across the whole year
    totals_row=wide.sum(axis=0).to_frame().T
    totals_row.index=["total"]
    #puts the final data together and resets index
    out=pd.concat([wide,totals_row], axis=0)
    out.insert(0,"time", out.index)
    out=out.reset_index(drop=True)
    return out

#find all xlsx files in Colorado/Data
xlsx_files = sorted([f for f in DATA_DIR.glob("*.xlsx") if f.is_file()])

if not xlsx_files:
    raise FileNotFoundError(f"No .xlsx files found in {DATA_DIR}")

print(f"Found {len(xlsx_files)} Excel file(s) to process")

#process each file and collect pivot tables
all_pivots = []
for xlsx_file in xlsx_files:
    print(f"Processing {xlsx_file.name}...")
    df_pivot = build_month_reason_pivot(str(xlsx_file))
    #remove the "total" row from individual files (we'll recalculate at the end)
    df_pivot = df_pivot[df_pivot["time"] != "total"].copy()
    #normalize all column names to strings, handling all edge cases
    new_cols = []
    for c in df_pivot.columns:
        try:
            if c is None or (isinstance(c, float) and pd.isna(c)):
                new_cols.append("nan")
            else:
                new_cols.append(str(c))
        except:
            new_cols.append("nan")
    df_pivot.columns = new_cols
    all_pivots.append(df_pivot)

#combine all pivot tables
#get all unique columns across all files (already normalized to strings)
all_columns_set = set()
for df in all_pivots:
    #ensure all columns are strings
    for col in df.columns:
        all_columns_set.add(str(col))

#ensure "time" is first, sort the rest
#explicitly convert all to strings and filter
all_columns_list = [str(c) for c in all_columns_set if str(c) != "time"]
all_columns_list.sort()  # sort in place
all_columns = ["time"] + all_columns_list

#combine all dataframes, filling missing columns with 0
combined_df = pd.concat(all_pivots, ignore_index=True)

#reindex to include all columns, filling missing with 0
#(column names are already normalized to strings)
for col in all_columns:
    if col not in combined_df.columns:
        combined_df[col] = 0

#reorder columns
combined_df = combined_df[all_columns]

#sort by time (excluding "total" rows for now)
combined_df = combined_df.sort_values("time").reset_index(drop=True)

#add a final total row that sums all months
#exclude "time" column from sum calculation
numeric_cols = [c for c in combined_df.columns if c != "time" and combined_df[c].dtype in [int, float, 'int64', 'float64']]
totals_data = {col: combined_df[col].sum() for col in numeric_cols}
totals_data["time"] = "total"
totals_row = pd.DataFrame([totals_data])
#ensure all columns are present, filling missing with 0
for col in all_columns:
    if col not in totals_row.columns:
        totals_row[col] = 0
totals_row = totals_row[all_columns]

#combine with totals row
df_out = pd.concat([combined_df, totals_row], ignore_index=True)

#explicit overrides for codes
EXPLICIT_CODE_CATEGORY = {
    "SFTC": "FTP",
    "RAOC": "road_safety",
    "RLSN": "road_safety",
    "CDHT": "Other",
    "CDJD": "FTA",
    "CDOJ": "FTA",
    "CDUR": "FTA",
    "CDOF": "FTP",
    "SNRV": "FTP",
    "RAON": "road_safety",
    "RAOH": "road_safety",
    "RDRC": "road_safety",
    "RDUI": "road_safety",
    "SDUI": "road_safety",
    "RLSC": "road_safety",
    "SHAR": "road_safety",
    "RVAS": "road_safety",
    "RVHM": "road_safety",
}

#keyword lists
FTP_KEYWORDS = [
    "failed to comply",
    "failed to pay",
    "failure to pay",
    "ftp",
    "unsatisfied judgment",
    "child support",
    "financial responsibility",
    "no liability insurance",
    "insurance",
    "sr22",
    "non-resident violator",
    "out of state ftp",
    "interlock lease",
    "failed to register",
    "fail to register",
]

FTA_KEYWORDS = [
    "failure to appear",
    "fail to appear",
    "fta",
    "default judgment",
    "default judgement",
    "judgment/default",
    "judgment",
    "judgement",
]

ROAD_SAFETY_KEYWORDS = [
    "dui",
    "alcohol",
    "bac",
    "drugs",
    "controlled substance",
    "leave scene",
    "accident",
    "hit and run",
    "vehicular assault",
    "vehicular homicide",
    "excessive points",
    "serious violations",
    "rail crossing",
    "license restriction",
    "restriction",
    "out-of-service",
    "out of service",
    "reckless",
    "speeding",
]

#extracts the action code from a colum name
def _extract_code(col_name: str) -> str | None:
    s = str(col_name).strip()
    m = re.match(r"^([A-Z0-9]{2,5})\b", s)
    if m:
        return m.group(1)
    return None

#infers tje category from the text description
def infer_category_from_text(text: str) -> str:
    desc = text.lower()
    if any(kw in desc for kw in FTA_KEYWORDS):
        return "FTA"
    if any(kw in desc for kw in FTP_KEYWORDS):
        return "FTP"
    if any(kw in desc for kw in ROAD_SAFETY_KEYWORDS):
        return "road_safety"
    return "Other"

#decides category for a single column
def infer_category_for_column(col_name: str) -> str:
    code = _extract_code(col_name)
    if code and code in EXPLICIT_CODE_CATEGORY:
        return EXPLICIT_CODE_CATEGORY[code]
    return infer_category_from_text(col_name)

#categorize and aggregate the combined pivot data
#identify code level columns
non_code_cols = {"time", "total"}
code_cols = [
    c for c in df_out.columns
    if c not in non_code_cols and not str(c).startswith("Unnamed")
]

#map each column to a category
col_to_category = {col: infer_category_for_column(col) for col in code_cols}

#prepare output
if "time" in df_out.columns:
    out = df_out[["time"]].copy()
else:
    out = pd.DataFrame(index=df_out.index)

categories = ["FTP", "FTA", "road_safety", "Other"]

#sum the columns that belong to each category
for cat in categories:
    cat_cols = [col for col, c in col_to_category.items() if c == cat]
    if cat_cols:
        out[cat] = df_out[cat_cols].sum(axis=1)
    else:
        out[cat] = 0

#compute total between the different categories
out["total"] = out[categories].sum(axis=1)

#convert all numeric columns to integers (removes .0 from CSV output)
for col in categories + ["total"]:
    #fill any NaN with 0, then convert to int
    out[col] = pd.to_numeric(out[col], errors='coerce').fillna(0).astype(int)

#ensure output directory exists
OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)

#categorized output - use float_format to ensure integers are written without .0
out.to_csv(OUTPUT_CSV, index=False, float_format='%.0f')
print(f"Output saved to {OUTPUT_CSV}")