#import
import pandas as pd
import re
from pathlib import Path
from calendar import month_name
import os


BASE_DIR = Path(__file__).resolve().parent
DATA_ROOT = BASE_DIR.parent

#paths
INPUT_EXCEL= DATA_ROOT / "Colorado" / "Data" / "2022_DeptActionsCORA.xlsx"
OUTPUT_CSV= DATA_ROOT / "Colorado" / "Data" / "2022_DeptActionsCORAEdited.xlsx"

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

#run
df_out = build_month_reason_pivot(INPUT_EXCEL)
df_out.to_csv(OUTPUT_CSV, index=False)

#paths
INPUT_CSV = DATA_ROOT / "Colorado" / "Data" / "2022_DeptActionsCORAEdited.xlsx"
OUTPUT_CSV = DATA_ROOT / "Outputs" / "Colorado.csv"

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

#read the input pivot (month × code)
df = pd.read_csv(INPUT_CSV)

#identify code level columns
non_code_cols = {"time", "total"}
code_cols = [
    c for c in df.columns
    if c not in non_code_cols and not str(c).startswith("Unnamed")
]

#map each column to a category
col_to_category = {col: infer_category_for_column(col) for col in code_cols}

#prepare output
if "time" in df.columns:
    out = df[["time"]].copy()
else:
    out = pd.DataFrame(index=df.index)

categories = ["FTP", "FTA", "road_safety", "Other"]

#sum the columns that belong to each category
for cat in categories:
    cat_cols = [col for col, c in col_to_category.items() if c == cat]
    if cat_cols:
        out[cat] = df[cat_cols].sum(axis=1)
    else:
        out[cat] = 0

#compute total between the different categories
out["total"] = out[categories].sum(axis=1)

#categorized output
out.to_csv(OUTPUT_CSV, index=False)

# delete temporary edited pivot file
edited_path = DATA_ROOT / "Colorado" / "Data" / "2022_DeptActionsCORAEdited.xlsx"
try:
    if edited_path.exists():
        edited_path.unlink()
except Exception as e:
    print(f"warning: could not delete {edited_path}: {e}")