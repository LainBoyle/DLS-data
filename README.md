# Driver License Suspension Data Processing

This repository contains Python scripts to process driver license suspension data from various states and generate visualizations.

---

## Installation Requirements

Before running any scripts, you must install the required Python packages:

!pip install -q pandas openpyxl pdfplumber matplotlib

---

## Adding Data for Each State

Each state has its own folder in the project root directory. To add new data files for a state, place the additional data files directly in the state's folder.


**File format compatibility**: 
   - **Excel files (.xlsx)**: Supported by Colorado, Illinois, Texas, Maryland, Oregon, Washington
   - **PDF files (.pdf)**: Supported by Virginia, New Mexico
   - **CSV/Text files**: Supported by various states (check individual scripts)

---

### State.py Output

Each script generates a CSV file in the `Outputs/` folder with the format:
- **Filename**: `Outputs/[State].csv`
- **Columns**: `time`, `FTP`, `FTA`, `road_safety`, `Other`, `total`
- **Time format**: `YYYY-MM` (e.g., `2018-01`, `2020-03`)

---

## Creating Graphs

The `create_graphs.py` script generates visualization graphs for all CSV files in the `Outputs/` folder.

---

## Adding Reforms to Reforms.txt

The `Reforms.txt` file contains information about driver license suspension reforms for each state. This data is used by `create_graphs.py` to add vertical reference lines on the graphs.

### Reforms.txt Format

The file uses a tab or comma-delimited format with the following columns:

1. **State** - State name (e.g., "Colorado", "Illinois", "New York")
2. **Enacted Date** - Date the reform was enacted (format: `M/YYYY`, e.g., `6/2021`)
3. **Bill Number** - Bill or law number (e.g., "HB21-1314", "SB 1786")
4. **Effective Date** - Date the reform became effective (format: `M/YYYY`, e.g., `1/2022`)
5. **Failure to Pay** - Type of FTP reform: `Full`, `Partial`, or `—` (if not applicable)
6. **Failure to Appear** - Type of FTA reform: `Full`, `Partial`, `Procedural`, or `—` (if not applicable)

### Example Entry

```
Colorado,	6/2021,	HB21-1314,	1/2022,	Full,	Full
```

This means:
- State: Colorado
- Enacted: June 2021
- Bill: HB21-1314
- Effective: January 2022
- FTP: Full reform
- FTA: Full reform


### Notes

- Use `—` (em dash) to indicate that a reform type does not apply
- Dates must be in `M/YYYY` format (e.g., `6/2021` for June 2021, `12/2020` for December 2020)
- State names should match the CSV filename (e.g., "New York" for `NewYork.csv`, "New Mexico" for `NewMexico.csv`)
- The script will try multiple variations of state names for matching (e.g., "New York", "NewYork", "new york")

