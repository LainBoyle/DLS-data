#import
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import re
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parent
OUTPUTS_DIR = BASE_DIR / "Outputs"
GRAPHS_DIR = BASE_DIR / "Graphs"
REFORMS_FILE = BASE_DIR / "Reforms.txt"

# Create Graphs directory if it doesn't exist
GRAPHS_DIR.mkdir(parents=True, exist_ok=True)

# Parse Reforms.txt to get reform information
def parse_reforms_file():
    """Parse Reforms.txt and return a dictionary mapping state names to reform data."""
    reforms_dict = {}
    if not REFORMS_FILE.exists():
        print(f"Warning: {REFORMS_FILE} not found. Vertical lines will not be added.")
        return reforms_dict
    
    try:
        with open(REFORMS_FILE, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            # Skip header line
            for line in lines[1:]:
                line = line.strip()
                if not line:
                    continue
                # Split by comma or tab
                parts = re.split(r'[,\t]+', line)
                if len(parts) >= 6:
                    state = parts[0].strip()
                    enacted_date_str = parts[1].strip()
                    effective_date_str = parts[3].strip()
                    ftp_type = parts[4].strip() if len(parts) > 4 else "—"
                    fta_type = parts[5].strip() if len(parts) > 5 else "—"
                    
                    reform_data = {}
                    
                    # Parse enacted date
                    try:
                        month, year = enacted_date_str.split('/')
                        reform_data['enacted_date'] = datetime(int(year), int(month), 1)
                    except (ValueError, IndexError):
                        print(f"Warning: Could not parse enacted date '{enacted_date_str}' for {state}")
                        continue
                    
                    # Parse effective date
                    try:
                        month, year = effective_date_str.split('/')
                        reform_data['effective_date'] = datetime(int(year), int(month), 1)
                    except (ValueError, IndexError):
                        print(f"Warning: Could not parse effective date '{effective_date_str}' for {state}")
                        continue
                    
                    # Store reform types
                    reform_data['ftp_type'] = ftp_type
                    reform_data['fta_type'] = fta_type
                    
                    # Determine if FTA is included (not "—")
                    reform_data['includes_fta'] = fta_type != "—"
                    
                    # Determine reform type for legend (use FTP type, or FTA if FTP is "—")
                    if ftp_type != "—":
                        reform_data['reform_type'] = ftp_type
                    elif fta_type != "—":
                        reform_data['reform_type'] = fta_type
                    else:
                        reform_data['reform_type'] = "Unknown"
                    
                    # Store with multiple key variations for easier lookup
                    reforms_dict[state] = reform_data
                    # Also store normalized versions
                    state_normalized = state.replace('.', '').replace(' ', '').lower()
                    reforms_dict[state_normalized] = reform_data
                    # Store title case version
                    reforms_dict[state.title()] = reform_data
    except Exception as e:
        print(f"Error reading {REFORMS_FILE}: {e}")
    
    return reforms_dict

# Load reforms data
reforms_dict = parse_reforms_file()

# Color scheme
COLORS = {
    'FTP': 'green',
    'FTA': 'blue',
    'road_safety': 'red',
    'Other': 'orange',
    'total': 'black'
}

# Find all CSV files in Outputs folder
csv_files = sorted([f for f in OUTPUTS_DIR.glob("*.csv") if f.is_file()])

if not csv_files:
    print("No CSV files found in Outputs folder")
    exit(1)

print(f"Found {len(csv_files)} CSV file(s) to process")

for csv_file in csv_files:
    print(f"\nProcessing {csv_file.name}...")
    
    # Read the CSV file
    df = pd.read_csv(csv_file)
    
    # Check for required columns
    if 'time' not in df.columns:
        print(f"  Warning: No 'time' column found, skipping")
        continue
    
    # Filter out the 'total' row for plotting
    df_plot = df[df['time'] != 'total'].copy()
    
    if len(df_plot) == 0:
        print(f"  Warning: No data rows found, skipping")
        continue
    
    # Convert time to datetime for proper sorting
    df_plot['time_dt'] = pd.to_datetime(df_plot['time'], format='%Y-%m', errors='coerce')
    df_plot = df_plot[df_plot['time_dt'].notna()].sort_values('time_dt')
    
    if len(df_plot) == 0:
        print(f"  Warning: No valid dates found, skipping")
        continue
    
    # Create the plot
    fig, ax = plt.subplots(figsize=(14, 8))
    
    # Plot each category
    # categories = ['FTP', 'FTA', 'road_safety', 'Other', 'total']
    categories = ['FTP', 'FTA', 'total']
    for category in categories:
        if category in df_plot.columns:
            ax.plot(df_plot['time_dt'], df_plot[category], 
                   label=category, color=COLORS.get(category, 'gray'), 
                   linewidth=2, marker='o', markersize=3)
    
    # Customize the plot
    ax.set_xlabel('Time (Months)', fontsize=12, fontweight='bold')
    ax.set_ylabel('Number of Suspensions', fontsize=12, fontweight='bold')
    
    # Get state name from filename
    state_name = csv_file.stem.replace('_', ' ').title()
    ax.set_title(f'Suspensions Over Time: {state_name}', fontsize=14, fontweight='bold')
    
    # Add vertical lines for Enacted Date and Effective Date if available
    # Try multiple state name variations for lookup
    state_variations = [
        state_name,
        state_name.replace(' ', ''),
        state_name.lower(),
        state_name.replace('.', '').replace(' ', '').lower(),
        csv_file.stem,  # Original filename without extension
        csv_file.stem.replace('_', ' ').title(),
        csv_file.stem.replace('_', ' ')
    ]
    
    found = False
    for variation in state_variations:
        if variation in reforms_dict:
            reform_data = reforms_dict[variation]
            enacted_date = reform_data['enacted_date']
            effective_date = reform_data['effective_date']
            includes_fta = reform_data['includes_fta']
            ftp_type = reform_data['ftp_type']
            fta_type = reform_data['fta_type']
            
            # Determine line color: green for FTP only, blue if FTA included
            line_color = 'blue' if includes_fta else 'green'
            
            # Build reform type label for legend
            if includes_fta and fta_type != "—":
                reform_label = f"FTP: {ftp_type}, FTA: {fta_type}"
            else:
                reform_label = f"FTP: {ftp_type}" if ftp_type != "—" else "Unknown"
            
            # Add dotted vertical line for Enacted Date
            ax.axvline(x=enacted_date, color=line_color, linestyle=':', linewidth=2, 
                      label=f'Reform Enacted ({reform_label})', alpha=0.7)
            
            # Add solid vertical line for Effective Date
            ax.axvline(x=effective_date, color=line_color, linestyle='-', linewidth=2, 
                      label=f'Reform Effective ({reform_label})', alpha=0.7)
            
            print(f"  Added vertical lines - Enacted: {enacted_date.strftime('%m/%Y')}, "
                  f"Effective: {effective_date.strftime('%m/%Y')} ({reform_label})")
            found = True
            break
    
    if not found:
        print(f"  Warning: No reform data found for {state_name} (tried variations: {state_variations[:3]})")
    
    # Format x-axis dates
    ax.tick_params(axis='x', rotation=45)
    fig.autofmt_xdate()
    
    # Add grid
    ax.grid(True, alpha=0.3, linestyle='--')
    
    # Add legend
    ax.legend(loc='best', fontsize=10)
    
    # Adjust layout
    plt.tight_layout()
    
    # Save the graph
    output_file = GRAPHS_DIR / f"{csv_file.stem}.png"
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"  Saved graph to {output_file}")

print(f"\n\nAll graphs saved to {GRAPHS_DIR}")


