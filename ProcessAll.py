#import
import pandas as pd
import importlib.util
from pathlib import Path
from datetime import datetime, timedelta
import time
import threading

BASE_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = BASE_DIR / "Scripts"
OUTPUTS_DIR = BASE_DIR / "Outputs"

# Configuration
USE_EXISTING_CSV = False  # If True, use existing CSV files instead of re-running scripts
# Set to False to always re-process data (needed to separate child support from fees)
CSV_MAX_AGE_HOURS = 24  # Only use CSV if it's less than this many hours old
STATE_TIMEOUT_SECONDS = 1800  # 30 minutes max per state

# Map state script names to state names
STATE_SCRIPTS = {
    'Colorado': 'Colorado',
    'Illinois': 'Illinois',
    'Maryland': 'Maryland',
    'Minnesota': 'Minnesota',
    'Nevada': 'Nevada',
    'NewMexico': 'New Mexico',
    'NewYork': 'New York',
    'Oregon': 'Oregon',
    'Texas': 'Texas',
    'Utah': 'Utah',
    'Vermont': 'Vermont',
    'Virginia': 'Virginia',
    'Washington': 'Washington'
}

def check_existing_csv(script_name):
    """Check if a CSV file exists and is recent enough to use"""
    if not USE_EXISTING_CSV:
        return None
    
    csv_path = OUTPUTS_DIR / f"{script_name}.csv"
    if not csv_path.exists():
        return None
    
    # Check file age
    file_age = datetime.now() - datetime.fromtimestamp(csv_path.stat().st_mtime)
    if file_age < timedelta(hours=CSV_MAX_AGE_HOURS):
        print(f"  Using existing CSV file (age: {file_age.total_seconds()/3600:.1f} hours)")
        try:
            return pd.read_csv(csv_path)
        except Exception as e:
            print(f"  Warning: Error reading existing CSV: {e}")
            return None
    
    return None

def print_progress_bar(current, total, prefix='', suffix='', length=40):
    """Print a progress bar using ASCII-safe characters"""
    percent = f"{100 * (current / float(total)):.1f}"
    filled_length = int(length * current // total)
    # Use ASCII characters that work on Windows console
    bar = '#' * filled_length + '-' * (length - filled_length)
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end='', flush=True)
    if current == total:
        print()  # New line when complete

def run_state_script(script_name, state_name, state_num, total_states):
    """Run a state script and capture its output dataframe"""
    script_path = SCRIPTS_DIR / f"{script_name}.py"
    
    if not script_path.exists():
        print(f"  Warning: Script {script_path} not found")
        return None
    
    # Check for existing CSV first
    existing_df = check_existing_csv(script_name)
    if existing_df is not None:
        print_progress_bar(state_num, total_states, 
                          prefix=f'[{state_num}/{total_states}] {state_name}', 
                          suffix='Using existing CSV')
        return existing_df
    
    print(f"  [{state_num}/{total_states}] Running {script_name}.py...")
    bar = '-' * 40
    print(f"  Progress: [{bar}] 0.0% - Starting...", end='', flush=True)
    start_time = time.time()
    last_update = start_time
    
    try:
        # Load the module
        spec = importlib.util.spec_from_file_location(script_name, script_path)
        module = importlib.util.module_from_spec(spec)
        
        # Capture the dataframe - try multiple methods
        captured_df = None
        
        # Method 1: Monkey-patch DataFrame.to_csv to capture the dataframe
        original_to_csv = pd.DataFrame.to_csv
        
        def capture_to_csv(self, *args, **kwargs):
            nonlocal captured_df
            # Only capture if it's being written to the Outputs directory
            path = kwargs.get('path_or_buf', args[0] if args else None)
            if path and 'Outputs' in str(path):
                captured_df = self.copy()
                print(f"    Captured dataframe via to_csv with {len(self)} rows")
            # Still write to CSV (we'll use it as fallback)
            return original_to_csv(self, *args, **kwargs)
        
        # Temporarily replace to_csv
        pd.DataFrame.to_csv = capture_to_csv
        
        # Execute the module (with timeout protection via try/except)
        # Use a thread to show progress updates during execution
        progress_stop = threading.Event()
        def show_progress():
            update_interval = 5  # Update every 5 seconds
            while not progress_stop.is_set():
                elapsed = time.time() - start_time
                elapsed_str = f"{elapsed/60:.1f} min" if elapsed > 60 else f"{elapsed:.0f} sec"
                # Simple progress indicator - we don't know actual progress, so show elapsed time
                print(f'\r  Progress: Processing... ({elapsed_str} elapsed)', end='', flush=True)
                time.sleep(update_interval)
        
        progress_thread = threading.Thread(target=show_progress, daemon=True)
        progress_thread.start()
        
        try:
            spec.loader.exec_module(module)
            progress_stop.set()
            elapsed = time.time() - start_time
            elapsed_str = f"{elapsed/60:.1f} minutes" if elapsed > 60 else f"{elapsed:.1f} seconds"
            bar = '#' * 40
            print(f'\r  Progress: [{bar}] 100.0% - Completed in {elapsed_str}')
        except KeyboardInterrupt:
            progress_stop.set()
            print(f'\r  Progress: Interrupted after {time.time() - start_time:.1f} seconds')
            raise
        except Exception as e:
            progress_stop.set()
            elapsed = time.time() - start_time
            if elapsed > STATE_TIMEOUT_SECONDS:
                print(f'\r  Progress: Warning - Script took {elapsed/60:.1f} minutes (may have timed out)')
            raise
        
        # Method 2: Try to get output dataframe from module namespace
        if captured_df is None:
            # Check common variable names used by state scripts
            for var_name in ['output_df', 'df_out', 'out', 'pivot_df', 'combined_df', 'df']:
                if hasattr(module, var_name):
                    var = getattr(module, var_name)
                    if isinstance(var, pd.DataFrame) and 'time' in var.columns:
                        captured_df = var.copy()
                        print(f"    Captured dataframe from module variable '{var_name}' with {len(var)} rows")
                        break
        
        # Restore original to_csv
        pd.DataFrame.to_csv = original_to_csv
        
        # Method 3: Read from CSV as fallback
        if captured_df is None:
            csv_path = OUTPUTS_DIR / f"{script_name}.csv"
            if csv_path.exists():
                print(f"    Reading from CSV file...")
                try:
                    captured_df = pd.read_csv(csv_path)
                    print(f"    Successfully read CSV with {len(captured_df)} rows")
                except Exception as e:
                    print(f"    Error reading CSV: {e}")
        
        if captured_df is None:
            print(f"    Warning: Could not capture dataframe from {script_name}.py")
            return None
        
        return captured_df
        
    except Exception as e:
        print(f"    Error running {script_name}.py: {e}")
        import traceback
        traceback.print_exc()
        # Try to read from CSV as fallback
        csv_path = OUTPUTS_DIR / f"{script_name}.csv"
        if csv_path.exists():
            print(f"    Attempting to read from CSV as fallback...")
            try:
                return pd.read_csv(csv_path)
            except:
                pass
        return None

def map_categories_to_output(df):
    """Map the standard categories (FTP, FTA, road_safety, Other) to output format
    
    Note: Child support is currently included in FTP. To separate it, state scripts
    need to be modified to output a separate 'child_support' category.
    """
    # Get the total row
    total_row = df[df['time'] == 'total']
    
    if len(total_row) == 0:
        # Calculate from all rows if no total row
        df_dates = df[df['time'] != 'total'].copy()
        if len(df_dates) == 0:
            return None
        
        # Map categories:
        # FTP -> Fees (child support is currently included in FTP)
        # FTA -> FTA
        # road_safety -> Road safety
        # Other -> Driving (car registration, insurance, parking tickets)
        # child_support -> Child support (if state script outputs it separately)
        
        driving = df_dates['Other'].sum() if 'Other' in df_dates.columns else 0
        fta = df_dates['FTA'].sum() if 'FTA' in df_dates.columns else 0
        road_safety = df_dates['road_safety'].sum() if 'road_safety' in df_dates.columns else 0
        
        # Check if child_support is a separate category (check both naming conventions)
        child_support_col = None
        if 'child_support' in df_dates.columns:
            child_support_col = 'child_support'
        elif 'Child_Support' in df_dates.columns:
            child_support_col = 'Child_Support'
        
        if child_support_col:
            child_support = df_dates[child_support_col].sum()
            # If Child_Support is a separate column, FTP already excludes it
            # So we don't need to subtract it
            fees = df_dates['FTP'].sum() if 'FTP' in df_dates.columns else 0
        else:
            # Child support is included in FTP - cannot separate without raw data
            child_support = 0
            fees = df_dates['FTP'].sum() if 'FTP' in df_dates.columns else 0
    else:
        # Use totals from the total row
        row = total_row.iloc[0]
        driving = int(row['Other']) if 'Other' in row and pd.notna(row['Other']) else 0
        fta = int(row['FTA']) if 'FTA' in row and pd.notna(row['FTA']) else 0
        road_safety = int(row['road_safety']) if 'road_safety' in row and pd.notna(row['road_safety']) else 0
        
        # Check if child_support is a separate category (check both naming conventions)
        child_support_col = None
        if 'child_support' in row.index and pd.notna(row['child_support']):
            child_support_col = 'child_support'
        elif 'Child_Support' in row.index and pd.notna(row['Child_Support']):
            child_support_col = 'Child_Support'
        
        if child_support_col:
            child_support = int(row[child_support_col])
            # If Child_Support is a separate column, FTP already excludes it
            # So we don't need to subtract it
            fees = int(row['FTP']) if 'FTP' in row and pd.notna(row['FTP']) else 0
        else:
            # Child support is included in FTP - cannot separate without raw data
            child_support = 0
            fees = int(row['FTP']) if 'FTP' in row and pd.notna(row['FTP']) else 0
    
    return {
        'Driving': driving,
        'Fees': fees,
        'FTA': fta,
        'Child support': child_support,
        'Road safety': road_safety
    }

def get_year_range(df):
    """Extract year range from the dataframe"""
    # Filter out the 'total' row
    df_dates = df[df['time'] != 'total'].copy()
    
    if len(df_dates) == 0:
        return None
    
    # Convert time to datetime
    df_dates['time_dt'] = pd.to_datetime(df_dates['time'], format='%Y-%m', errors='coerce')
    df_dates = df_dates[df_dates['time_dt'].notna()]
    
    if len(df_dates) == 0:
        return None
    
    min_year = df_dates['time_dt'].min().year
    max_year = df_dates['time_dt'].max().year
    
    return f"{min_year}-{max_year}"

# Process all states
print("Processing all state scripts...")
print("=" * 60)
print(f"Configuration: USE_EXISTING_CSV={USE_EXISTING_CSV}, CSV_MAX_AGE_HOURS={CSV_MAX_AGE_HOURS}")
print(f"Total states to process: {len(STATE_SCRIPTS)}")
print("=" * 60)

all_state_data = []
failed_states = []
total_start_time = time.time()

for idx, (script_name, state_name) in enumerate(STATE_SCRIPTS.items(), 1):
    # Overall progress indicator
    overall_progress = (idx - 1) / len(STATE_SCRIPTS) * 100
    elapsed_total = time.time() - total_start_time
    avg_time_per_state = elapsed_total / (idx - 1) if idx > 1 else 0
    remaining_states = len(STATE_SCRIPTS) - (idx - 1)
    estimated_remaining = avg_time_per_state * remaining_states if avg_time_per_state > 0 else 0
    
    print(f"\n{'=' * 60}")
    print(f"Overall Progress: [{idx-1}/{len(STATE_SCRIPTS)}] {overall_progress:.1f}%")
    if idx > 1 and estimated_remaining > 0:
        est_str = f"{estimated_remaining/60:.1f} min" if estimated_remaining > 60 else f"{estimated_remaining:.0f} sec"
        print(f"Estimated time remaining: ~{est_str}")
    print(f"{'=' * 60}")
    print(f"[{idx}/{len(STATE_SCRIPTS)}] Processing {state_name}...")
    state_start_time = time.time()
    
    try:
        # Run the state script and get the dataframe
        df = run_state_script(script_name, state_name, idx, len(STATE_SCRIPTS))
        
        if df is None or len(df) == 0:
            print(f"  Warning: No data for {state_name}, skipping")
            failed_states.append((state_name, "No data"))
            continue
    
        # Get year range
        years_str = get_year_range(df)
        if years_str is None:
            print(f"  Warning: Could not determine year range for {state_name}")
            years_str = "Unknown"
        
        # Map categories
        category_data = map_categories_to_output(df)
        if category_data is None:
            print(f"  Warning: Could not extract category data for {state_name}")
            failed_states.append((state_name, "Could not extract category data"))
            continue
        
        # Store state data
        state_data = {
            'State': state_name,
            'Years': years_str,
            'Driving': category_data['Driving'],
            'Fees': category_data['Fees'],
            'FTA': category_data['FTA'],
            'Child support': category_data['Child support'],
            'Road safety': category_data['Road safety']
        }
        
        all_state_data.append(state_data)
        state_elapsed = time.time() - state_start_time
        print(f"  [OK] {state_name} ({years_str}) - {state_elapsed:.1f}s")
        print(f"    Driving: {category_data['Driving']:,}, Fees: {category_data['Fees']:,}, "
              f"FTA: {category_data['FTA']:,}, Child support: {category_data['Child support']:,}, "
              f"Road safety: {category_data['Road safety']:,}")
    
    except KeyboardInterrupt:
        print(f"\n  Interrupted by user. Processed {len(all_state_data)} states so far.")
        break
    except Exception as e:
        state_elapsed = time.time() - state_start_time
        print(f"  [ERROR] Failed to process {state_name} after {state_elapsed:.1f}s: {e}")
        failed_states.append((state_name, str(e)))
        # Continue with next state
        continue

# Create DataFrame with all state data
total_elapsed = time.time() - total_start_time

if all_state_data:
    df_all = pd.DataFrame(all_state_data)
    
    # Sort by state name
    df_all = df_all.sort_values('State')
    
    # Save to All.csv
    output_file = OUTPUTS_DIR / "All.csv"
    df_all.to_csv(output_file, index=False)
    
    print(f"\n{'=' * 60}")
    print(f"All.csv created successfully at {output_file}")
    print(f"Total states processed successfully: {len(all_state_data)}/{len(STATE_SCRIPTS)}")
    print(f"Total processing time: {total_elapsed/60:.1f} minutes")
    
    if failed_states:
        print(f"\nFailed states ({len(failed_states)}):")
        for state, reason in failed_states:
            print(f"  - {state}: {reason}")
    
    print(f"\nPreview of All.csv:")
    print(df_all.to_string(index=False))
else:
    print(f"\n\nNo data to process. All.csv was not created.")
    print(f"Total processing time: {total_elapsed/60:.1f} minutes")
    if failed_states:
        print(f"\nFailed states ({len(failed_states)}):")
        for state, reason in failed_states:
            print(f"  - {state}: {reason}")
