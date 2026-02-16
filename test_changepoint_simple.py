"""
Simple test to debug changepoint detection indexing issue.
"""

import pandas as pd
import numpy as np
from datetime import date, timedelta

def feature_changepoint_count_and_dates(df):
    """Variance-based changepoint detection that returns both count and dates."""
    try:
        if len(df) < 10:
            return 0, []
        
        df = df.copy()
        df['ds'] = pd.to_datetime(df['ds'])
        df = df.sort_values('ds').reset_index(drop=True)
        
        # Filter out NaN and Inf values
        valid_mask = ~(df['y'].isna() | np.isinf(df['y']))
        df_clean = df[valid_mask].reset_index(drop=True)
        
        if len(df_clean) < 10:
            return 0, []
        
        values = df_clean['y'].values
        dates = df_clean['ds'].values
        
        window_size = min(20, len(values) // 4)
        if window_size < 5:
            return 0, []
        
        rolling_var = pd.Series(values).rolling(window=window_size, center=True).var()
        var_mean = rolling_var.mean()
        var_std = rolling_var.std()
        threshold = var_mean + 2 * var_std if not np.isnan(var_std) and var_std > 0 else var_mean * 2
        
        changepoints_mask = (rolling_var > threshold) & (~np.isnan(rolling_var))
        
        changepoint_dates = []
        in_changepoint = False
        for idx, is_cp in enumerate(changepoints_mask):
            if is_cp and not in_changepoint:
                # FIXED LOGIC: Don't adjust the index
                # When variance FIRST becomes high at idx, the change likely occurred
                # between idx-1 and idx. Use idx-1 as the changepoint (previous_date).
                
                if idx < len(dates):
                    # Get the date at the detection index
                    changepoint_date = pd.Timestamp(dates[idx])
                    changepoint_date_date = changepoint_date.date()
                    
                    # Get previous date (the day before the detected changepoint)
                    previous_date_date = None
                    if idx > 0:
                        previous_date = pd.Timestamp(dates[idx - 1])
                        previous_date_date = previous_date.date()
                    
                    changepoint_dates.append((changepoint_date_date, previous_date_date))
                in_changepoint = True
            elif not is_cp:
                in_changepoint = False
        
        return len(changepoint_dates), changepoint_dates
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 0, []

# Create a simple test case
start_date = date(2024, 1, 1)
num_days = 100
known_cp_idx = 50  # Changepoint at day 50

# Create data with clear changepoint
dates = [start_date + timedelta(days=i) for i in range(num_days)]
values = []
for i in range(num_days):
    if i < 50:
        values.append(100.0)  # Constant value
    else:
        values.append(200.0)  # Sudden jump

# Add some missing values (but not at changepoint)
missing = [10, 20, 30, 70, 80]
for idx in missing:
    if idx < len(values):
        values[idx] = np.nan

df = pd.DataFrame({'ds': dates, 'y': values})

print("=" * 80)
print("TEST: Simple changepoint detection")
print("=" * 80)
print(f"Known changepoint: {start_date + timedelta(days=known_cp_idx)} (day {known_cp_idx})")
print(f"Original length: {len(df)}")
print(f"Missing values: {df['y'].isna().sum()}")

# Show original data
print(f"\nOriginal data around changepoint:")
for i in range(45, 55):
    if i < len(df):
        marker = " <-- KNOWN CP" if i == known_cp_idx else ""
        missing = " (MISSING)" if pd.isna(df.iloc[i]['y']) else ""
        print(f"  orig_idx={i:3d}, date={df.iloc[i]['ds'].date()}, value={df.iloc[i]['y']}{missing}{marker}")

# Clean data
df_clean = df[~(df['y'].isna() | np.isinf(df['y']))].reset_index(drop=True)
print(f"\nCleaned length: {len(df_clean)}")

# Find known CP in cleaned data
known_cp_clean_idx = None
for idx, row in df_clean.iterrows():
    if row['ds'].date() == start_date + timedelta(days=known_cp_idx):
        known_cp_clean_idx = idx
        break

if known_cp_clean_idx is not None:
    print(f"Known changepoint at cleaned index: {known_cp_clean_idx}")
    print(f"\nCleaned data around changepoint:")
    for i in range(max(0, known_cp_clean_idx-5), min(len(df_clean), known_cp_clean_idx+5)):
        marker = " <-- KNOWN CP" if i == known_cp_clean_idx else ""
        print(f"  clean_idx={i:3d}, date={df_clean.iloc[i]['ds'].date()}, value={df_clean.iloc[i]['y']}{marker}")

# Calculate rolling variance
values_clean = df_clean['y'].values
window_size = min(20, len(values_clean) // 4)
print(f"\nWindow size: {window_size}")

rolling_var = pd.Series(values_clean).rolling(window=window_size, center=True).var()
var_mean = rolling_var.mean()
var_std = rolling_var.std()
threshold = var_mean + 2 * var_std

print(f"Variance mean: {var_mean:.2f}, std: {var_std:.2f}, threshold: {threshold:.2f}")

# Show where high variance is detected
print(f"\nRolling variance around changepoint:")
if known_cp_clean_idx is not None:
    for i in range(max(0, known_cp_clean_idx-10), min(len(rolling_var), known_cp_clean_idx+10)):
        var_val = rolling_var.iloc[i] if i < len(rolling_var) else np.nan
        is_high = not np.isnan(var_val) and var_val > threshold
        marker = " <-- HIGH VAR" if is_high else ""
        marker2 = " <-- KNOWN CP" if i == known_cp_clean_idx else ""
        print(f"  idx={i:3d}, rolling_var={var_val:.2f}, threshold={threshold:.2f}{marker}{marker2}")

# Run detection
count, dates_list = feature_changepoint_count_and_dates(df)

print(f"\nDetected changepoints: {count}")
for i, (cp_date, prev_date) in enumerate(dates_list):
    print(f"  CP {i+1}: detected_date={cp_date}, previous_date={prev_date}")
    if known_cp_clean_idx is not None:
        # Find this date in cleaned data
        for idx, row in df_clean.iterrows():
            if row['ds'].date() == cp_date:
                print(f"    -> detected at cleaned index: {idx}")
                print(f"    -> adjusted from rolling_var index (need to trace back)")
                break

print("\n" + "=" * 80)
print("ANALYSIS:")
print("=" * 80)
print("The issue is likely:")
print("1. Rolling variance with center=True means variance at idx is calculated from")
print("   values[idx-window_size//2 : idx+window_size//2+1]")
print("2. When we detect high variance at idx, the change likely started earlier")
print("3. We adjust by -window_size//2, but this might not be correct")
print("4. We should look at where the variance FIRST exceeds threshold, not just where it's high")
