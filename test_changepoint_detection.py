"""
Test script for changepoint detection function.
Tests the feature_changepoint_count_and_dates function with synthetic data
that includes missing points and known changepoints.
"""

import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
import sys
from pathlib import Path

# Add notebook directory to path to import the function
notebook_dir = Path(__file__).parent / "victoria_metrics_jobs" / "jobs" / "metrics_forecast_notebooks" / "notebooks"
sys.path.insert(0, str(notebook_dir))

# Import the function (we'll need to extract it or recreate it)
# For now, let's recreate it here for testing

def feature_changepoint_count_and_dates(df):
    """Variance-based changepoint detection that returns both count and dates.
    
    Args:
        df: pandas DataFrame with 'ds' (datetime) and 'y' (value) columns
    
    Returns:
        Tuple of (changepoint_count, changepoint_dates_list)
        changepoint_count: Integer count of changepoints
        changepoint_dates_list: List of tuples (changepoint_date, previous_date)
    """
    try:
        if len(df) < 10:
            return 0, []
        
        # Ensure 'ds' is datetime and sort
        df = df.copy()
        df['ds'] = pd.to_datetime(df['ds'])
        df = df.sort_values('ds').reset_index(drop=True)
        
        # Filter out NaN and Inf values while keeping dates aligned
        # Create a mask for valid values
        valid_mask = ~(df['y'].isna() | np.isinf(df['y']))
        df_clean = df[valid_mask].reset_index(drop=True)
        
        if len(df_clean) < 10:
            return 0, []
        
        # Get aligned values and dates
        values = df_clean['y'].values
        dates = df_clean['ds'].values
        
        window_size = min(20, len(values) // 4)
        if window_size < 5:
            return 0, []
        
        # Calculate rolling variance with center=True
        # This means the variance at index i is calculated from values[i-window_size//2 : i+window_size//2+1]
        # When center=True, the first window_size//2 and last window_size//2 values will be NaN
        rolling_var = pd.Series(values).rolling(window=window_size, center=True).var()
        var_mean = rolling_var.mean()
        var_std = rolling_var.std()
        threshold = var_mean + 2 * var_std if not np.isnan(var_std) and var_std > 0 else var_mean * 2
        
        # Create mask for changepoints (high variance regions)
        changepoints_mask = (rolling_var > threshold) & (~np.isnan(rolling_var))
        
        changepoint_dates = []
        in_changepoint = False
        for idx, is_cp in enumerate(changepoints_mask):
            if is_cp and not in_changepoint:
                # When we detect high variance at index idx, the variance is calculated from
                # a window centered at idx (spanning idx-window_size//2 to idx+window_size//2).
                # The actual changepoint likely occurred at the start of this window.
                # 
                # Adjust the index to account for the centered window:
                # Use idx - window_size//2 to get closer to where the change actually occurred
                adjusted_idx = max(0, idx - window_size // 2)
                
                if adjusted_idx < len(dates):
                    # Get the date at the adjusted changepoint index
                    changepoint_date = pd.Timestamp(dates[adjusted_idx])
                    changepoint_date_date = changepoint_date.date()
                    
                    # Get previous date (the day before the changepoint)
                    # This is what Prophet needs - the date before the change occurs
                    previous_date_date = None
                    if adjusted_idx > 0:
                        previous_date = pd.Timestamp(dates[adjusted_idx - 1])
                        previous_date_date = previous_date.date()
                    
                    # Store as tuple: (changepoint_date, previous_date)
                    # For Prophet, we use previous_date as the changepoint date
                    # since the change occurs between previous_date and changepoint_date
                    changepoint_dates.append((changepoint_date_date, previous_date_date))
                in_changepoint = True
            elif not is_cp:
                in_changepoint = False
        
        return len(changepoint_dates), changepoint_dates
    except Exception as e:
        print(f"Error in changepoint detection: {e}")
        import traceback
        traceback.print_exc()
        return 0, []


def generate_test_data_with_changepoints(
    start_date: date,
    num_days: int,
    changepoint_dates: list,
    base_value: float = 100.0,
    noise_level: float = 5.0,
    missing_probability: float = 0.1
):
    """
    Generate synthetic time series data with known changepoints and missing points.
    
    Args:
        start_date: Starting date for the time series
        num_days: Number of days to generate
        changepoint_dates: List of dates where changepoints occur (trend changes)
        base_value: Base value for the time series
        noise_level: Standard deviation of noise
        missing_probability: Probability of missing data points
    
    Returns:
        DataFrame with 'ds' and 'y' columns
    """
    dates = [start_date + timedelta(days=i) for i in range(num_days)]
    values = []
    
    current_trend = 0.0
    trend_changes = {pd.Timestamp(d).date(): True for d in changepoint_dates}
    
    for i, dt in enumerate(dates):
        dt_date = dt.date() if isinstance(dt, date) else dt
        
        # Check if this is a changepoint date
        if dt_date in trend_changes:
            # Change the trend at this point
            current_trend = np.random.choice([-2.0, -1.0, 1.0, 2.0])  # Random trend change
        
        # Generate value with trend
        value = base_value + current_trend * i + np.random.normal(0, noise_level)
        
        # Add missing values randomly
        if np.random.random() < missing_probability:
            values.append(np.nan)
        else:
            values.append(value)
    
    df = pd.DataFrame({
        'ds': dates,
        'y': values
    })
    
    return df


def test_changepoint_detection():
    """Test changepoint detection with various scenarios."""
    
    print("=" * 80)
    print("Testing Changepoint Detection Function")
    print("=" * 80)
    
    # Test 1: Simple case with one changepoint, no missing data
    print("\nTest 1: Single changepoint, no missing data")
    print("-" * 80)
    np.random.seed(42)  # For reproducibility
    start_date = date(2024, 1, 1)
    num_days = 100
    known_changepoint = date(2024, 2, 15)  # Day 45 (0-indexed: Jan 1 = day 0)
    
    # Create data with a clear changepoint - sudden jump in value
    dates = [start_date + timedelta(days=i) for i in range(num_days)]
    values = []
    for i in range(num_days):
        if i < 45:
            values.append(100.0 + i * 0.5 + np.random.normal(0, 2))  # Slow upward trend
        else:
            # Sudden jump and different trend
            values.append(150.0 + (i - 45) * 2.0 + np.random.normal(0, 2))  # Faster upward trend
    
    df1 = pd.DataFrame({'ds': dates, 'y': values})
    
    # Debug: show data structure
    print(f"Original dataframe length: {len(df1)}")
    print(f"Missing values: {df1['y'].isna().sum()}")
    
    count1, dates1 = feature_changepoint_count_and_dates(df1)
    
    print(f"\nKnown changepoint: {known_changepoint} (day {known_changepoint.toordinal() - start_date.toordinal()})")
    print(f"Detected changepoints: {count1}")
    for i, (cp_date, prev_date) in enumerate(dates1):
        print(f"  Changepoint {i+1}: detected_date={cp_date}, previous_date={prev_date}")
        days_diff = (cp_date - known_changepoint).days
        print(f"    Difference from known: {days_diff} days")
        if prev_date:
            print(f"    Using previous_date for Prophet: {prev_date} (diff: {(prev_date - known_changepoint).days} days)")
    
    # Test 2: Multiple changepoints with missing data
    print("\n\nTest 2: Multiple changepoints with missing data")
    print("-" * 80)
    start_date = date(2024, 1, 1)
    num_days = 200
    known_changepoints = [date(2024, 2, 1), date(2024, 3, 15), date(2024, 4, 30)]
    
    dates = [start_date + timedelta(days=i) for i in range(num_days)]
    values = []
    current_trend = 0.5
    
    for i in range(num_days):
        current_date = dates[i].date() if isinstance(dates[i], date) else dates[i]
        
        # Check for changepoints
        if current_date in known_changepoints:
            current_trend = -current_trend * 1.5  # Reverse and increase trend
        
        value = 100.0 + current_trend * i + np.random.normal(0, 3)
        
        # Add missing values (10% probability)
        if np.random.random() < 0.1:
            values.append(np.nan)
        else:
            values.append(value)
    
    df2 = pd.DataFrame({'ds': dates, 'y': values})
    count2, dates2 = feature_changepoint_count_and_dates(df2)
    
    print(f"Known changepoints: {known_changepoints}")
    print(f"Detected changepoints: {count2}")
    for i, (cp_date, prev_date) in enumerate(dates2):
        print(f"  Changepoint {i+1}: detected_date={cp_date}, previous_date={prev_date}")
        # Find closest known changepoint
        closest = min(known_changepoints, key=lambda x: abs((cp_date - x).days))
        days_diff = (cp_date - closest).days
        print(f"    Closest known: {closest}, difference: {days_diff} days")
    
    # Test 3: Edge case - changepoint near the beginning
    print("\n\nTest 3: Changepoint near beginning of series")
    print("-" * 80)
    start_date = date(2024, 1, 1)
    num_days = 100
    known_changepoint = date(2024, 1, 10)  # Day 9
    
    dates = [start_date + timedelta(days=i) for i in range(num_days)]
    values = []
    for i in range(num_days):
        if i < 9:
            values.append(100.0 + np.random.normal(0, 2))
        else:
            values.append(150.0 + (i - 9) * 1.0 + np.random.normal(0, 2))
    
    df3 = pd.DataFrame({'ds': dates, 'y': values})
    count3, dates3 = feature_changepoint_count_and_dates(df3)
    
    print(f"Known changepoint: {known_changepoint}")
    print(f"Detected changepoints: {count3}")
    for i, (cp_date, prev_date) in enumerate(dates3):
        print(f"  Changepoint {i+1}: detected_date={cp_date}, previous_date={prev_date}")
        days_diff = (cp_date - known_changepoint).days
        print(f"    Difference from known: {days_diff} days")
    
    # Test 4: Detailed alignment test with missing data
    print("\n\nTest 4: Detailed alignment test with missing data")
    print("-" * 80)
    np.random.seed(123)  # Different seed
    start_date = date(2024, 1, 1)
    num_days = 150
    known_changepoint = date(2024, 2, 20)  # Day 50
    
    dates = [start_date + timedelta(days=i) for i in range(num_days)]
    values = []
    
    # Create data with missing points around the changepoint
    for i in range(num_days):
        if i < 50:
            values.append(100.0 + i * 0.3 + np.random.normal(0, 1.5))
        else:
            # Sudden change at day 50
            values.append(120.0 + (i - 50) * 1.5 + np.random.normal(0, 1.5))
        
        # Make specific dates missing (around changepoint and scattered)
        if i in [48, 49, 51, 52] or np.random.random() < 0.05:
            values[-1] = np.nan
    
    df4 = pd.DataFrame({'ds': dates, 'y': values})
    
    # Show data around changepoint
    print(f"Known changepoint: {known_changepoint} (day {known_changepoint.toordinal() - start_date.toordinal()})")
    print(f"Original dataframe length: {len(df4)}")
    print(f"Missing values: {df4['y'].isna().sum()}")
    
    print(f"\nData around changepoint (showing valid values only):")
    df_clean = df4[~(df4['y'].isna() | np.isinf(df4['y']))].reset_index(drop=True)
    print(f"Cleaned dataframe length: {len(df_clean)}")
    
    changepoint_idx_in_clean = None
    for idx, row in df_clean.iterrows():
        if row['ds'].date() == known_changepoint:
            changepoint_idx_in_clean = idx
            break
    
    if changepoint_idx_in_clean is not None:
        start_show = max(0, changepoint_idx_in_clean - 15)
        end_show = min(len(df_clean), changepoint_idx_in_clean + 15)
        print(f"  Showing indices {start_show} to {end_show} in cleaned data:")
        print(f"  Known changepoint is at cleaned index: {changepoint_idx_in_clean}")
        for idx in range(start_show, end_show):
            marker = " <-- KNOWN CHANGEPOINT" if df_clean.iloc[idx]['ds'].date() == known_changepoint else ""
            print(f"    idx={idx:3d}, date={df_clean.iloc[idx]['ds'].date()}, value={df_clean.iloc[idx]['y']:.2f}{marker}")
    
    # Also show rolling variance calculation
    if len(df_clean) >= 20:
        values_clean = df_clean['y'].values
        window_size = min(20, len(values_clean) // 4)
        rolling_var = pd.Series(values_clean).rolling(window=window_size, center=True).var()
        var_mean = rolling_var.mean()
        var_std = rolling_var.std()
        threshold = var_mean + 2 * var_std if not np.isnan(var_std) and var_std > 0 else var_mean * 2
        
        print(f"\n  Rolling variance around changepoint (window_size={window_size}):")
        if changepoint_idx_in_clean is not None:
            start_var = max(0, changepoint_idx_in_clean - 10)
            end_var = min(len(rolling_var), changepoint_idx_in_clean + 10)
            for idx in range(start_var, end_var):
                var_val = rolling_var.iloc[idx] if idx < len(rolling_var) else np.nan
                is_high = not np.isnan(var_val) and var_val > threshold
                marker = " <-- HIGH VAR" if is_high else ""
                marker2 = " <-- KNOWN CP" if df_clean.iloc[idx]['ds'].date() == known_changepoint else ""
                print(f"    idx={idx:3d}, rolling_var={var_val:.2f}, threshold={threshold:.2f}{marker}{marker2}")
    
    count4, dates4 = feature_changepoint_count_and_dates(df4)
    print(f"\nDetected changepoints: {count4}")
    for i, (cp_date, prev_date) in enumerate(dates4):
        print(f"  Changepoint {i+1}: detected_date={cp_date}, previous_date={prev_date}")
        days_diff = (cp_date - known_changepoint).days
        print(f"    Difference from known: {days_diff} days")
        if prev_date:
            prev_diff = (prev_date - known_changepoint).days
            print(f"    Using previous_date for Prophet: {prev_date} (diff: {prev_diff} days)")
    
    # Test 5: Very explicit test with exact known positions
    print("\n\nTest 5: Explicit indexing test")
    print("-" * 80)
    np.random.seed(999)
    start_date = date(2024, 1, 1)
    num_days = 80
    known_changepoint_idx = 40  # Exact index in original array
    known_changepoint = start_date + timedelta(days=known_changepoint_idx)
    
    dates = [start_date + timedelta(days=i) for i in range(num_days)]
    values = []
    
    # Create very clear changepoint - sudden jump
    for i in range(num_days):
        if i < known_changepoint_idx:
            values.append(100.0 + np.random.normal(0, 1))
        else:
            values.append(200.0 + np.random.normal(0, 1))  # Sudden jump to 200
    
    # Add some missing values (but not at the changepoint)
    missing_indices = [10, 15, 25, 30, 55, 60]
    for idx in missing_indices:
        if idx < len(values):
            values[idx] = np.nan
    
    df5 = pd.DataFrame({'ds': dates, 'y': values})
    
    print(f"Known changepoint: {known_changepoint} (original index: {known_changepoint_idx})")
    print(f"Original dataframe length: {len(df5)}")
    print(f"Missing values: {df5['y'].isna().sum()}")
    
    # Show original data structure
    print(f"\nOriginal data (first 10 and around changepoint):")
    for i in [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, known_changepoint_idx-2, known_changepoint_idx-1, 
              known_changepoint_idx, known_changepoint_idx+1, known_changepoint_idx+2]:
        if i < len(df5):
            marker = " <-- KNOWN CP" if i == known_changepoint_idx else ""
            missing = " (MISSING)" if pd.isna(df5.iloc[i]['y']) else ""
            print(f"  orig_idx={i:3d}, date={df5.iloc[i]['ds'].date()}, value={df5.iloc[i]['y']}{missing}{marker}")
    
    # Show cleaned data
    df_clean = df5[~(df5['y'].isna() | np.isinf(df5['y']))].reset_index(drop=True)
    print(f"\nCleaned dataframe length: {len(df_clean)}")
    
    # Find where known changepoint is in cleaned data
    known_cp_in_clean = None
    for idx, row in df_clean.iterrows():
        if row['ds'].date() == known_changepoint:
            known_cp_in_clean = idx
            break
    
    if known_cp_in_clean is not None:
        print(f"Known changepoint is at cleaned index: {known_cp_in_clean}")
        print(f"\nCleaned data around changepoint:")
        start_show = max(0, known_cp_in_clean - 5)
        end_show = min(len(df_clean), known_cp_in_clean + 5)
        for idx in range(start_show, end_show):
            marker = " <-- KNOWN CP" if idx == known_cp_in_clean else ""
            print(f"  clean_idx={idx:3d}, date={df_clean.iloc[idx]['ds'].date()}, value={df_clean.iloc[idx]['y']:.2f}{marker}")
    
    count5, dates5 = feature_changepoint_count_and_dates(df5)
    print(f"\nDetected changepoints: {count5}")
    for i, (cp_date, prev_date) in enumerate(dates5):
        print(f"  Changepoint {i+1}: detected_date={cp_date}, previous_date={prev_date}")
        days_diff = (cp_date - known_changepoint).days
        print(f"    Difference from known: {days_diff} days")
        if prev_date:
            prev_diff = (prev_date - known_changepoint).days
            print(f"    Using previous_date for Prophet: {prev_date} (diff: {prev_diff} days)")
    
    print("\n" + "=" * 80)
    print("Test Summary")
    print("=" * 80)
    print(f"Test 1: {'PASS' if count1 > 0 and any(abs((cp - known_changepoint).days) <= 5 for cp, _ in dates1) else 'FAIL'}")
    print(f"Test 2: {'PASS' if count2 >= 2 else 'FAIL'}")
    print(f"Test 3: {'PASS' if count3 > 0 else 'FAIL'}")
    print(f"Test 4: {'PASS' if count4 > 0 and any(abs((cp - known_changepoint).days) <= 10 for cp, _ in dates4) else 'FAIL'}")
    print(f"Test 5: {'PASS' if count5 > 0 and any(abs((cp - known_changepoint).days) <= 5 for cp, _ in dates5) else 'FAIL'}")


if __name__ == "__main__":
    test_changepoint_detection()
