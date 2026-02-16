# Changepoint Detection Fix

## Problem
The changepoint detection function is returning wrong dates because of an incorrect index adjustment.

## Current (Incorrect) Logic
```python
adjusted_idx = max(0, idx - window_size // 2)
```

This adjusts the index backward by `window_size//2`, which is too aggressive and causes wrong dates.

## Root Cause
When we detect high variance at index `idx`:
- The rolling variance at `idx` is calculated from a window centered at `idx` (spanning `idx - window_size//2` to `idx + window_size//2`)
- We use `in_changepoint` flag to capture the FIRST index where variance becomes high
- This means `idx` is already the first detection point
- The changepoint likely occurred just before `idx`, not `idx - window_size//2` positions before

## Fix
Remove the index adjustment and use the detection index directly:

```python
# OLD (WRONG):
adjusted_idx = max(0, idx - window_size // 2)
if adjusted_idx < len(dates):
    changepoint_date = pd.Timestamp(dates[adjusted_idx])
    if adjusted_idx > 0:
        previous_date = pd.Timestamp(dates[adjusted_idx - 1])

# NEW (CORRECT):
if idx < len(dates):
    changepoint_date = pd.Timestamp(dates[idx])
    if idx > 0:
        previous_date = pd.Timestamp(dates[idx - 1])
```

## Location
File: `victoria_metrics_jobs/jobs/metrics_forecast_notebooks/notebooks/tsfel_prophet_forecast.ipynb`
Cell: 10
Function: `feature_changepoint_count_and_dates`

## Test
Run `test_changepoint_simple.py` to verify the fix works correctly.
