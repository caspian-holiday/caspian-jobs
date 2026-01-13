# Fix: Keep Changepoint Dates as Date Objects (Not Timestamps)

## Location
File: `victoria_metrics_jobs/jobs/metrics_forecast_notebooks/notebooks/tsfel_prophet_forecast.ipynb`
Cell: 15
Function: `generate_prophet_params`

## Current Code (WRONG - converts to timestamps)
```python
# Convert date to datetime for Prophet
if isinstance(date_to_use, date):
    changepoint_datetime = pd.Timestamp(date_to_use)
elif isinstance(date_to_use, pd.Timestamp):
    changepoint_datetime = date_to_use
else:
    changepoint_datetime = pd.to_datetime(date_to_use)

# Ensure timezone-naive
if changepoint_datetime.tz is not None:
    changepoint_datetime = changepoint_datetime.tz_localize(None)

changepoint_dates_clean.append(changepoint_datetime)
```

## Fixed Code (CORRECT - keeps as date objects)
```python
# Keep as date object (not timestamp) for Prophet
if isinstance(date_to_use, date):
    changepoint_date_obj = date_to_use
elif isinstance(date_to_use, pd.Timestamp):
    changepoint_date_obj = date_to_use.date()
elif isinstance(date_to_use, datetime):
    changepoint_date_obj = date_to_use.date()
else:
    # Try to convert to date
    changepoint_date_obj = pd.to_datetime(date_to_use).date()

changepoint_dates_clean.append(changepoint_date_obj)
```

## Also Update Legacy Format Handling
Replace:
```python
elif isinstance(cp_entry, date):
    # Handle legacy format (just a date)
    changepoint_datetime = pd.Timestamp(cp_entry)
    if changepoint_datetime.tz is not None:
        changepoint_datetime = changepoint_datetime.tz_localize(None)
    changepoint_dates_clean.append(changepoint_datetime)
```

With:
```python
elif isinstance(cp_entry, date):
    # Handle legacy format (just a date) - keep as date
    changepoint_dates_clean.append(cp_entry)
elif isinstance(cp_entry, pd.Timestamp):
    # Convert timestamp to date
    changepoint_dates_clean.append(cp_entry.date())
elif isinstance(cp_entry, datetime):
    # Convert datetime to date
    changepoint_dates_clean.append(cp_entry.date())
```

## Summary
- Remove all `pd.Timestamp()` conversions
- Keep dates as `date` objects
- If input is already a timestamp/datetime, convert to date using `.date()`
- Prophet will accept date objects in the `changepoints` parameter
