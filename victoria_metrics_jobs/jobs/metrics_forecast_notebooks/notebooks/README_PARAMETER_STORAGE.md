# Saving Model Parameters to Database

## Overview

This document explains how to save model parameters to the database for each notebook run, enabling parameter auditing and forecast reproducibility.

## Database Schema

The `vm_forecast_job` table is used to track each forecast run and store model parameters:

```sql
CREATE TABLE vm_forecast_job (
    run_id BIGSERIAL PRIMARY KEY,
    job_id VARCHAR(255),
    selection_value TEXT,
    prophet_config JSONB,          -- Model parameters (used for all model types)
    prophet_fit_config JSONB,      -- Model fit parameters (optional)
    config_source VARCHAR(255),
    history_days INTEGER,
    forecast_horizon_days INTEGER,
    min_history_points INTEGER,
    business_date DATE,
    started_at TIMESTAMP WITH TIME ZONE,
    status VARCHAR(50),
    ...
);
```

**Note:** The existing schema uses `prophet_config` field name, but it can store parameters for any model type. The model type is included in the JSON structure.

## Recommended Approach

### 1. Create Run Record Before Saving Forecasts

Create a run record using `create_forecast_run_record()` function:

```python
from database_helpers import create_forecast_run_record, save_forecasts_to_database

# Create run record with model parameters
run_id = create_forecast_run_record(
    conn=conn,
    job_id="metrics_forecast_notebooks",  # Or use a job-specific identifier
    selection_value=SELECTOR,              # Your PromQL selector
    model_type="prophet",                  # or "arima"
    model_config=PROPHET_PARAMS,           # Your model parameters dict
    model_fit_config=PROPHET_FIT_PARAMS,   # Optional fit parameters
    history_days=HISTORY_DAYS,
    forecast_horizon_days=FORECAST_HORIZON_DAYS,
    min_history_points=MIN_HISTORY_POINTS,
    config_source="notebook",              # Indicates config came from notebook
)

# Save forecasts with run_id
rows_inserted = save_forecasts_to_database(
    conn=conn,
    metric_name=series_info['metric_name'],
    labels=series_info['labels'],
    forecast_df=forecast_df,
    forecast_types=forecast_types,
    run_id=run_id,  # Link forecasts to parameter record
)
```

### 2. For Prophet Notebooks

```python
# After generating forecast_df
run_id = create_forecast_run_record(
    conn=conn,
    job_id="metrics_forecast_notebooks",
    selection_value=SELECTOR,
    model_type="prophet",
    model_config=PROPHET_PARAMS,
    model_fit_config=PROPHET_FIT_PARAMS,
    history_days=HISTORY_DAYS,
    forecast_horizon_days=FORECAST_HORIZON_DAYS,
    min_history_points=MIN_HISTORY_POINTS,
)

rows_inserted = save_forecasts_to_database(
    conn=conn,
    metric_name=series_info['metric_name'],
    labels=series_info['labels'],
    forecast_df=forecast_df,
    forecast_types=forecast_types,
    run_id=run_id,
)
```

### 3. For ARIMA Notebooks

```python
# After generating forecast_df
run_id = create_forecast_run_record(
    conn=conn,
    job_id="metrics_forecast_notebooks",
    selection_value=SELECTOR,
    model_type="arima",
    model_config=ARIMA_PARAMS,  # e.g., {'p': 1, 'd': 1, 'q': 1}
    history_days=HISTORY_DAYS,
    forecast_horizon_days=FORECAST_HORIZON_DAYS,
    min_history_points=MIN_HISTORY_POINTS,
)

rows_inserted = save_forecasts_to_database(
    conn=conn,
    metric_name=series_info['metric_name'],
    labels=series_info['labels'],
    forecast_df=forecast_df,
    forecast_types=forecast_types,
    run_id=run_id,
)
```

## How Parameters Are Stored

The `create_forecast_run_record` function stores parameters in JSONB format:

**For Prophet:**
```json
{
  "model_type": "prophet",
  "yearly_seasonality": true,
  "weekly_seasonality": false,
  "daily_seasonality": false,
  "seasonality_mode": "additive",
  "changepoint_prior_scale": 0.05
}
```

**For ARIMA:**
```json
{
  "model_type": "arima",
  "p": 1,
  "d": 1,
  "q": 1
}
```

## Querying Parameter History

### View Recent Runs with Parameters

```sql
SELECT 
    run_id,
    run_timestamp,
    selection_value,
    prophet_config->>'model_type' as model_type,
    prophet_config,
    history_days,
    forecast_horizon_days,
    status
FROM vm_forecast_job
WHERE job_id = 'metrics_forecast_notebooks'
ORDER BY run_timestamp DESC
LIMIT 10;
```

### Find Forecasts by Run ID

```sql
SELECT 
    vmd.metric_timestamp,
    vmd.metric_value,
    vmm.metric_name,
    vmm.metric_labels,
    vfj.prophet_config as model_params
FROM vm_metric_data vmd
JOIN vm_metric_metadata vmm ON vmd.job_idx = vmm.job_idx AND vmd.metric_id = vmm.metric_id
LEFT JOIN vm_forecast_job vfj ON vmd.run_id = vfj.run_id
WHERE vmd.run_id = :run_id
ORDER BY vmd.metric_timestamp;
```

### Compare Parameters Across Runs

```sql
SELECT 
    run_id,
    run_timestamp,
    prophet_config->>'model_type' as model_type,
    prophet_config->'p' as arima_p,        -- For ARIMA
    prophet_config->'d' as arima_d,
    prophet_config->'q' as arima_q,
    prophet_config->>'changepoint_prior_scale' as prophet_changepoint,  -- For Prophet
    status
FROM vm_forecast_job
WHERE selection_value = '{job="extractor"}'
ORDER BY run_timestamp DESC;
```

## Benefits

1. **Reproducibility**: Know exactly which parameters were used for each forecast
2. **Auditing**: Track parameter changes over time
3. **Debugging**: Identify which parameter sets work best
4. **A/B Testing**: Compare forecast quality across different parameter sets
5. **Compliance**: Maintain a record of all forecasting decisions

## Future Schema Improvements (Optional)

For better multi-model support, consider extending the schema:

```sql
ALTER TABLE vm_forecast_job 
ADD COLUMN model_type VARCHAR(50);  -- Explicit model type column

ALTER TABLE vm_forecast_job
ADD COLUMN model_config JSONB;      -- Generic model config (alternative to prophet_config)

-- Migrate existing data
UPDATE vm_forecast_job 
SET model_type = prophet_config->>'model_type';
```

This would make the schema more explicit and supportable for multiple model types.

