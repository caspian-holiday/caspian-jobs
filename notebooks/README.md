# Notebooks Directory

This directory contains Jupyter notebooks for forecasting and parameter tuning.

## Notebook Types

### 1. Executable Forecast Notebooks

These notebooks are executed by the `metrics_forecast_notebooks` job. Example notebooks are located in:
- `victoria_metrics_jobs/jobs/metrics_forecast_notebooks/notebooks/prophet_forecast.ipynb` - Example Prophet forecast notebook
- `victoria_metrics_jobs/jobs/metrics_forecast_notebooks/notebooks/arima_forecast.ipynb` - Example ARIMA forecast notebook

**Creating Executable Notebooks:**

1. Create a new `.ipynb` file in `victoria_metrics_jobs/jobs/metrics_forecast_notebooks/notebooks/` directory
2. Hardcode your selector and model parameters in the configuration cell
3. Use darts wrappers from the same directory: `darts_prophet_wrapper.py` or `darts_arima_wrapper.py` (both are in the job's notebooks directory)
4. Include database saving logic (see example notebooks)
5. The job will automatically discover and execute your notebook from the job's notebooks directory

**Key Requirements:**
- Notebook must be executable standalone (no user interaction)
- Use hardcoded configuration (selector, model params, etc.)
- Must save results to database (vm_metric_data and vm_metric_metadata tables)
- Should handle errors gracefully

See example notebooks for complete implementation patterns.

### 2. Interactive Tuning Notebooks

- `prophet_parameter_tuning.ipynb` - Interactive tool for exploring and tuning Prophet parameters

## Darts Wrappers

Helper modules for using forecasting models via darts. These are located in the job's notebooks directory:
- `victoria_metrics_jobs/jobs/metrics_forecast_notebooks/notebooks/darts_prophet_wrapper.py` - Prophet model wrapper
- `victoria_metrics_jobs/jobs/metrics_forecast_notebooks/notebooks/darts_arima_wrapper.py` - ARIMA model wrapper

These wrappers provide:
- DataFrame to TimeSeries conversion
- Business day data preparation
- Model training and forecasting
- Forecast format conversion for database saving

---

# Prophet Parameter Tuning Notebook

## Overview

The `prophet_parameter_tuning.ipynb` notebook is an interactive tool for exploring and tuning Prophet forecast parameters before adding them to the `vm_forecast_config` database table.

## Purpose

Use this notebook to:
- **Query** historical metrics from Victoria Metrics
- **Test** different Prophet parameter combinations
- **Visualize** forecast results side-by-side
- **Compare** forecast statistics and characteristics
- **Generate SQL** to add optimal configuration to database

## Usage

### 1. Install Dependencies

```bash
pip install pandas numpy matplotlib seaborn prophet prometheus-api-client jupyter
```

### 2. Start Jupyter

```bash
cd /home/victor/code/victoria_metrics_jobs
jupyter notebook notebooks/prophet_parameter_tuning.ipynb
```

### 3. Configure the Notebook

In cell 3, update these variables:

```python
VM_QUERY_URL = "http://your-victoria-metrics:8428"
VM_TOKEN = "your_token_here"  # If needed
SELECTOR = '{job="your_job_name"}'  # Or any PromQL selector
HISTORY_DAYS = 365
FORECAST_DAYS = 20
```

### 4. Run All Cells

Execute all cells in order (Cell → Run All)

### 5. Analyze Results

The notebook will:
- Fetch historical data for your selector
- Train 5 different Prophet models with varying parameters
- Show comparison plots
- Generate statistics table

### 6. Choose Best Configuration

Based on visual analysis:
- Look for forecasts that capture trends without overfitting
- Check if seasonal patterns are represented accurately
- Verify uncertainty bands are reasonable

### 7. Generate SQL

In the last cell, set:
```python
selected_config = 'flexible_trend'  # Your chosen config
```

Run the cell to generate SQL like:
```sql
INSERT INTO vm_forecast_config (
    selection_type,
    selection_value,
    prophet_params,
    ...
) VALUES (
    'selector',
    '{job="extractor"}',
    '{"changepoint_prior_scale": 0.5, ...}'::jsonb,
    ...
);
```

### 8. Add to Database

Execute the generated SQL in your database:
```bash
psql -d forecasts_db -c "INSERT INTO vm_forecast_config ..."
```

## Pre-Configured Parameter Sets

The notebook tests 5 configurations:

### 1. **Default**
- Moderate flexibility
- Standard yearly seasonality
- Good starting point

### 2. **Flexible Trend**
- High `changepoint_prior_scale` (0.5)
- More changepoints (50)
- For volatile metrics

### 3. **Rigid Trend**
- Low `changepoint_prior_scale` (0.001)
- Very smooth trend
- For stable metrics

### 4. **Strong Seasonality**
- High `seasonality_prior_scale` (50.0)
- Emphasizes yearly patterns
- For metrics with clear seasonal cycles

### 5. **Multiplicative Seasonality**
- Seasonal effects scale with trend
- For growing metrics where patterns scale proportionally

## Customizing Configurations

You can add your own configurations in cell 10:

```python
PARAM_CONFIGS['my_custom'] = {
    'name': 'My Custom Config',
    'params': {
        'yearly_seasonality': True,
        'changepoint_prior_scale': 0.2,
        'seasonality_prior_scale': 25.0,
        # ... any Prophet parameters
    },
    'fit': {
        'algorithm': 'Newton',
        'iterations': 1000
    }
}
```

## Tips

### For Volatile Metrics:
- Increase `changepoint_prior_scale` (0.1-0.5)
- Increase `n_changepoints` (30-50)
- Use flexible trend configuration

### For Stable Metrics:
- Decrease `changepoint_prior_scale` (0.001-0.01)
- Use rigid trend configuration
- May increase seasonality strength

### For Growing Businesses:
- Use `seasonality_mode: 'multiplicative'`
- Seasonal patterns will scale with growth
- Good for revenue, user counts, etc.

### For Seasonal Patterns:
- Increase `seasonality_prior_scale` (20-100)
- Ensure `yearly_seasonality: True`
- Use strong seasonality configuration

## Troubleshooting

### "No data found"
- Check your `SELECTOR` syntax (use double quotes in PromQL)
- Verify metrics exist in Victoria Metrics
- Adjust `HISTORY_DAYS` if needed

### "Model training failed"
- Check for constant values (no variation)
- Verify sufficient data points (>30 recommended)
- Check for NaN or infinite values

### "Forecast looks wrong"
- Try different parameter configurations
- Adjust `changepoint_prior_scale` first
- Check if data has seasonal patterns

## Next Steps

After tuning parameters:

1. ✅ Generate SQL using the notebook
2. ✅ Execute SQL to add config to `vm_forecast_config`
3. ✅ Run the metrics_forecast job
4. ✅ Query results from `vm_forecasted_metric` table
5. ✅ Monitor forecast quality over time
6. ✅ Adjust parameters as needed (update database record)

## Related Files

- `../database/vm_forecast_config_ddl.sql` - Config table schema
- `../database/vm_forecast_job_ddl.sql` - Run tracking schema
- `../database/vm_forecast_ddl.sql` - Forecast data schema
- `../victoria_metrics_jobs/jobs/metrics_forecast/metrics_forecast.py` - The forecast job
- `../database/MIGRATION_DB_DRIVEN.md` - Migration guide

## Support

For more information on Prophet parameters, see:
- https://facebook.github.io/prophet/docs/quick_start.html
- https://facebook.github.io/prophet/docs/diagnostics.html

