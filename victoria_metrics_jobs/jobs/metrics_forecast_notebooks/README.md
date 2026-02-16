# Metrics Forecast Notebooks Job

## Overview

The `metrics_forecast_notebooks` job executes Jupyter notebooks programmatically for time series forecasting. Each notebook contains its own selector logic and model configuration, uses darts wrappers for forecasting models (Prophet or ARIMA), and saves results to the same database tables as the `metrics_forecast` job.

## Key Features

- **Self-contained notebooks**: Each notebook has hardcoded selector logic and model parameters
- **Darts integration**: Uses darts library wrappers for Prophet and ARIMA models
- **Date-partitioned storage**: Executed notebooks stored in YYYY/MM/DD structure (same as metrics)
- **HTML rendering**: High-quality static HTML versions generated for easy viewing
- **HTTP serving**: Executed notebooks accessible via `/notebooks` endpoint
- **Automatic cleanup**: Old notebooks cleaned up using same retention policy as metrics

## Architecture

```
metrics_forecast_notebooks Job
├── Scans notebooks/ folder for .ipynb files
├── Executes each notebook (using papermill)
├── Converts to HTML (using nbconvert)
└── Stores outputs in notebooks_output_dir/YYYY/MM/DD/
```

Each notebook:
- Queries VictoriaMetrics using its own selector
- Uses darts wrappers for forecasting
- Saves forecasts to vm_metric_data & vm_metric_metadata tables

## Configuration

### Job Configuration

```yaml
metrics_forecast_notebooks:
  id: metrics_forecast_notebooks
  name: Metrics Forecast Notebooks
  enabled: true
  script: "python"
  args: ["-m", "victoria_metrics_jobs.jobs.metrics_forecast_notebooks", "--config", "...", "--job-id", "metrics_forecast_notebooks"]
  schedule:
    type: cron
    args:
      hour: 8
      minute: 30
  job_type: metrics_forecast_notebooks
  victoria_metrics: ${environments.local.victoria_metrics}
  database: ${environments.local.database}
  notebooks_directory: "notebooks"  # Input: source notebooks to execute
  cutoff_hour: 6  # Business date cutoff hour
```

### Metrics Configuration

```yaml
environments:
  local:
    metrics:
      notebooks_output_directory: /var/lib/scheduler/notebooks_output
      notebooks_retention_days: 14  # Same as metrics retention by default
```

## Usage

### Creating Notebooks

1. Create a new `.ipynb` file in the `victoria_metrics_jobs/jobs/metrics_forecast_notebooks/notebooks/` directory
2. Add a **parameters cell** (first cell) with default parameter dictionary - this allows papermill to inject values
3. Use parameters or environment variables for VM and DB configuration (see Configuration section below)
4. Include your selector logic and model parameters (can be hardcoded in notebook or passed via parameters)
5. Use darts wrappers from the same directory: `darts_prophet_wrapper.py` or `darts_arima_wrapper.py`
6. See example notebooks: `prophet_forecast.ipynb` and `arima_forecast.ipynb` in the job's notebooks directory

### Configuration: Parameters vs Environment Variables

Notebooks support two configuration methods:

#### 1. When executed by the job (via papermill)
- The job automatically injects configuration parameters:
  - `vm_query_url`: Victoria Metrics query URL
  - `vm_token`: Victoria Metrics authentication token
  - `db_connection_string`: Full PostgreSQL connection string
- These override any defaults in the parameters cell

#### 2. When running locally (direct execution)
- Use environment variables:
  ```bash
  export VM_QUERY_URL="http://victoria-metrics:8428"
  export VM_TOKEN="your-token"
  export DB_CONNECTION_STRING="postgresql://user:pass@host:port/dbname?sslmode=prefer"
  export SELECTOR='{job="extractor"}'
  export HISTORY_DAYS=365
  export FORECAST_HORIZON_DAYS=20
  ```
- Or edit the parameters cell directly in the notebook
- The notebook checks parameters first, then falls back to environment variables

#### Example Parameters Cell
```python
# Parameters cell (tagged for papermill injection)
parameters = {
    'vm_query_url': '',
    'vm_token': '',
    'db_connection_string': '',
    'selector': '',
    # ... other parameters
}
```

#### Example Configuration Usage in Notebook
```python
import os
VM_QUERY_URL = parameters.get('vm_query_url') or os.getenv('VM_QUERY_URL', 'default-value')
DB_CONNECTION_STRING = parameters.get('db_connection_string') or os.getenv('DB_CONNECTION_STRING', '')
```

### Running the Job

```bash
# Run directly
python -m victoria_metrics_jobs.jobs.metrics_forecast_notebooks \
  --config victoria_metrics_jobs/victoria_metrics_jobs.yml \
  --job-id metrics_forecast_notebooks

# Or via scheduler (if configured)
# The job will run on schedule and execute all notebooks in the notebooks/ directory
```

### Viewing Executed Notebooks

Executed notebooks are accessible via HTTP:

- **Directory listing**: `http://localhost:8000/notebooks`
- **Raw notebook**: `http://localhost:8000/notebooks/YYYY/MM/DD/notebook_name_timestamp.ipynb`
- **HTML version**: `http://localhost:8000/notebooks/YYYY/MM/DD/notebook_name_timestamp.html`

## Output Structure

Notebooks are stored in date-partitioned directories:

```
notebooks_output_dir/
└── YYYY/
    └── MM/
        └── DD/
            ├── notebook_name_20240101_120000.ipynb
            └── notebook_name_20240101_120000.html
```

This structure:
- Matches metrics file structure for consistency
- Enables reuse of cleanup logic
- Organizes notebooks by execution date

## Database Integration

Notebooks write forecasts to the same tables as `metrics_forecast`:

- **vm_metric_metadata**: Stores metric metadata (job_idx, metric_id, job_id, metric_name, metric_labels)
- **vm_metric_data**: Stores forecast values (job_idx, metric_id, metric_timestamp, metric_value)

Forecast types saved:
- `trend` (yhat) - Main forecast
- `lower` (yhat_lower) - Lower bound
- `upper` (yhat_upper) - Upper bound

Each forecast type becomes a separate timeseries with `forecast_type` in metric_labels.

## Cleanup

The `metrics_cleanup` job automatically cleans up old notebooks using the same retention policy as metrics files. Old notebook directories (older than `notebooks_retention_days`) are removed or archived.

## Dependencies

- `papermill` - For executing notebooks
- `darts` - For Prophet and ARIMA wrappers
- `nbconvert` - For converting notebooks to HTML (includes `jupyter-core` as a dependency)
- `pygments` - For code syntax highlighting

## Example Notebooks

- `victoria_metrics_jobs/jobs/metrics_forecast_notebooks/notebooks/prophet_forecast.ipynb` - Example using Prophet via darts
- `victoria_metrics_jobs/jobs/metrics_forecast_notebooks/notebooks/arima_forecast.ipynb` - Example using ARIMA via darts

## See Also

- `notebooks/README.md` - Guide for creating forecast notebooks
- `victoria_metrics_jobs/jobs/metrics_forecast/README.md` - Related metrics_forecast job documentation
- `database/vm_metric_data.sql` - Database schema documentation

