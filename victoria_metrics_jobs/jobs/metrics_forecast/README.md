# Metrics Forecast Job

Prophet-powered forecasting pipeline that reads historical Victoria Metrics series filtered by a configurable label (default `source`), generates business-day predictions, and publishes the results back to the same cluster. Each forecasted sample preserves every original label and adds a `forecast` label describing the variant (`trend`, `lower`, `upper`, etc.). Timestamps are fabricated at midnight for the target business date and incremented by one second for reruns covering the same horizon, ensuring idempotent writes.

## Key Features

- **Business-day aware**: Automatically skips weekends for both history windows and forward-looking horizons.
- **Prophet configuration**: Default settings disable weekend seasonality and can be overridden via config.
- **Multiple forecast variants**: Emit `yhat`, `yhat_lower`, `yhat_upper`, or any other Prophet column by configuring `forecast_types`.
- **Prometheus client only**: Uses `prometheus-api-client` for both reads and remote writes to Victoria Metrics gateway.

## Configuration Snippet

```yaml
metrics_forecast:
  id: metrics_forecast
  name: Metrics Forecast
  enabled: true
  script: "python"
  args: ["-m", "victoria_metrics_jobs.jobs.metrics_forecast", "--config", "victoria_metrics_jobs/victoria_metrics_jobs.yml", "--job-id", "metrics_forecast"]
  schedule:
    type: cron
    args:
      hour: 7
      minute: 0

  # Job settings
  job_type: metrics_forecast
  victoria_metrics: ${environments.dev.victoria_metrics}
  source_job_names:
    - apex_collector
  source_label: source
  metric_selectors:
    - "{__name__!=\"\"}"
  history_days: 365          # how many days of history to train on
  history_offset_days: 0     # offset applied to the end of the history window
  forecast_horizon_days: 20  # number of business days to forecast
  min_history_points: 30
  history_step_hours: 24     # sampling resolution for range queries
  cutoff_hour: 6             # derive business date (UTC) before querying
  forecast_types:
    - name: trend
      field: yhat
    - name: lower
      field: yhat_lower
    - name: upper
      field: yhat_upper
  prophet:
    weekly_seasonality: false
    daily_seasonality: false
    yearly_seasonality: true
    seasonality_mode: additive
  prophet_fit:
    algorithm: MAP          # passed to Prophet.fit(...)
    iterations: 500         # forwarded to underlying Stan backend
```

### Metric Selectors & Labels

- `source_job_names` contains the values you expect under `source_label` (default `source`). Override `source_label` if your metrics store the identifier elsewhere (e.g., `region`).
- Each entry in `metric_selectors` can be a raw selector (`metric_name{label="value"}`) or use `$SOURCE`/`$JOB` placeholders (e.g., `requests_total{source="$SOURCE",env="dev"}`).
- When placeholders are omitted, the job automatically injects `source_label="<value from source_job_names>"` into the selector so only matching series are fetched.

### Prophet Tuning

- Use the `prophet` block for constructor-level settings (seasonality, changepoints, growth, etc.).
- Use the `prophet_fit` block to pass keyword arguments directly to `Prophet.fit()`, such as `algorithm`, `iterations`, `warmup`, or any other Stan sampling options.
- For backwards compatibility, if `algorithm` or `iterations` are accidentally placed inside `prophet`, the job will move them to `prophet_fit` automatically.

### Forecast Types

`forecast_types` links Prophet output columns to the value stored in the `forecast` label. For example, the configuration above produces:

- `forecast="trend"` for `yhat`
- `forecast="lower"` for `yhat_lower`
- `forecast="upper"` for `yhat_upper`

### Timestamp Fabrication

For each forecasted business date:

1. Start with midnight (`00:00:00`) timestamp.
2. Query Victoria Metrics for existing samples with the same metric + labels.
3. If entries exist, increment the timestamp by one second relative to the latest stored value.

This guarantees deterministic ordering without overwriting previous runs targeting the same horizon.

### Troubleshooting

**cmdstanpy errors (signal 3221225657, 32212256857, or "terminated by signal"):**

If you encounter cmdstanpy crashes during Prophet fitting (Windows access violations), the job includes automatic retry logic with exponential backoff. Common causes and solutions:

1. **Memory issues**: 
   - Reduce `history_days` to limit training data size (e.g., 180 days instead of 365)
   - The job warns if datasets exceed 10,000 points
   - Close other applications to free memory

2. **Windows compatibility**: 
   - Ensure cmdstanpy and Stan are properly installed: `pip install cmdstanpy` and run `cmdstanpy.install_cmdstan()`
   - On Windows, cmdstanpy may need Visual C++ redistributables
   - Consider running on Linux/WSL if crashes persist

3. **Resource contention**: 
   - The job adds 500ms delays between series to avoid resource conflicts
   - If running multiple forecast jobs in parallel, reduce parallelism
   - Memory is cleaned up after each series (garbage collection)

4. **Data quality**: 
   - The job validates training data (NaN, infinite values) before fitting
   - Very large or sparse datasets may cause issues

5. **Stan compilation**: 
   - First run may be slower as Stan compiles models
   - Subsequent runs should be faster (compiled models are cached)

The job automatically retries up to 3 times with exponential backoff (2s, 4s, 8s) when cmdstanpy crashes are detected. If crashes persist after retries, consider reducing dataset size or running on a different platform.

