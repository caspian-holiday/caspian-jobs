-- ============================================================================
-- Victoria Metrics Jobs - Forecast Metadata per Metric per Run
-- Table: vm_metrics_forecast_metadata
-- Pure PostgreSQL definition (no TimescaleDB dependencies)
--
-- Prerequisites:
--   - vm_metric_metadata table must exist (vm_metric_metadata.sql)
--   - vm_forecast_job table must exist (vm_forecast_job.sql)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Table: vm_metrics_forecast_metadata
-- Purpose: Stores per-metric, per-run TSFEL features, predictability
--          classification, and Prophet parameters used for forecasting.
--          Enables auditing, reproducibility, and analysis of classification.
-- (job_idx, metric_id) references the forecast metric in vm_metric_metadata
-- (same job_idx, metric_id used when writing forecast points to vm_metric_data).
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS vm_metrics_forecast_metadata (
    job_idx BIGINT NOT NULL,
    metric_id INT NOT NULL,
    run_id BIGINT NOT NULL,

    -- Single JSONB payload: tsfel_features, classification, prophet_params
    -- tsfel_features: stat/TSFEL features from extract_tsfel_features()
    -- classification: { "category": "Predictable"|"Low Predictability"|"Not Suitable", "reason": "..." }
    -- prophet_params: Prophet parameters for this metric, or null when Not Suitable
    metadata JSONB NOT NULL,

    PRIMARY KEY (job_idx, metric_id, run_id),
    FOREIGN KEY (job_idx, metric_id) REFERENCES public.vm_metric_metadata(job_idx, metric_id),
    FOREIGN KEY (run_id) REFERENCES public.vm_forecast_job(run_id)
);

-- Index for "all metadata rows for a run"
CREATE INDEX IF NOT EXISTS idx_vm_metrics_forecast_metadata_run_id
    ON vm_metrics_forecast_metadata(run_id);

-- Index for "all runs for this metric"
CREATE INDEX IF NOT EXISTS idx_vm_metrics_forecast_metadata_metric
    ON vm_metrics_forecast_metadata(job_idx, metric_id);

COMMENT ON TABLE vm_metrics_forecast_metadata IS
    'Per-metric, per-run forecast metadata: TSFEL features, classification, and Prophet parameters.';

COMMENT ON COLUMN vm_metrics_forecast_metadata.metadata IS
    'JSON: tsfel_features (object), classification (category, reason), prophet_params (object or null).';
