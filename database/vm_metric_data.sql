-- ============================================================================
-- Victoria Metrics Jobs - Metric Data Schema DDL
-- Table: vm_metric_data
-- Pure PostgreSQL definition (no TimescaleDB dependencies)
-- 
-- Prerequisites:
--   - vm_metric_metadata table must exist (created by vm_metric_metadata.sql)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Table: vm_metric_data
-- Purpose: Stores time-series metric values
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS vm_metric_data (
    job_idx BIGINT NOT NULL,
    metric_id INT NOT NULL,
    metric_timestamp TIMESTAMPTZ NOT NULL,
    metric_value DOUBLE PRECISION NOT NULL,
    run_id BIGINT,  -- Reference to vm_forecast_job(run_id) - no FK constraint enforced
    PRIMARY KEY (job_idx, metric_id, metric_timestamp),
    FOREIGN KEY (job_idx, metric_id) REFERENCES public.vm_metric_metadata(job_idx, metric_id)
);

-- Indexes for efficient time-series queries
CREATE INDEX IF NOT EXISTS idx_vm_metric_data_timestamp 
    ON vm_metric_data (metric_timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_vm_metric_data_job_metric 
    ON vm_metric_data (job_idx, metric_id);

-- Composite index for common query patterns
CREATE INDEX IF NOT EXISTS idx_vm_metric_data_job_metric_time 
    ON vm_metric_data (job_idx, metric_id, metric_timestamp DESC);

-- Index for run_id queries
CREATE INDEX IF NOT EXISTS idx_vm_metric_data_run_id 
    ON vm_metric_data (run_id);

