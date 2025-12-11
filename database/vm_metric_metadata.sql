-- ============================================================================
-- Victoria Metrics Jobs - Metric Metadata Schema DDL
-- Table: vm_metric_metadata
-- Pure PostgreSQL definition (no TimescaleDB dependencies)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Table: vm_metric_metadata
-- Purpose: Stores metadata for each unique metric series
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS vm_metric_metadata (
    job_idx BIGSERIAL NOT NULL,
    metric_id INT NOT NULL,
    job_id VARCHAR NOT NULL, -- equals to the job label on the input data and job_id in the victoria_metrics_job system
    metric_name VARCHAR NOT NULL,
    metric_labels JSONB NOT NULL,
    PRIMARY KEY (job_idx, metric_id)
);

-- Indexes for efficient lookups
CREATE INDEX IF NOT EXISTS idx_vm_metric_metadata_job_id 
    ON vm_metric_metadata (job_id);

CREATE INDEX IF NOT EXISTS idx_vm_metric_metadata_metric_name 
    ON vm_metric_metadata (metric_name);

-- GIN index for efficient JSONB queries on metric_labels
CREATE INDEX IF NOT EXISTS idx_vm_metric_metadata_labels 
    ON vm_metric_metadata USING GIN (metric_labels);

-- Composite index for the lookup pattern used by metrics_forecast job
-- (job_id, metric_name, normalized metric_labels)
CREATE INDEX IF NOT EXISTS idx_vm_metric_metadata_lookup 
    ON vm_metric_metadata (job_id, metric_name);

