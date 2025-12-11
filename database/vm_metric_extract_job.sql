-- ============================================================================
-- PostgreSQL DDL Script for Metric Extract Job Run Tracking
-- This table stores information about each extraction run and tracks the
-- latest timestamp extracted for each selector
--
-- Purpose: 
-- - Track when extractions were performed
-- - Store the latest timestamp extracted per selector for incremental extraction
-- - Enable tracking of which extraction run produced which data points
-- ============================================================================

-- Table to store extract job runs and their latest timestamps
CREATE TABLE IF NOT EXISTS vm_metric_extract_job (
    run_id BIGSERIAL PRIMARY KEY,
    
    -- Run identification
    run_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    job_id VARCHAR(255) NOT NULL,          -- Job identifier from scheduler
    selection_value TEXT NOT NULL,          -- PromQL selector, e.g., '{job="api"}' or '{job="api", env="prod"}'
    
    -- Extraction tracking
    last_timestamp TIMESTAMPTZ,             -- Latest timestamp extracted for this selector in this run
    
    -- Run statistics
    series_count INTEGER DEFAULT 0,         -- Number of series extracted
    metrics_saved_count INTEGER DEFAULT 0,   -- Number of metric data points saved
    
    -- Timing
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    duration_seconds NUMERIC(10,2),
    
    -- Status
    status VARCHAR(50),                     -- 'running', 'completed', 'failed', 'partial'
    error_message TEXT,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_metric_extract_job_run_timestamp 
    ON vm_metric_extract_job(run_timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_metric_extract_job_selection 
    ON vm_metric_extract_job(selection_value);

CREATE INDEX IF NOT EXISTS idx_metric_extract_job_last_timestamp 
    ON vm_metric_extract_job(last_timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_metric_extract_job_status 
    ON vm_metric_extract_job(status);

CREATE INDEX IF NOT EXISTS idx_metric_extract_job_job_id 
    ON vm_metric_extract_job(job_id);

-- Composite index for finding latest run per selector
CREATE INDEX IF NOT EXISTS idx_metric_extract_job_selector_run 
    ON vm_metric_extract_job(selection_value, run_id DESC);

-- Comments for documentation
COMMENT ON TABLE vm_metric_extract_job IS 
    'Tracks metric extraction job runs and stores the latest timestamp extracted for each selector. Enables incremental extraction by querying data since last_timestamp.';

COMMENT ON COLUMN vm_metric_extract_job.run_id IS 
    'Unique identifier for each extraction run';

COMMENT ON COLUMN vm_metric_extract_job.selection_value IS 
    'PromQL selector string (e.g., "{job=\"api\"}" or "{job=\"api\", env=\"prod\"}")';

COMMENT ON COLUMN vm_metric_extract_job.last_timestamp IS 
    'Latest timestamp extracted for this selector in this run. Used for incremental extraction in subsequent runs.';

-- Example queries

-- View recent runs with their latest timestamps
-- SELECT 
--     run_id,
--     run_timestamp,
--     selection_value,
--     last_timestamp,
--     series_count,
--     metrics_saved_count,
--     status,
--     duration_seconds
-- FROM vm_metric_extract_job
-- ORDER BY run_timestamp DESC
-- LIMIT 10;

-- Find latest timestamp for a specific selector (for incremental extraction)
-- SELECT 
--     last_timestamp
-- FROM vm_metric_extract_job
-- WHERE selection_value = '{job="api"}'
-- ORDER BY run_id DESC
-- LIMIT 1;

-- Find runs for a specific selector
-- SELECT 
--     run_id,
--     run_timestamp,
--     last_timestamp,
--     series_count,
--     metrics_saved_count,
--     status
-- FROM vm_metric_extract_job
-- WHERE selection_value = '{job="api"}'
-- ORDER BY run_timestamp DESC;

