-- PostgreSQL DDL Script for Extracted Metrics Storage
-- This table stores metrics extracted from VictoriaMetrics by extractor jobs
--
-- Purpose:
-- - Store raw metric data extracted from VictoriaMetrics
-- - Support flexible label storage via JSONB
-- - Enable downstream processing and analysis

-- Set search path to include your existing schema
-- Replace 'your_existing_schema' with your actual schema name
SET search_path TO your_existing_schema, public;

-- Table to store extracted metrics from VictoriaMetrics
-- Note: auid renamed from audit_id for consistency with vm_forecasted_metric table
-- Note: metric_labels stores additional labels as JSONB (excludes: source/job, auid, biz_date, __name__)
CREATE TABLE IF NOT EXISTS vm_extracted_metrics (
    id BIGSERIAL PRIMARY KEY,
    biz_date DATE NOT NULL,
    auid VARCHAR(255),
    metric_name VARCHAR(255) NOT NULL,
    value DECIMAL(20,8),
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    metric_labels JSONB,
    extracted_at TIMESTAMP WITH TIME ZONE NOT NULL,
    job_id VARCHAR(255) NOT NULL,
    job_execution_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    
    -- Constraints
    CONSTRAINT chk_extracted_metrics_value CHECK (value IS NOT NULL)
);

-- Indexes for efficient queries
CREATE INDEX IF NOT EXISTS idx_vm_extracted_metrics_biz_date 
    ON vm_extracted_metrics(biz_date DESC);

CREATE INDEX IF NOT EXISTS idx_vm_extracted_metrics_metric_name 
    ON vm_extracted_metrics(metric_name);

CREATE INDEX IF NOT EXISTS idx_vm_extracted_metrics_job_id 
    ON vm_extracted_metrics(job_id);

CREATE INDEX IF NOT EXISTS idx_vm_extracted_metrics_timestamp 
    ON vm_extracted_metrics(timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_vm_extracted_metrics_labels 
    ON vm_extracted_metrics USING GIN (metric_labels);

CREATE INDEX IF NOT EXISTS idx_vm_extracted_metrics_auid 
    ON vm_extracted_metrics(auid);

CREATE INDEX IF NOT EXISTS idx_vm_extracted_metrics_job_execution 
    ON vm_extracted_metrics(job_id, job_execution_timestamp);

-- Composite index for common filtering patterns
CREATE INDEX IF NOT EXISTS idx_vm_extracted_metrics_job_date_name 
    ON vm_extracted_metrics(job_id, biz_date DESC, metric_name);

-- Comments for documentation
COMMENT ON TABLE vm_extracted_metrics IS 
    'Stores metrics extracted from VictoriaMetrics by extractor jobs';

COMMENT ON COLUMN vm_extracted_metrics.biz_date IS 
    'Business date for which the metric was extracted';

COMMENT ON COLUMN vm_extracted_metrics.auid IS 
    'Audit ID or unique identifier for the metric source (optional)';

COMMENT ON COLUMN vm_extracted_metrics.metric_name IS 
    'Name of the metric (from Prometheus __name__ label)';

COMMENT ON COLUMN vm_extracted_metrics.value IS 
    'Numeric value of the metric';

COMMENT ON COLUMN vm_extracted_metrics.timestamp IS 
    'Original timestamp of the metric data point';

COMMENT ON COLUMN vm_extracted_metrics.metric_labels IS 
    'Additional metric labels as JSONB (excludes job, auid, biz_date, __name__)';

COMMENT ON COLUMN vm_extracted_metrics.job_id IS 
    'Extractor job identifier that extracted this metric';

COMMENT ON COLUMN vm_extracted_metrics.job_execution_timestamp IS 
    'Execution timestamp of the extractor job (links to vm_extraction_jobs)';

-- Example queries

-- Query metrics for a specific job and date
-- SELECT 
--     metric_name,
--     value,
--     timestamp,
--     metric_labels
-- FROM vm_extracted_metrics
-- WHERE job_id = 'apex_extractor'
--   AND biz_date = '2024-01-15'
-- ORDER BY timestamp DESC;

-- Find metrics by label value
-- SELECT 
--     metric_name,
--     value,
--     timestamp
-- FROM vm_extracted_metrics
-- WHERE metric_labels @> '{"env": "prod"}'::jsonb
--   AND biz_date >= CURRENT_DATE - INTERVAL '7 days'
-- ORDER BY timestamp DESC;

