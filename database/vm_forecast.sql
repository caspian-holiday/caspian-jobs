-- PostgreSQL DDL Script for Metrics Forecast Storage
-- This table stores Prophet-generated metric forecasts from the metrics_forecast job
--
-- Note: Assumes tables will be placed in an existing schema
-- Update the schema name below as needed for your environment

-- Set search path to include your existing schema
-- Replace 'your_existing_schema' with your actual schema name
SET search_path TO your_existing_schema, public;

-- Table to store metric forecasts
-- This table is used by the metrics_forecast job to store Prophet-generated forecasts
-- Partitioned by job for better performance with multiple data sources
-- Note: created_at and updated_at timestamps are supplied by the client application
CREATE TABLE IF NOT EXISTS vm_forecasted_metric (
    job VARCHAR(255) NOT NULL,
    biz_date DATE NOT NULL,
    auid VARCHAR(255) NOT NULL,
    metric_name VARCHAR(255) NOT NULL,
    value NUMERIC NOT NULL,
    metric_labels JSONB,
    forecast_type VARCHAR(50) NOT NULL,
    forecast_run_id BIGINT,              -- References vm_forecast_job(run_id) for parameter tracking
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- Primary key for upsert operations
    -- Re-running forecasts on the same biz_date will overwrite existing values
    PRIMARY KEY (job, biz_date, auid, metric_name, forecast_type)
) PARTITION BY LIST (job);

-- Foreign key to forecast job run (deferred because partitioned table)
-- Note: You may need to add this constraint after creating partitions
-- ALTER TABLE vm_forecasted_metric 
--     ADD CONSTRAINT fk_forecast_run 
--     FOREIGN KEY (forecast_run_id) 
--     REFERENCES vm_forecast_job(run_id) 
--     ON DELETE SET NULL;

-- Create default partition for sources without specific partitions
-- This catches any source values that don't have a dedicated partition
CREATE TABLE IF NOT EXISTS vm_forecast_default PARTITION OF vm_forecasted_metric DEFAULT;

-- Create specific partition for autosys_jobs source
-- This provides optimized storage and query performance for autosys_jobs forecasts
CREATE TABLE IF NOT EXISTS vm_forecast_autosys_jobs 
  PARTITION OF vm_forecasted_metric FOR VALUES IN ('autosys_jobs');

-- Example: Create additional source-specific partitions as needed
-- CREATE TABLE IF NOT EXISTS vm_forecast_apex_collector 
--   PARTITION OF vm_forecasted_metric FOR VALUES IN ('apex_collector');

-- Indexes on forecast table for common query patterns
CREATE INDEX IF NOT EXISTS idx_vm_forecast_biz_date 
    ON vm_forecasted_metric (biz_date DESC);

CREATE INDEX IF NOT EXISTS idx_vm_forecast_metric_name 
    ON vm_forecasted_metric (metric_name);

CREATE INDEX IF NOT EXISTS idx_vm_forecast_auid 
    ON vm_forecasted_metric (auid);

CREATE INDEX IF NOT EXISTS idx_vm_forecast_forecast_type 
    ON vm_forecasted_metric (forecast_type);

-- GIN index for efficient JSONB queries on metric_labels
CREATE INDEX IF NOT EXISTS idx_vm_forecast_metric_labels 
    ON vm_forecasted_metric USING GIN (metric_labels);

-- Composite index for common filtering patterns
CREATE INDEX IF NOT EXISTS idx_vm_forecast_source_date_name 
    ON vm_forecasted_metric (job, biz_date DESC, metric_name);

-- Index on forecast_run_id for joining with vm_forecast_job
CREATE INDEX IF NOT EXISTS idx_vm_forecast_run_id 
    ON vm_forecasted_metric (forecast_run_id);

-- Example queries

-- Query forecasts with their run parameters
-- SELECT 
--     f.biz_date,
--     f.metric_name,
--     f.forecast_type,
--     f.value,
--     j.run_timestamp,
--     j.prophet_config,
--     j.config_source
-- FROM vm_forecasted_metric f
-- JOIN vm_forecast_job j ON f.forecast_run_id = j.run_id
-- WHERE f.job = 'extractor'
--   AND f.biz_date >= CURRENT_DATE
-- ORDER BY f.biz_date, f.metric_name;

-- Find which parameters were used for a specific forecast
-- SELECT 
--     f.metric_name,
--     f.biz_date,
--     f.value,
--     j.prophet_config->>'changepoint_prior_scale' as changepoint_scale,
--     j.prophet_config->>'yearly_seasonality' as yearly_seasonality,
--     j.config_source,
--     j.run_timestamp
-- FROM vm_forecasted_metric f
-- JOIN vm_forecast_job j ON f.forecast_run_id = j.run_id
-- WHERE f.metric_name = 'revenue_total'
--   AND f.auid = 'company_123'
-- ORDER BY f.biz_date DESC;

