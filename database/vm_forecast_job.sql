-- PostgreSQL DDL Script for Forecast Job Run Tracking
-- This table stores information about each forecast run and the Prophet parameters used
--
-- Purpose: 
-- - Track when forecasts were generated
-- - Store Prophet model/fit parameters once per run (not per data point)
-- - Enable reproducibility and parameter auditing

-- Set search path to include your existing schema
SET search_path TO your_existing_schema, public;

-- Table to store forecast job runs and their Prophet configurations
CREATE TABLE IF NOT EXISTS vm_forecast_job (
    run_id BIGSERIAL PRIMARY KEY,
    
    -- Run identification
    run_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    job_id VARCHAR(255),                  -- Job identifier from scheduler
    selection_value TEXT NOT NULL,        -- PromQL selector, e.g., '{job="extractor"}' or '{job="api", env="prod"}'
    
    -- Prophet configuration used for this run
    prophet_config JSONB NOT NULL,        -- Prophet model parameters
    prophet_fit_config JSONB,             -- Prophet fit parameters (optional)
    config_source VARCHAR(255),           -- Where config came from (e.g., "default", "job=extractor")
    
    -- Run metadata
    history_days INTEGER,                 -- History window used
    forecast_horizon_days INTEGER,        -- Forecast horizon
    min_history_points INTEGER,           -- Minimum points required
    business_date DATE,                   -- Business date for this run
    
    -- Run statistics
    series_count INTEGER DEFAULT 0,       -- Number of series forecasted
    success_count INTEGER DEFAULT 0,      -- Number of successful forecasts
    failed_count INTEGER DEFAULT 0,       -- Number of failed forecasts
    
    -- Timing
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    duration_seconds NUMERIC(10,2),
    
    -- Status
    status VARCHAR(50),                   -- 'running', 'completed', 'failed', 'partial'
    error_message TEXT,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_forecast_job_run_timestamp 
    ON vm_forecast_job(run_timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_forecast_job_selection 
    ON vm_forecast_job(selection_value);

CREATE INDEX IF NOT EXISTS idx_forecast_job_business_date 
    ON vm_forecast_job(business_date DESC);

CREATE INDEX IF NOT EXISTS idx_forecast_job_status 
    ON vm_forecast_job(status);

CREATE INDEX IF NOT EXISTS idx_forecast_job_config_source 
    ON vm_forecast_job(config_source);

-- Comments for documentation
COMMENT ON TABLE vm_forecast_job IS 
    'Tracks forecast job runs and stores Prophet parameters used for each run. Enables parameter auditing and forecast reproducibility.';

COMMENT ON COLUMN vm_forecast_job.run_id IS 
    'Unique identifier for each forecast run';

COMMENT ON COLUMN vm_forecast_job.selection_value IS 
    'PromQL selector string (e.g., "{job=\"extractor\"}" or "{job=\"api\", env=\"prod\"}")';

COMMENT ON COLUMN vm_forecast_job.prophet_config IS 
    'Prophet model parameters used for this run (e.g., {"changepoint_prior_scale": 0.5, "yearly_seasonality": true})';

COMMENT ON COLUMN vm_forecast_job.prophet_fit_config IS 
    'Prophet fit parameters used for this run (e.g., {"algorithm": "Newton", "iterations": 1000})';

COMMENT ON COLUMN vm_forecast_job.config_source IS 
    'Source of configuration: "default" or "job=extractor" or "selector={...}"';

-- Example queries

-- View recent runs with their configurations
-- SELECT 
--     run_id,
--     run_timestamp,
--     selection_value,
--     config_source,
--     prophet_config,
--     series_count,
--     status,
--     duration_seconds
-- FROM vm_forecast_job
-- ORDER BY run_timestamp DESC
-- LIMIT 10;

-- Find runs for a specific selector
-- SELECT 
--     run_id,
--     run_timestamp,
--     business_date,
--     prophet_config->>'changepoint_prior_scale' as changepoint_scale,
--     series_count,
--     success_count,
--     failed_count
-- FROM vm_forecast_job
-- WHERE selection_value = '{job="extractor"}'
-- ORDER BY run_timestamp DESC;

-- Compare parameters across runs
-- SELECT 
--     run_id,
--     run_timestamp,
--     config_source,
--     prophet_config->>'changepoint_prior_scale' as changepoint_scale,
--     prophet_config->>'seasonality_prior_scale' as seasonality_scale,
--     success_count,
--     failed_count
-- FROM vm_forecast_job
-- WHERE selection_value = '{job="extractor"}'
-- ORDER BY run_timestamp DESC
-- LIMIT 5;

