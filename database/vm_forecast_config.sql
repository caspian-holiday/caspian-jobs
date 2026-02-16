-- PostgreSQL DDL Script for Per-Selection Prophet Configuration
-- This table stores custom Prophet parameters for different job/selector groups
--
-- Usage:
-- - Configure different Prophet parameters for different metrics sources
-- - Dynamically tune forecasting without redeploying code
-- - A/B test different parameter combinations

-- Set search path to include your existing schema
-- Replace 'your_existing_schema' with your actual schema name
SET search_path TO your_existing_schema, public;

-- Table to store Prophet configuration per selector
CREATE TABLE IF NOT EXISTS vm_forecast_config (
    config_id SERIAL PRIMARY KEY,
    
    -- Job identification (from scheduler)
    job_id VARCHAR(255) NOT NULL,        -- Scheduler job ID (e.g., 'metrics_forecast', 'metrics_forecast_prod')
    
    -- Selection identification (PromQL selector)
    selection_value TEXT NOT NULL,        -- PromQL selector, e.g., '{job="extractor"}' or '{job="api", env="prod"}'
    
    -- Prophet parameters as JSON
    prophet_params JSONB NOT NULL,        -- Prophet model parameters
    prophet_fit_params JSONB,             -- Prophet fit parameters (optional)
    
    -- History and forecast configuration
    history_days INTEGER DEFAULT 365,     -- Days of history to fetch
    history_offset_days INTEGER DEFAULT 0, -- Offset from current date (days to skip at end)
    history_step_hours INTEGER DEFAULT 24, -- Sampling interval in hours
    forecast_horizon_days INTEGER DEFAULT 20, -- Business days to forecast ahead
    min_history_points INTEGER DEFAULT 30, -- Minimum data points required
    cutoff_hour INTEGER DEFAULT 6,        -- Hour (UTC) for business date cutoff
    
    -- Management fields
    enabled BOOLEAN DEFAULT true NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    notes TEXT,
    
    -- Ensure unique configuration per job and selector combination
    CONSTRAINT uq_forecast_config_job_selector UNIQUE (job_id, selection_value)
);

-- Indexes for efficient lookups
CREATE INDEX IF NOT EXISTS idx_forecast_config_job_selection 
    ON vm_forecast_config(job_id, selection_value) 
    WHERE enabled = true;

CREATE INDEX IF NOT EXISTS idx_forecast_config_job_enabled 
    ON vm_forecast_config(job_id, enabled);

-- Comments for documentation
COMMENT ON TABLE vm_forecast_config IS 
    'Stores Prophet forecasting parameters and history configuration per PromQL selector and scheduler job. Allows different jobs to have different configurations for the same selectors.';

COMMENT ON COLUMN vm_forecast_config.job_id IS 
    'Scheduler job ID (e.g., "metrics_forecast", "metrics_forecast_prod"). Each job loads only its own configurations.';

COMMENT ON COLUMN vm_forecast_config.selection_value IS 
    'PromQL selector string (e.g., "{job=\"extractor\"}" or "{job=\"api\", env=\"prod\"}")';

COMMENT ON COLUMN vm_forecast_config.prophet_params IS 
    'Prophet model parameters as JSON (e.g., {"changepoint_prior_scale": 0.5, "yearly_seasonality": true})';

COMMENT ON COLUMN vm_forecast_config.prophet_fit_params IS 
    'Prophet fit parameters as JSON (e.g., {"algorithm": "Newton", "iterations": 1000})';

COMMENT ON COLUMN vm_forecast_config.history_days IS 
    'Number of days of historical data to fetch for training';

COMMENT ON COLUMN vm_forecast_config.history_offset_days IS 
    'Days to skip at the end of history window (e.g., exclude recent incomplete data)';

COMMENT ON COLUMN vm_forecast_config.history_step_hours IS 
    'Sampling interval in hours (e.g., 24 for daily, 1 for hourly)';

COMMENT ON COLUMN vm_forecast_config.forecast_horizon_days IS 
    'Number of business days to forecast ahead';

COMMENT ON COLUMN vm_forecast_config.min_history_points IS 
    'Minimum number of data points required to generate forecast';

COMMENT ON COLUMN vm_forecast_config.cutoff_hour IS 
    'Hour (UTC) for business date cutoff (before this hour, use previous business day)';

-- Example configurations

-- Example 1: Volatile metrics with flexible trend and extended history
-- INSERT INTO vm_forecast_config (
--     job_id,
--     selection_value, 
--     prophet_params, 
--     history_days,
--     forecast_horizon_days,
--     min_history_points,
--     notes
-- ) VALUES (
--     'metrics_forecast',
--     '{job="extractor"}',
--     '{"changepoint_prior_scale": 0.5, "seasonality_prior_scale": 1.0, "n_changepoints": 50}',
--     730,  -- 2 years of history for volatile metrics
--     30,   -- Forecast 30 days ahead
--     60,   -- Require 60 points minimum
--     'Extractor has volatile metrics that need flexible trend detection'
-- );

-- Example 2: Stable metrics with strong seasonality
-- INSERT INTO vm_forecast_config (
--     job_id,
--     selection_value,
--     prophet_params,
--     prophet_fit_params,
--     history_days,
--     notes
-- ) VALUES (
--     'metrics_forecast',
--     '{job="loader"}',
--     '{"changepoint_prior_scale": 0.01, "seasonality_prior_scale": 50.0, "yearly_seasonality": true}',
--     '{"algorithm": "LBFGS", "iterations": 2000}',
--     365,  -- 1 year of history
--     'Loader metrics are stable with strong yearly patterns'
-- );

-- Example 3: Production API metrics with custom history window (for production job)
-- INSERT INTO vm_forecast_config (
--     job_id,
--     selection_value,
--     prophet_params,
--     history_days,
--     history_offset_days,
--     forecast_horizon_days,
--     notes
-- ) VALUES (
--     'metrics_forecast_prod',
--     '{job="api", env="prod"}',
--     '{"yearly_seasonality": true, "seasonality_mode": "multiplicative"}',
--     545,  -- 18 months of history
--     7,    -- Exclude last 7 days (incomplete data)
--     20,   -- Forecast 20 business days
--     'Production API metrics scale seasonally with growth'
-- );

-- Query to view all active configurations for a specific job
-- SELECT 
--     job_id,
--     selection_value,
--     prophet_params,
--     prophet_fit_params,
--     history_days,
--     forecast_horizon_days,
--     notes,
--     updated_at
-- FROM vm_forecast_config
-- WHERE job_id = 'metrics_forecast'
--   AND enabled = true
-- ORDER BY selection_value;

