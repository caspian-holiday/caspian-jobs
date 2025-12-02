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
-- Partitioned by source for better performance with multiple data sources
-- Note: created_at and updated_at timestamps are supplied by the client application
CREATE TABLE IF NOT EXISTS vm_forecast (
    source VARCHAR(255) NOT NULL,
    biz_date DATE NOT NULL,
    metric_auid VARCHAR(255) NOT NULL,
    metric_name VARCHAR(255) NOT NULL,
    metric_value NUMERIC NOT NULL,
    metric_labels JSONB,
    forecast_type VARCHAR(50) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- Primary key for upsert operations
    -- Re-running forecasts on the same biz_date will overwrite existing values
    PRIMARY KEY (source, biz_date, metric_auid, metric_name, forecast_type)
) PARTITION BY LIST (source);

-- Create default partition for sources without specific partitions
-- This catches any source values that don't have a dedicated partition
CREATE TABLE IF NOT EXISTS vm_forecast_default PARTITION OF vm_forecast DEFAULT;

-- Create specific partition for autosys_jobs source
-- This provides optimized storage and query performance for autosys_jobs forecasts
CREATE TABLE IF NOT EXISTS vm_forecast_autosys_jobs 
  PARTITION OF vm_forecast FOR VALUES IN ('autosys_jobs');

-- Example: Create additional source-specific partitions as needed
-- CREATE TABLE IF NOT EXISTS vm_forecast_apex_collector 
--   PARTITION OF vm_forecast FOR VALUES IN ('apex_collector');

-- Indexes on forecast table for common query patterns
CREATE INDEX IF NOT EXISTS idx_vm_forecast_biz_date 
    ON vm_forecast (biz_date DESC);

CREATE INDEX IF NOT EXISTS idx_vm_forecast_metric_name 
    ON vm_forecast (metric_name);

CREATE INDEX IF NOT EXISTS idx_vm_forecast_auid 
    ON vm_forecast (metric_auid);

CREATE INDEX IF NOT EXISTS idx_vm_forecast_forecast_type 
    ON vm_forecast (forecast_type);

-- GIN index for efficient JSONB queries on metric_labels
CREATE INDEX IF NOT EXISTS idx_vm_forecast_metric_labels 
    ON vm_forecast USING GIN (metric_labels);

-- Composite index for common filtering patterns
CREATE INDEX IF NOT EXISTS idx_vm_forecast_source_date_name 
    ON vm_forecast (source, biz_date DESC, metric_name);

