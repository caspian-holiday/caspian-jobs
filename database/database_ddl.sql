-- PostgreSQL DDL Script for VictoriaMetrics Jobs Integration
-- This script creates the necessary tables for extractor and forecast jobs
-- Generic approach supporting any system that needs data extraction from VictoriaMetrics
--
-- Note: Assumes tables will be placed in an existing schema
-- Update the schema name below as needed for your environment

-- Set search path to include your existing schema
-- Replace 'your_existing_schema' with your actual schema name
SET search_path TO your_existing_schema, public;

-- Table to track extraction job executions
CREATE TABLE IF NOT EXISTS vm_extraction_jobs (
    job_id VARCHAR(255) NOT NULL,
    biz_date DATE NOT NULL,
    execution_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    started_at TIMESTAMP WITH TIME ZONE NOT NULL,
    completed_at TIMESTAMP WITH TIME ZONE,
    records_processed INTEGER DEFAULT 0,
    records_failed INTEGER DEFAULT 0,
    execution_time_seconds DECIMAL(10,3),
    max_data_timestamp TIMESTAMP WITH TIME ZONE,
    error_message TEXT,
    
    -- Primary key is composite of job_id, biz_date, and execution_timestamp
    PRIMARY KEY (job_id, biz_date, execution_timestamp)
);

-- Table to store extracted metrics from VictoriaMetrics
CREATE TABLE IF NOT EXISTS vm_extracted_metrics (
    id BIGSERIAL PRIMARY KEY,
    biz_date DATE NOT NULL,
    audit_id VARCHAR(255),
    metric_name VARCHAR(255) NOT NULL,
    value DECIMAL(20,8),
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    extracted_at TIMESTAMP WITH TIME ZONE NOT NULL,
    job_id VARCHAR(255) NOT NULL,
    job_execution_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    
    -- Constraints
    CONSTRAINT chk_extracted_metrics_value CHECK (value IS NOT NULL)
);

-- Table to store metric forecasts
-- This table is used by the metrics_forecast job to store Prophet-generated forecasts
-- Partitioned by source for better performance with multiple data sources
-- Note: created_at and updated_at timestamps are supplied by the client application
CREATE TABLE IF NOT EXISTS forecasts (
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
CREATE TABLE IF NOT EXISTS forecasts_default PARTITION OF forecasts DEFAULT;

-- Create specific partition for autosys_jobs source
-- This provides optimized storage and query performance for autosys_jobs forecasts
CREATE TABLE IF NOT EXISTS forecasts_autosys_jobs 
  PARTITION OF forecasts FOR VALUES IN ('autosys_jobs');

-- Example: Create additional source-specific partitions as needed
-- CREATE TABLE IF NOT EXISTS forecasts_apex_collector 
--   PARTITION OF forecasts FOR VALUES IN ('apex_collector');

-- Indexes on forecast table for common query patterns
CREATE INDEX IF NOT EXISTS idx_forecasts_biz_date 
    ON forecasts (biz_date DESC);

CREATE INDEX IF NOT EXISTS idx_forecasts_metric_name 
    ON forecasts (metric_name);

CREATE INDEX IF NOT EXISTS idx_forecasts_auid 
    ON forecasts (metric_auid);

CREATE INDEX IF NOT EXISTS idx_forecasts_forecast_type 
    ON forecasts (forecast_type);

-- GIN index for efficient JSONB queries on metric_labels
CREATE INDEX IF NOT EXISTS idx_forecasts_metric_labels 
    ON forecasts USING GIN (metric_labels);

-- Composite index for common filtering patterns
CREATE INDEX IF NOT EXISTS idx_forecasts_source_date_name 
    ON forecasts (source, biz_date DESC, metric_name);











-- Create sample data for testing (optional)
-- Uncomment the following section if you want to create sample data

/*

-- Insert sample extraction job
INSERT INTO vm_extraction_jobs (job_id, business_date, started_at, completed_at, status, records_processed, execution_time_seconds)
VALUES ('apex_extractor', '2024-01-15', '2024-01-15 06:15:00+00', '2024-01-15 06:15:45+00', 'completed', 100, 45.5);

*/





