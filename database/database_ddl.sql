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
-- Note: metric_auid renamed from audit_id for consistency with vm_forecast table
-- Note: metric_labels stores additional labels as JSONB (excludes: source/job, auid, biz_date, __name__)
CREATE TABLE IF NOT EXISTS vm_extracted_metrics (
    id BIGSERIAL PRIMARY KEY,
    biz_date DATE NOT NULL,
    metric_auid VARCHAR(255),
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

-- Indexes for vm_extracted_metrics
CREATE INDEX IF NOT EXISTS idx_vm_extracted_metrics_labels 
    ON vm_extracted_metrics USING GIN (metric_labels);

CREATE INDEX IF NOT EXISTS idx_vm_extracted_metrics_auid 
    ON vm_extracted_metrics (metric_auid);

-- Note: Forecast table DDL has been moved to vm_forecast_ddl.sql
-- See vm_forecast_ddl.sql for the vm_forecast table and partitions











-- Create sample data for testing (optional)
-- Uncomment the following section if you want to create sample data

/*

-- Insert sample extraction job
INSERT INTO vm_extraction_jobs (job_id, business_date, started_at, completed_at, status, records_processed, execution_time_seconds)
VALUES ('apex_extractor', '2024-01-15', '2024-01-15 06:15:00+00', '2024-01-15 06:15:45+00', 'completed', 100, 45.5);

*/





