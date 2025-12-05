-- PostgreSQL DDL Script for Extraction Job Tracking
-- This table tracks execution history for extractor jobs
--
-- Purpose:
-- - Track when extraction jobs ran
-- - Record execution statistics (records processed, duration, errors)
-- - Enable idempotent job execution tracking

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

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_vm_extraction_jobs_job_date 
    ON vm_extraction_jobs(job_id, biz_date DESC);

CREATE INDEX IF NOT EXISTS idx_vm_extraction_jobs_execution_timestamp 
    ON vm_extraction_jobs(execution_timestamp DESC);

-- Comments for documentation
COMMENT ON TABLE vm_extraction_jobs IS 
    'Tracks execution history for extractor jobs, including timing, statistics, and errors';

COMMENT ON COLUMN vm_extraction_jobs.job_id IS 
    'Extractor job identifier (e.g., "apex_extractor", "system_a_extractor")';

COMMENT ON COLUMN vm_extraction_jobs.biz_date IS 
    'Business date for which data was extracted';

COMMENT ON COLUMN vm_extraction_jobs.execution_timestamp IS 
    'Timestamp when the job execution started (allows multiple runs per biz_date)';

COMMENT ON COLUMN vm_extraction_jobs.records_processed IS 
    'Number of metric records successfully extracted';

COMMENT ON COLUMN vm_extraction_jobs.records_failed IS 
    'Number of metric records that failed to extract';

COMMENT ON COLUMN vm_extraction_jobs.max_data_timestamp IS 
    'Latest timestamp found in the extracted data';

-- Example queries

-- View recent extraction jobs
-- SELECT 
--     job_id,
--     biz_date,
--     execution_timestamp,
--     records_processed,
--     execution_time_seconds,
--     error_message
-- FROM vm_extraction_jobs
-- ORDER BY execution_timestamp DESC
-- LIMIT 10;

-- Find failed extractions
-- SELECT 
--     job_id,
--     biz_date,
--     execution_timestamp,
--     error_message
-- FROM vm_extraction_jobs
-- WHERE error_message IS NOT NULL
-- ORDER BY execution_timestamp DESC;

