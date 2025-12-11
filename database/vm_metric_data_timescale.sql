-- ============================================================================
-- Victoria Metrics Jobs - TimescaleDB Conversion Script
-- Converts vm_metric_data table to TimescaleDB hypertable
-- 
-- Prerequisites:
--   1. TimescaleDB extension must be installed and enabled
--   2. vm_metric_data table must exist (created by vm_metric_data.sql)
-- 
-- Usage:
--   Run this script after creating the base tables and installing TimescaleDB
-- ============================================================================

-- Verify TimescaleDB extension is available
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
        RAISE EXCEPTION 'TimescaleDB extension is not installed. Please install it first.';
    END IF;
END $$;

-- 1. Convert existing table to hypertable
SELECT create_hypertable(
    'vm_metric_data',
    'metric_timestamp',
    chunk_time_interval => INTERVAL '1 month',
    partitioning_column => 'job_idx',
    number_partitions => 8,
    migrate_data => true  -- Migrates existing data into chunks
);

-- 2. Enable compression with segmentation
ALTER TABLE vm_metric_data SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'job_idx, metric_id',
    timescaledb.compress_orderby = 'metric_timestamp DESC'
);

-- 3. Add compression policy (compress data older than 30 days)
SELECT add_compression_policy('vm_metric_data', INTERVAL '30 days');

-- 4. Add retention policy (optional - drop data older than 7 years)
SELECT add_retention_policy('vm_metric_data', INTERVAL '7 years');

