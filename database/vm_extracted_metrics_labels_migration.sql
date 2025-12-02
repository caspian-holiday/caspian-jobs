-- Migration Script: Add Label Storage to vm_extracted_metrics
-- This migration adds JSONB label storage and renames audit_id to metric_auid
-- for consistency with the vm_forecast table structure
--
-- Note: Assumes tables are in an existing schema
-- Update the schema name below as needed for your environment

-- Set search path to include your existing schema
-- Replace 'your_existing_schema' with your actual schema name
SET search_path TO your_existing_schema, public;

-- Step 1: Add metric_labels JSONB column
-- This will store additional metric labels as JSON for flexible querying
ALTER TABLE vm_extracted_metrics 
  ADD COLUMN IF NOT EXISTS metric_labels JSONB;

-- Step 2: Rename audit_id to metric_auid for consistency
-- NOTE: This will fail if there are views or foreign keys referencing this column
-- In that case, you may need to:
--   1. Drop dependent views
--   2. Run this ALTER
--   3. Recreate views
ALTER TABLE vm_extracted_metrics 
  RENAME COLUMN audit_id TO metric_auid;

-- Step 3: Create index on metric_labels for efficient JSONB queries
CREATE INDEX IF NOT EXISTS idx_vm_extracted_metrics_labels 
    ON vm_extracted_metrics USING GIN (metric_labels);

-- Step 4: Create index on metric_auid for common queries
CREATE INDEX IF NOT EXISTS idx_vm_extracted_metrics_auid 
    ON vm_extracted_metrics (metric_auid);

-- Step 5: Add comment to document the change
COMMENT ON COLUMN vm_extracted_metrics.metric_labels IS 
  'Additional metric labels stored as JSONB. Excludes: source (job), auid, biz_date, __name__';

COMMENT ON COLUMN vm_extracted_metrics.metric_auid IS 
  'Application unique identifier (renamed from audit_id for consistency with vm_forecast table)';

