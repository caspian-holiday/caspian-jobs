# Database Setup for APEX Integration

This directory contains the database setup files for the APEX integration with VictoriaMetrics.

## Files

- `database_ddl.sql` - PostgreSQL DDL script to create all necessary tables, views, and functions
- `setup_database.py` - Python script to automate database setup
- `README.md` - This documentation file

## Quick Setup

### Option 1: Automated Setup (Recommended)

Use the Python setup script for automated database creation:

```bash
# Basic setup with defaults
python setup_database.py

# Setup with custom database
DB_NAME=my_apex_db DB_USER=my_user DB_PASSWORD=secret python setup_database.py

# Setup with remote database
DB_HOST=db.example.com DB_PORT=5432 DB_NAME=apex_prod DB_USER=apex_user DB_PASSWORD=secret python setup_database.py
```

### Option 2: Manual Setup

1. Create the database:
```sql
CREATE DATABASE victoria_metrics_jobs;
```

2. Connect to the database:
```bash
psql -h localhost -U postgres -d victoria_metrics_jobs
```

3. Execute the DDL script:
```sql
\i database_ddl.sql
```

## Database Schema

The VictoriaMetrics jobs integration uses the `vm_jobs` schema with the following tables:

### Core Tables

- **`extraction_jobs`** - Tracks extraction job executions from VictoriaMetrics to database
- **`extracted_metrics`** - Stores extracted metrics data from VictoriaMetrics

### Views

- **`v_extraction_job_summary`** - Summary of extraction job executions
- **`v_metrics_summary_by_date`** - Metrics summary grouped by business date


## Configuration

Update your configuration file with the database connection details:

```yaml
database:
  host: localhost
  port: 5432
  name: victoria_metrics_jobs
  user: vm_user
  password: vm_password
  ssl_mode: prefer
  connect_timeout: 10
  application_name: victoria_metrics_jobs
  schema: vm_jobs
```

## Permissions

The DDL script grants permissions to a `vm_user` role. Adjust as needed:

```sql
-- Create user if it doesn't exist
CREATE USER vm_user WITH PASSWORD 'vm_password';

-- Grant permissions
GRANT USAGE ON SCHEMA vm_jobs TO vm_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA vm_jobs TO vm_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA vm_jobs TO vm_user;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA vm_jobs TO vm_user;
```

## Monitoring Queries

### Check Job Status
```sql
-- Replace 'your_schema' with your actual schema name
SELECT * FROM your_schema.v_extraction_job_summary 
WHERE business_date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY started_at DESC;
```


### Check Metrics Summary
```sql
-- Replace 'your_schema' with your actual schema name
SELECT * FROM your_schema.v_metrics_summary_by_date 
WHERE business_date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY business_date DESC, metric_name;
```

### Check for Failed Jobs
```sql
-- Replace 'your_schema' with your actual schema name
SELECT * FROM your_schema.extraction_jobs 
WHERE status = 'failed' 
AND started_at >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY started_at DESC;
```

## Maintenance

### Cleanup Old Data
```sql
-- Replace 'your_schema' with your actual schema name
-- Clean up old extraction jobs (keep 90 days)
DELETE FROM your_schema.extraction_jobs 
WHERE started_at < NOW() - INTERVAL '90 days';

-- Clean up old extracted metrics (keep 1 year)
DELETE FROM your_schema.extracted_metrics 
WHERE extracted_at < NOW() - INTERVAL '1 year';
```


## Troubleshooting

### Common Issues

1. **Permission Denied**: Ensure the database user has proper permissions on the schema and tables
2. **Schema Not Found**: Make sure the `apex_integration` schema exists
3. **Connection Failed**: Check database host, port, and credentials
4. **Table Not Found**: Verify the DDL script was executed successfully

### Verification Queries

```sql
-- Check if tables exist (replace 'your_schema' with your actual schema name)
SELECT table_name FROM information_schema.tables WHERE table_schema = 'your_schema' AND table_name IN ('extraction_jobs', 'extracted_metrics');

-- Check if views exist (replace 'your_schema' with your actual schema name)
SELECT table_name FROM information_schema.views WHERE table_schema = 'your_schema' AND table_name IN ('v_extraction_job_summary', 'v_metrics_summary_by_date');
```

## Performance Considerations

### Indexes

The DDL script creates comprehensive indexes for optimal performance:

- Primary key indexes on all tables
- Foreign key indexes
- Business date indexes for time-based queries
- GIN indexes on JSONB columns
- Composite indexes for common query patterns

### Partitioning

For high-volume environments, consider partitioning the `extracted_metrics` table by `business_date`:

```sql
-- Example partitioning by month
CREATE TABLE apex_integration.extracted_metrics_2024_01 
PARTITION OF apex_integration.extracted_metrics
FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
```

### Vacuum and Analyze

Regular maintenance:

```sql
-- Analyze tables for query optimization
ANALYZE apex_integration.extracted_metrics;
ANALYZE apex_integration.extraction_jobs;

-- Vacuum to reclaim space
VACUUM ANALYZE apex_integration.extracted_metrics;
```

## Security

### Best Practices

1. **Use dedicated database user** with minimal required permissions
2. **Enable SSL** for remote connections
3. **Regular password rotation** for database users
4. **Monitor access logs** for suspicious activity
5. **Backup regularly** including schema and data

### Connection Security

```yaml
database:
  ssl_mode: require  # Force SSL for production
  ssl_cert: /path/to/client-cert.pem
  ssl_key: /path/to/client-key.pem
  ssl_rootcert: /path/to/ca-cert.pem
```

## Backup and Recovery

### Backup Schema and Data
```bash
# Backup schema only
# Replace 'your_schema' with your actual schema name
pg_dump -h localhost -U postgres -d victoria_metrics_jobs --schema=your_schema --schema-only > vm_jobs_schema.sql

# Backup data only
pg_dump -h localhost -U postgres -d victoria_metrics_jobs --schema=your_schema --data-only > vm_jobs_data.sql

# Full backup
pg_dump -h localhost -U postgres -d victoria_metrics_jobs --schema=your_schema > vm_jobs_full.sql
```

### Restore
```bash
# Restore from backup
psql -h localhost -U postgres -d victoria_metrics_jobs < vm_jobs_full.sql
```
