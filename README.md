# Victoria Metrics Jobs Service

A robust, systemd-managed job scheduler service built with Python and APScheduler that executes Victoria Metrics-related Python script jobs with environment-specific configuration management.

## Features

- **Python Script Jobs**: Execute custom Python scripts with command-line arguments
- **Flexible Scheduling**: Cron, interval, and date-based scheduling using APScheduler
- **YAML Configuration**: Easy-to-edit YAML configuration files with environment variable support
- **Systemd Integration**: Runs as a proper systemd service with automatic restart
- **Comprehensive Logging**: Structured logging with rotation and multiple output options
- **Security**: Runs with restricted privileges and resource limits
- **Signal Handling**: Graceful shutdown on SIGTERM/SIGINT
- **Environment-Specific Configuration**: Unified configuration files with environment variable support
- **Custom Job Scripts**: Built-in extractor (multiple job configurations) and prime-collector jobs
- **Advisory Locking**: PostgreSQL-based advisory locks prevent simultaneous execution of the same job across multiple scheduler instances

## Installation

### Prerequisites

- Python 3.9 or higher
- Poetry (for dependency management)
- systemd (for service management)

### Environment-Specific Configuration

The service uses unified configuration files with environment-specific sections. Set the `VM_JOBS_ENVIRONMENT` variable to `local`, `dev`, `stg`, or `prod` to select the appropriate configuration.

```bash
# Clone and setup
git clone <repository-url>
cd victoria_metrics_jobs

# Set environment (local, dev, stg, or prod) - REQUIRED
export VM_JOBS_ENVIRONMENT=dev

# Install dependencies
poetry install

# Run the scheduler
python -m victoria_metrics_jobs.scheduler.service --config victoria_metrics_jobs/victoria_metrics_jobs.yml
```

### Development Setup

For local development:

```bash
# Clone and setup
git clone <repository-url>
cd victoria_metrics_jobs
poetry install

# Set environment for development - REQUIRED
export VM_JOBS_ENVIRONMENT=dev

# Run the scheduler
python -m victoria_metrics_jobs.scheduler.service --config victoria_metrics_jobs/victoria_metrics_jobs.yml
```

## Custom Jobs

The scheduler includes two custom Python job scripts built with a unified architecture:

### Job Architecture

All job scripts inherit from a common `BaseJob` class that provides:
- **Step-by-Step Workflows**: Jobs are composed of a series of steps that transform a state object
- **Functional Error Handling**: Uses Result types for clean error propagation without exceptions
- **Unified Configuration Management**: Automatic config loading and validation
- **Standardized Logging**: Consistent logging setup across all jobs
- **Common CLI Interface**: Standardized argument parsing (`--config`, `--job-id`, `--list-jobs`, `--verbose`)
- **State Management**: Clear data flow and accumulation through state objects
- **Execution Timing**: Automatic execution time tracking and status reporting

## APEX Integration with VictoriaMetrics

The system includes a comprehensive APEX integration that collects data from APEX systems and pushes it to VictoriaMetrics, with optional database extraction.

### Architecture

```
APEX System → ApexCollectorJob → VictoriaMetrics → ExtractorJob → Database
```

### Key Features

- **Weekdays Only**: Processes only Monday-Friday business days
- **Watermark-Based**: Uses VictoriaMetrics watermark to track last processed date
- **UTC Timezone**: All operations use UTC timezone
- **Sliding Window**: Optional reprocessing of previous N days
- **Observability**: Comprehensive logging and metrics
- **Error Handling**: Robust error handling with partial success support
- **Transaction Management**: Savepoint-based transactions ensure job status is always recorded

### Extractor Job (`jobs/extractor/`)
Data extraction and processing script that can handle multiple job configurations.

**Workflow Steps:**
1. Derive current business date
2. Derive list of weekdays from start_date_offset_days and current business_date
3. Associate each weekday with timestamp saved in database for that date
4. Associate each weekday with max timestamp saved in Victoria Metrics for that date
5. Derive weekdays that need to be updated by comparing timestamps
6. For each weekday that needs updating, perform time-range extraction using chunk_size_days

**Features:**
- Multiple job configurations per script
- Job-specific parameters via job ID
- **Time-range extraction** - uses `chunk_size_days` and `start_date_offset_days` for Prometheus-style queries
- **VictoriaMetrics integration** - uses `/api/v1/query_range` for efficient time-range data extraction
- Data processing and transformation
- **Database storage** - saves extracted data to PostgreSQL database with batch inserts
- Configurable via YAML with environment variables
- Unified JSON result format

### Apex Collector Job (`jobs/apex_collector/`)
APEX data collection script that fetches data from APEX REST API and pushes to VictoriaMetrics.

**Workflow Steps:**
1. Derive current business date
2. Read watermark from VictoriaMetrics
3. Generate list of weekdays to process
4. Process weekdays to update
5. Update watermark after successful processing

**Features:**
- Job-specific configuration via job ID
- APEX REST API integration
- VictoriaMetrics gateway publishing
- Watermark-based incremental processing
- Configurable via YAML with environment variables
- Unified JSON result format

### Job Result Format

All jobs return a consistent JSON structure:

**Success Response:**
```json
{
  "job_id": "system_a_extractor",
  "job_display_name": "Human-readable job name",
  "job_description": "Job description from config",
  "started_at": "2025-09-25T15:59:58.032217",
  "completed_at": "2025-09-25T15:59:58.032315",
  "status": "success",
  "message": "Job completed successfully message",
  "job_config": {...},
  "timestamp": "2025-09-25T15:59:58.032137",
  "execution_time_seconds": 0.000305
}
```

**Error Response:**
```json
{
  "job_id": "system_a_extractor",
  "job_display_name": "system_a_extractor",
  "job_description": "",
  "started_at": "2025-09-25T15:59:58.032217",
  "completed_at": "2025-09-25T15:59:58.032315",
  "status": "error",
  "message": "Job failed message",
  "error": "Detailed error message",
  "timestamp": "2025-09-25T15:59:58.032137",
  "execution_time_seconds": 0.000305
}
```

### Watermark Strategy

The watermark is stored as a gauge metric in VictoriaMetrics:

- **Metric Name**: `apex_collector_watermark_days`
- **Labels**: `source="apex"`, `job="apex_collector"`, `env`
- **Value**: Unix timestamp of last successfully processed business date (00:00 UTC)

#### Watermark Operations

- **Read**: Query VictoriaMetrics for current watermark value
- **Write**: Update watermark only after successful processing
- **Backfill**: Support for manual backfill by setting watermark backward

### Data Format

#### APEX Data Input

The system expects APEX data in JSON format. If the data contains a `business_date` field, it will be used for filtering. Otherwise, all data will be processed.

#### VictoriaMetrics Output

Data is transformed to Prometheus format:

```
apex_data_{field_name}{business_date="2024-01-15",source="apex",job="apex_collector",environment="production"} 123.45 1705276800
```

#### Database Schema

The extractor job saves data to these tables:

```sql
-- Extraction job tracking with composite primary key
CREATE TABLE vm_extraction_jobs (
    job_id VARCHAR(255) NOT NULL,
    business_date DATE NOT NULL,
    execution_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    started_at TIMESTAMP WITH TIME ZONE NOT NULL,
    completed_at TIMESTAMP WITH TIME ZONE,
    records_processed INTEGER DEFAULT 0,
    execution_time_seconds DECIMAL(10,3),
    max_data_timestamp TIMESTAMP WITH TIME ZONE,
    error_message TEXT,
    
    PRIMARY KEY (job_id, business_date, execution_timestamp)
);

-- Extracted metrics table
CREATE TABLE vm_extracted_metrics (
    id BIGSERIAL PRIMARY KEY,
    biz_date DATE NOT NULL,
    audit_id VARCHAR(255),
    metric_name VARCHAR(255) NOT NULL,
    value DECIMAL(20,8),
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    extracted_at TIMESTAMP WITH TIME ZONE NOT NULL,
    job_id VARCHAR(255) NOT NULL,
    job_execution_timestamp TIMESTAMP WITH TIME ZONE NOT NULL
);
```

### Transaction Strategy

The extractor job uses a simplified transaction approach with optimized batch inserts:

1. **Start Transaction**: Begin database transaction
2. **Prepare Metrics Data**: Collect all metrics in memory for batch processing
3. **Batch Insert Metrics**: Use PostgreSQL `executemany()` for efficient bulk insert
4. **Calculate Statistics**: Count records and find max timestamp from inserted data
5. **Insert Job Record**: Create job record with calculated statistics
6. **Commit Transaction**: Commit the entire transaction
7. **On Failure**: Rollback entire transaction (no partial data saved)

### Performance Optimization

- **Batch Inserts**: Uses PostgreSQL `executemany()` for efficient bulk data insertion
- **Memory Preparation**: All metric data is prepared in memory before database operations
- **Single Transaction**: All operations occur within one transaction for consistency
- **Time-Range Queries**: Uses VictoriaMetrics `/api/v1/query_range` API for efficient time-range extraction
- **Configurable Chunking**: `chunk_size_days` parameter allows tuning of extraction time windows
- **Offset Support**: `start_date_offset_days` enables flexible time range positioning from current business date
- **Retrospective Metrics**: End time set to tomorrow to capture recently inserted metrics with past timestamps

This ensures that job execution status is always tracked, even if data loading fails.

### Monitoring and Alerting

#### Key Metrics

- `apex_collector_job_status`: Job execution status (1=success, 0=failure)
- `extractor_job_status`: Job execution status (1=success, 0=failure)
- `apex_collector_watermark_days`: Last processed business date
- `apex_data_*`: Business metrics from APEX

### Usage

#### Running Jobs

```bash
# List available job configurations
VM_JOBS_ENVIRONMENT=local poetry run python -m victoria_metrics_jobs.jobs.apex_collector --config victoria_metrics_jobs/victoria_metrics_jobs.yml --list-jobs

# Run APEX collector
VM_JOBS_ENVIRONMENT=local poetry run python -m victoria_metrics_jobs.jobs.apex_collector --config victoria_metrics_jobs/victoria_metrics_jobs.yml --job-id apex_collector --verbose

# Run APEX extractor
VM_JOBS_ENVIRONMENT=local poetry run python -m victoria_metrics_jobs.jobs.extractor --config victoria_metrics_jobs/victoria_metrics_jobs.yml --job-id apex_extractor --verbose
```

#### Manual Backfill

To reprocess historical data:

1. Update the watermark metric in VictoriaMetrics to the desired start date
2. Run the collector job
3. The job will process all weekdays from the watermark to current business date

#### Sliding Window

Enable sliding window reprocessing by setting `sliding_window_days` in the configuration. This will reprocess the previous N weekdays on each run to capture late-arriving data.

### Error Handling

#### Partial Success

The system supports partial success scenarios:

- If some weekdays fail to process, the job continues with others
- Watermark is only advanced for successfully processed dates
- Job status reflects partial success with warning

#### Retry Logic

- Failed API calls are logged but don't stop the job
- Watermark is not advanced on failures
- Next run will retry failed dates

### Troubleshooting

#### Common Issues

1. **No data processed**: Check APEX API connectivity and authentication
2. **Watermark not advancing**: Verify VictoriaMetrics gateway connectivity
3. **Partial failures**: Check APEX API response format and data validation
4. **Database errors**: Verify database connectivity and schema

#### Logs

Jobs provide comprehensive logging:

- Business date calculation
- Watermark operations
- API calls and responses
- Processing results
- Error details

#### Metrics

Monitor these key metrics:

- Job success/failure rates
- Watermark lag
- Data ingestion rates
- Processing times

See [jobs/README.md](jobs/README.md) for detailed job documentation.

## Configuration

The scheduler uses YAML configuration files (`.yml` extension) powered by **OmegaConf** with support for:
- Environment variable resolution with defaults
- Configuration path interpolation (reference values from other parts of config)
- Environment-specific configurations
- Type-safe configuration management

### Environment-Specific Configuration

The scheduler uses a single consolidated configuration file with environment-specific sections:

```
scheduler/
└── scheduler.yml              # Single consolidated config with all environments and job configurations
```

The configuration file contains:
- Environment-specific sections (dev, stg, prod)
- Job definitions with embedded configurations
- All job-specific settings within each job definition

### Environment Setup

**Development Environment:**
```bash
# Bootstrap development environment
make dev-up

# Start scheduler in development mode
make dev-run

# Clean up development environment
make dev-down
```

**Environment-Specific Configuration:**
```bash
# Local development environment
VM_JOBS_ENVIRONMENT=local python -m victoria_metrics_jobs.victoria_metrics_jobs --config victoria_metrics_jobs/victoria_metrics_jobs.yml

# Development environment
VM_JOBS_ENVIRONMENT=dev python -m victoria_metrics_jobs.victoria_metrics_jobs --config victoria_metrics_jobs/victoria_metrics_jobs.yml

# Staging environment
VM_JOBS_ENVIRONMENT=stg python -m victoria_metrics_jobs.victoria_metrics_jobs --config victoria_metrics_jobs/victoria_metrics_jobs.yml

# Production environment
VM_JOBS_ENVIRONMENT=prod python -m victoria_metrics_jobs.victoria_metrics_jobs --config victoria_metrics_jobs/victoria_metrics_jobs.yml
```

**Manual Configuration:**
```bash
# Using specific config file (required)
python main.py --config /path/to/config.yml

# Example with unified scheduler config
VM_JOBS_ENVIRONMENT=dev python -m victoria_metrics_jobs.victoria_metrics_jobs --config victoria_metrics_jobs/victoria_metrics_jobs.yml
```

See `victoria_metrics_jobs/victoria_metrics_jobs.yml` for a comprehensive example.

### Configuration Interpolation

Configuration files use **OmegaConf** for powerful interpolation capabilities, supporting both environment variables and config path references.

#### Environment Variables

- **With defaults**: `${oc.env:VAR_NAME,default_value}`
- **Required variables**: `${env_required:VAR_NAME,Error message}`

#### Config Path Interpolation

Reference values from other parts of the configuration:

- **Simple reference**: `${common.extractor.target_url}`
- **Nested reference**: `${environments.dev.common.extractor.target_url}`
- **Nested defaults**: `${oc.env:VAR,${common.default_value}}`

#### Examples:

```yaml
# Common configuration
common:
  extractor:
    target_url: ${env_required:TARGET_URL,Target URL is required}
    default_chunk_size: 7

# Job using interpolation
jobs:
  system_a_extractor:
    # Job-specific
    source_url: ${env_required:SYSTEM_A_URL,Source URL required}
    # Reference common values
    target_url: ${environments.dev.common.extractor.target_url}
    chunk_size_days: ${oc.env:CHUNK_SIZE,${environments.dev.common.extractor.default_chunk_size}}
```

#### Legacy YAML Syntax (Deprecated):

```yaml
# Basic environment variable
url: "${API_URL}"

# With default value
timeout: ${TIMEOUT:-30}

# Required variable (fails if not set)
api_key: "${API_KEY:?API key is required}"

# In nested structures
headers:
  Authorization: "Bearer ${TOKEN:-default-token}"
```

### Configuration Structure

```yaml
# Global settings
max_workers: 10

# Job definitions
jobs:
  - id: job_id
    name: Human Readable Name
    enabled: true|false
    script: "/path/to/script.py"
    args: ["--arg1", "value1"]
    schedule:
      type: cron|interval|date
      args: {...}
    # Job-specific configuration...
```

### Job Configuration

All jobs are Python scripts. Each job executes a Python script with command-line arguments.

#### Python Script Jobs
Execute Python scripts with command-line arguments.

```yaml
- id: data_processor
  name: Data Processor
  enabled: true
  script: "/opt/scripts/process_data.py"
  args: ["--input", "/var/data/input", "--output", "/var/data/output"]
  schedule:
    type: cron
    args:
      hour: 1
      minute: 0
```

#### Built-in Jobs

The scheduler comes with two built-in job scripts:

1. **Extractor Job**: Data extraction with multiple job configurations (system_a_extractor, system_b_extractor)
2. **Prime-Collector Job**: Prime number collection and analysis

### Scheduling

#### Cron Schedule
```yaml
schedule:
  type: cron
  args:
    year: 2024
    month: 1-12
    day: 1-31
    week: 1-53
    day_of_week: 0-6  # 0=Monday
    hour: 0-23
    minute: 0-59
    second: 0-59
```

#### Interval Schedule
```yaml
schedule:
  type: interval
  args:
    weeks: 0
    days: 0
    hours: 0
    minutes: 30
    seconds: 0
```

#### Date Schedule
```yaml
schedule:
  type: date
  args:
    run_date: "2024-12-31 23:59:59"
```

## Advisory Locking

The scheduler includes PostgreSQL-based advisory locking to prevent simultaneous execution of the same job across multiple scheduler instances. This is particularly useful in high-availability deployments where multiple scheduler instances might be running.

### How It Works

- Each job is assigned a unique advisory lock based on its `job_id`
- Before executing a job, the scheduler attempts to acquire a PostgreSQL advisory lock
- If the lock is already held by another scheduler instance, the job execution is skipped
- The lock is automatically released when the job completes (successfully or with an error)
- Locks are also released if the scheduler process terminates unexpectedly

### Database Configuration

Add a `database` section to your scheduler configuration:

```yaml
# Database settings for job locking
database:
  host: ${DB_HOST:-localhost}
  port: ${DB_PORT:-5432}
  name: ${DB_NAME:-scheduler}
  user: ${DB_USER:-scheduler}
  password: ${DB_PASSWORD:?Database password is required}
  ssl_mode: ${DB_SSL_MODE:-prefer}
  connection_timeout: ${DB_CONNECTION_TIMEOUT:-10}
```

### Environment Variables

All environment variables are prefixed with `VM_JOBS_` to avoid conflicts with other applications.

**Required Environment Variables:**
```bash
# Environment selection (REQUIRED - no default)
export VM_JOBS_ENVIRONMENT=local  # or dev, stg, prod
```

**Database Configuration:**
```bash
export VM_JOBS_DB_HOST=your-postgres-host
export VM_JOBS_DB_PORT=5432
export VM_JOBS_DB_NAME=scheduler
export VM_JOBS_DB_USER=scheduler
export VM_JOBS_DB_PASSWORD=your_secure_password
export VM_JOBS_DB_SSL_MODE=require
export VM_JOBS_DB_CONNECTION_TIMEOUT=10
```

**Scheduler Configuration:**
```bash
export VM_JOBS_SCHEDULER_MAX_WORKERS=10
```

**Database Configuration:**
```bash
export VM_JOBS_DB_HOST=localhost
export VM_JOBS_DB_PORT=5432
export VM_JOBS_DB_NAME=scheduler
export VM_JOBS_DB_USER=scheduler
export VM_JOBS_DB_PASSWORD=your_password
export VM_JOBS_DB_SSL_MODE=prefer
export VM_JOBS_DB_CONNECTION_TIMEOUT=10
```

**Victoria Metrics Configuration:**
```bash
export VM_JOBS_VM_URL=https://victoria-metrics.example.com
export VM_JOBS_VM_TIMEOUT=30
export VM_JOBS_VM_RETRY_ATTEMPTS=3
export VM_JOBS_VM_RETRY_DELAY=1
```

**Job-Specific Configuration:**
```bash
# System A source configuration (extractor saves to database)
export VM_JOBS_SYSTEM_A_SOURCE_URL=https://system-a.example.com/api
export VM_JOBS_SYSTEM_A_START_OFFSET=0
export VM_JOBS_SYSTEM_A_CHUNK_SIZE=7

# System B source configuration (extractor saves to database)
export VM_JOBS_SYSTEM_B_SOURCE_URL=https://system-b.example.com/api
export VM_JOBS_SYSTEM_B_START_OFFSET=0
export VM_JOBS_SYSTEM_B_CHUNK_SIZE=30

# Apex collector configuration (sends to Victoria Metrics via VM_JOBS_VM_URL)
export VM_JOBS_APEX_SOURCE_URL=https://apex.example.com/api
export VM_JOBS_APEX_SOURCE_TOKEN=apex_token
```

### Database Setup

Create a PostgreSQL database and user for the scheduler:

```sql
-- Create database
CREATE DATABASE scheduler;

-- Create user
CREATE USER scheduler WITH PASSWORD 'your_secure_password';

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE scheduler TO scheduler;

-- Connect to the database and grant schema privileges
\c scheduler
GRANT ALL ON SCHEMA public TO scheduler;
```

### Password Handling

The system automatically handles special characters in database passwords by URL-encoding them. This means you can use passwords with special characters like `@`, `:`, `/`, `?`, `#`, `[`, `]`, `%`, `+`, `&`, `=`, etc.

**Examples of supported password formats:**
```bash
# Simple password
export VM_JOBS_DB_PASSWORD=mypassword123

# Password with special characters
export VM_JOBS_DB_PASSWORD=my@pass:word#123

# Complex password with multiple special characters
export VM_JOBS_DB_PASSWORD=P@ssw0rd!@#$%^&*()

# Password with spaces and special characters
export VM_JOBS_DB_PASSWORD="my password with spaces & symbols!"
```

**Note:** The system automatically URL-encodes the password when building the database connection string, so you don't need to manually encode special characters.

### Victoria Metrics Setup

The service requires a Victoria Metrics instance for metrics collection and monitoring:

1. **Install Victoria Metrics** (if not already installed):
   ```bash
   # Using Docker
   docker run -d --name victoria-metrics \
     -p 8428:8428 \
     -v victoria-metrics-data:/victoria-metrics-data \
     victoriametrics/victoria-metrics:latest
   
   # Or using binary
   wget https://github.com/VictoriaMetrics/VictoriaMetrics/releases/latest/download/victoria-metrics-linux-amd64.tar.gz
   tar -xzf victoria-metrics-linux-amd64.tar.gz
   ./victoria-metrics-prod -storageDataPath=/var/lib/victoria-metrics-data
   ```

2. **Configure Victoria Metrics**:
   - Set up authentication if needed
   - Configure retention policies
   - Set up backup strategies

3. **Verify Installation**:
   ```bash
   curl http://localhost:8428/api/v1/status/buildinfo
   ```

### Fallback Behavior

If no database configuration is provided, the scheduler will:
- Log a warning about missing database configuration
- Execute jobs without advisory locking
- Continue to function normally (but without duplicate execution protection)

### Lock ID Generation

Advisory lock IDs are generated using SHA-256 hashing of the job ID to ensure:
- Consistent lock IDs across scheduler restarts
- No collisions between different job IDs
- Positive integer values (required by PostgreSQL advisory locks)

## Usage

### Service Management

```bash
# Start the service
sudo systemctl start victoria_metrics_jobs

# Stop the service
sudo systemctl stop victoria_metrics_jobs

# Restart the service
sudo systemctl restart victoria_metrics_jobs

# Check status
sudo systemctl status victoria_metrics_jobs

# View logs
sudo journalctl -u victoria_metrics_jobs -f
```

### Manual Job Execution

You can execute individual jobs directly without the scheduler service for testing or manual runs. This is particularly useful for:
- Testing job configurations
- Manual data extraction
- Debugging job issues
- One-off data processing tasks

#### Direct Module Execution

Run jobs directly using Python modules from the **project root directory**:

**IMPORTANT:** All job commands must be run from the project root directory (`/path/to/victoria_metrics_jobs`), not from within the `jobs/` directory.

**Note:** Use `poetry run python` to ensure dependencies are available, or activate the Poetry environment first.

```bash
# Execute extractor job directly (using local environment config)
VM_JOBS_ENVIRONMENT=local poetry run python -m victoria_metrics_jobs.jobs.extractor --config victoria_metrics_jobs/victoria_metrics_jobs.yml --job-id system_a_extractor

# Execute apex collector job directly (using dev environment config)
VM_JOBS_ENVIRONMENT=dev poetry run python -m victoria_metrics_jobs.jobs.apex_collector --config victoria_metrics_jobs/victoria_metrics_jobs.yml --job-id apex_collector

# List available job configurations
VM_JOBS_ENVIRONMENT=local poetry run python -m victoria_metrics_jobs.jobs.extractor --config victoria_metrics_jobs/victoria_metrics_jobs.yml --list-jobs

# Run with verbose logging
VM_JOBS_ENVIRONMENT=local poetry run python -m victoria_metrics_jobs.jobs.extractor --config victoria_metrics_jobs/victoria_metrics_jobs.yml --job-id system_a_extractor --verbose
```

**Alternative (if Poetry environment is activated):**
```bash
# Activate Poetry environment first
poetry shell

# Then run with python directly
VM_JOBS_ENVIRONMENT=local python -m victoria_metrics_jobs.jobs.extractor --config victoria_metrics_jobs/victoria_metrics_jobs.yml --job-id system_a_extractor
```

#### Job Execution Features

When running jobs directly, you get:
- **Full configuration loading**: Jobs load their specific configuration from the YAML file
- **Environment variable support**: All environment variables are resolved
- **Database connectivity**: Jobs can connect to PostgreSQL for advisory locking (optional)
- **Comprehensive logging**: Detailed logs for debugging and monitoring
- **JSON output**: Structured results for easy parsing and integration
- **Error handling**: Graceful error handling with detailed error messages

#### Example Output

```json
{
  "job_id": "system_a_extractor",
  "job_display_name": "System A Data Extraction (Dev)",
  "job_description": "",
  "started_at": "2025-10-01T14:50:26.209380",
  "completed_at": "2025-10-01T14:50:26.640315",
  "status": "success",
  "message": "Extraction completed for job: system_a_extractor",
  "job_config": {
    "job_type": "extractor",
    "source_url": "https://api.example.com/data",
    "target_url": "https://victoria-metrics.example.com",
    "chunk_size_days": 7
  },
  "timestamp": "2025-10-01T14:50:26.209315",
  "execution_time_seconds": 0.43148
}
```

### Manual Scheduler Service Execution

```bash
# Run victoria_metrics_jobs service with custom config
VM_JOBS_ENVIRONMENT=dev python -m victoria_metrics_jobs.scheduler.service --config victoria_metrics_jobs/victoria_metrics_jobs.yml

# Run victoria_metrics_jobs service with debug logging
VM_JOBS_ENVIRONMENT=dev python -m victoria_metrics_jobs.scheduler.service --config victoria_metrics_jobs/victoria_metrics_jobs.yml --log-level DEBUG

# Run victoria_metrics_jobs service with different environment
VM_JOBS_ENVIRONMENT=prod python -m victoria_metrics_jobs.scheduler.service --config victoria_metrics_jobs/victoria_metrics_jobs.yml
```

### Configuration Management

```bash
# Edit configuration
nano victoria_metrics_jobs/victoria_metrics_jobs.yml

# Validate configuration
python -c "import yaml; yaml.safe_load(open('victoria_metrics_jobs/victoria_metrics_jobs.yml'))"

# Test configuration with specific environment
VM_JOBS_ENVIRONMENT=dev python -m victoria_metrics_jobs.scheduler.service --config victoria_metrics_jobs/victoria_metrics_jobs.yml --dry-run
```

## Logging

Logs are written to `/var/log/victoria_metrics_jobs/victoria_metrics_jobs.log` by default with automatic rotation:

- Maximum file size: 10MB
- Backup files: 5
- Log level: INFO (configurable)

### Log Levels

- `DEBUG`: Detailed information for debugging
- `INFO`: General information about job execution
- `WARNING`: Warning messages for non-critical issues
- `ERROR`: Error messages for failed jobs
- `CRITICAL`: Critical errors that may cause service failure

### Viewing Logs

```bash
# View current logs
sudo tail -f /var/log/victoria_metrics_jobs/victoria_metrics_jobs.log

# View systemd logs
sudo journalctl -u victoria_metrics_jobs

# View logs with timestamps
sudo journalctl -u victoria_metrics_jobs --since "1 hour ago"
```

## Security

The service runs with several security restrictions:

- Dedicated system user (`victoria_metrics_jobs`)
- No new privileges
- Private temporary directory
- Protected system directories
- Resource limits (memory, CPU, file descriptors)
- Read-only access to most system directories

## Troubleshooting

### Common Issues

1. **Service fails to start**
   - Check configuration syntax: `python -c "import yaml; yaml.safe_load(open('/etc/victoria_metrics_jobs/config.yml'))"`
   - Check file permissions: `ls -la /etc/victoria_metrics_jobs/config.yml`
   - Check logs: `sudo journalctl -u victoria_metrics_jobs`

2. **Jobs not executing**
   - Verify job is enabled: `enabled: true`
   - Check schedule configuration
   - Verify job command/script exists and is executable
   - Check job-specific logs in the main log file

3. **Permission errors**
   - Ensure `victoria_metrics_jobs` user owns log directory: `sudo chown victoria_metrics_jobs:victoria_metrics_jobs /var/log/victoria_metrics_jobs`
   - Check file permissions for scripts and commands
   - Verify working directory permissions

### Debug Mode

Enable debug logging for detailed information:

```bash
sudo systemctl edit victoria_metrics_jobs
```

Add:
```ini
[Service]
Environment=VICTORIA_METRICS_JOBS_DEBUG=true
```

Then restart:
```bash
sudo systemctl restart victoria_metrics_jobs
```

## Development

### Running in Development

```bash
# Install dependencies
poetry install

# Run with debug logging
VM_JOBS_ENVIRONMENT=dev VICTORIA_METRICS_JOBS_DEBUG=true poetry run python -m victoria_metrics_jobs.scheduler.service --log-level DEBUG --config victoria_metrics_jobs/victoria_metrics_jobs.yml
```

### Testing

The project includes a comprehensive test suite with unit and integration tests:

```bash
# Run all tests
make test

# Run unit tests only
make test-unit

# Run integration tests only
make test-integration

# Run tests with coverage report
make test-coverage

# Run tests in watch mode
make test-watch

# Clean test artifacts
make clean
```

**Test Structure:**
```
tests/
├── unit/                    # Unit tests (26 tests)
│   ├── test_advisory_locks.py
│   ├── test_config.py
│   └── test_jobs.py
├── integration/             # Integration tests (3 tests)
│   └── test_job_execution.py
├── fixtures/                # Test configuration files
│   └── test_*.yml
└── conftest.py             # Shared pytest fixtures
```

**Test Coverage:**
- Database and advisory locking functionality
- Configuration loading and validation
- Job execution and error handling
- End-to-end job execution scenarios

**Linting:**
```bash
# Lint code
poetry run flake8 scheduler/

# Format code
make format
```

## License

[Add your license information here]

## Contributing

[Add contribution guidelines here]
