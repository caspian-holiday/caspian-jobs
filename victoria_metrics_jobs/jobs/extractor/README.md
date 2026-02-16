# Extractor Job

A simplified data extraction job with common configuration and job-specific parameters.

## Features

- **Common Configuration**: Shared settings for source URL, token, and target URL
- **Job-specific Parameters**: Each job has configurable date offset and chunk size
- **Environment Variable Support**: Configuration files support environment variable substitution
- **Comprehensive Logging**: Structured logging with configurable levels
- **JSON Output**: Results output in JSON format for easy parsing

## Usage

### Command Line

```bash
# List available jobs
python extractor.py --config config.yml --list-jobs

# Run extraction for system_a_extractor
python extractor.py --config config.yml --job system_a_extractor

# Run extraction for system_b_extractor
python extractor.py --config config.yml --job system_b_extractor

# Verbose output
python extractor.py --config config.yml --job system_a_extractor --verbose
```

### Configuration File

Create a `config.yml` file (YAML format with `.yml` extension) with common configuration and job-specific parameters:

```yaml
# Common configuration
common:
  source_url: ${SOURCE_URL:?Source URL is required}
  source_token: ${SOURCE_TOKEN:?Source token is required}
  target_url: ${TARGET_URL:?Target URL is required}

# Job configurations
# Each job_id has start_date_offset_days and chunk_size_days parameters
jobs:
  system_a_extractor:
    start_date_offset_days: ${SYSTEM_A_START_OFFSET:-0}
    chunk_size_days: ${SYSTEM_A_CHUNK_SIZE:-7}

  system_b_extractor:
    start_date_offset_days: ${SYSTEM_B_START_OFFSET:-0}
    chunk_size_days: ${SYSTEM_B_CHUNK_SIZE:-30}
```

#### Environment Variable Syntax:

- **Basic substitution**: `${VAR}` or `$VAR`
- **Default values**: `${VAR:-default_value}`
- **Required variables**: `${VAR:?error_message}`

#### Common Environment Variables:

- `SOURCE_URL`: Required source URL for data extraction
- `SOURCE_TOKEN`: Required authentication token for source system
- `TARGET_URL`: Required target URL for data delivery
- `SYSTEM_A_START_OFFSET`: Start date offset in days for system_a_extractor (default: 0)
- `SYSTEM_A_CHUNK_SIZE`: Chunk size in days for system_a_extractor (default: 7)
- `SYSTEM_B_START_OFFSET`: Start date offset in days for system_b_extractor (default: 0)
- `SYSTEM_B_CHUNK_SIZE`: Chunk size in days for system_b_extractor (default: 30)

### Scheduler Integration

Add extractor jobs to your scheduler configuration:

```yaml
jobs:
  # System A extraction
  - id: system_a_extractor
    name: System A Data Extraction
    type: python
    enabled: true
    script: "/opt/victoria_metrics_jobs/jobs/extractor/extractor.py"
    args: ["--config", "/etc/victoria_metrics_jobs/jobs/extractor.yml", "--job", "system_a_extractor"]
    schedule:
      type: cron
      args:
        hour: 2
        minute: 0

  # System B extraction
  - id: system_b_extractor
    name: System B Data Extraction
    type: python
    enabled: true
    script: "/opt/victoria_metrics_jobs/jobs/extractor/extractor.py"
    args: ["--config", "/etc/victoria_metrics_jobs/jobs/extractor.yml", "--job", "system_b_extractor"]
    schedule:
      type: cron
      args:
        hour: 2
        minute: 30
```

## Implementation

### ExtractorJob Class

The `ExtractorJob` class provides a simplified interface:

```python
from extractor import ExtractorJob

# Initialize with configuration
extractor = ExtractorJob(config_path='config.yml')

# Run job for specific job_id
results = extractor.run_job('system_a_extractor')
```

### run_job Method

The `run_job` method is the main entry point for extraction:

```python
def run_job(self, job_id: str) -> Dict[str, Any]:
    """Run the extractor job for a specific job_id.
    
    This method should be implemented as needed for specific extraction requirements.
    
    Args:
        job_id: Name of the extraction job to use
        
    Returns:
        Job execution results
    """
```

**Note**: The `run_job` method is a placeholder that should be customized as needed for specific extraction requirements.

### Parameters

#### Common Configuration

The common configuration section defines shared settings:

- `source_url`: Required source URL for data extraction
- `source_token`: Required authentication token for source system
- `target_url`: Required target URL for data delivery

#### Job Configuration

Each job in the configuration can define:

- `start_date_offset_days`: Number of days to offset from the start date (default: 0)
- `chunk_size_days`: Size of data chunks in days for processing

#### Environment Variables

All configuration values support environment variable substitution:

- `${VAR}`: Basic substitution
- `${VAR:-default}`: With default value
- `${VAR:?error}`: Required variable with error message

### Output

The extractor outputs JSON results:

```json
{
  "job_name": "extractor",
  "job_id": "system_a_extractor",
  "started_at": "2024-01-01T12:00:00",
  "completed_at": "2024-01-01T12:00:05",
  "status": "success",
  "message": "Extraction completed for job: system_a_extractor",
  "job_config": {
    "start_date_offset_days": 0,
    "chunk_size_days": 7
  },
  "common_config": {
    "source_url": "https://api.example.com/data",
    "target_url": "https://target.example.com/api"
  }
}
```

### Error Handling

The extractor handles errors gracefully:

```json
{
  "job_name": "extractor",
  "job_id": "system_a_extractor",
  "started_at": "2024-01-01T12:00:00",
  "completed_at": "2024-01-01T12:00:01",
  "status": "error",
  "error": "Required environment variable 'SOURCE_URL' not set"
}
```

## Customization

### Implementing run_job

To customize the extraction logic, modify the `run_job` method in the `ExtractorJob` class:

```python
def run_job(self, job_id: str) -> Dict[str, Any]:
    """Run the extractor job for a specific job_id."""
    start_time = datetime.now()
    
    try:
        # Get common configuration
        common_config = self.get_common_config()
        job_config = self.get_job_config(job_id)
        
        # Implement your extraction logic here
        source_url = common_config.get('source_url')
        source_token = common_config.get('source_token')
        target_url = common_config.get('target_url')
        
        start_offset = job_config.get('start_date_offset_days', 0)
        chunk_size = job_config.get('chunk_size_days', 7)
        
        # Example: Extract data from source
        # data = extract_from_source(source_url, source_token, start_offset, chunk_size)
        
        # Example: Send data to target
        # send_to_target(data, target_url)
        
        return {
            'job_name': 'extractor',
            'job_id': job_id,
            'status': 'success',
            'message': f'Extraction completed for job: {job_id}',
            'started_at': start_time.isoformat(),
            'completed_at': datetime.now().isoformat(),
            'job_config': job_config,
            'common_config': common_config
        }
        
    except Exception as e:
        return {
            'job_name': 'extractor',
            'job_id': job_id,
            'status': 'error',
            'error': str(e),
            'started_at': start_time.isoformat(),
            'completed_at': datetime.now().isoformat()
        }
```

### Adding New Jobs

To add new jobs, simply add them to the configuration file:

```yaml
jobs:
  system_c_extractor:
    start_date_offset_days: ${SYSTEM_C_START_OFFSET:-0}
    chunk_size_days: ${SYSTEM_C_CHUNK_SIZE:-14}
```

## Examples

### Basic Usage

```bash
# Set required environment variables
export SOURCE_URL="https://api.example.com/data"
export SOURCE_TOKEN="your-api-token"
export TARGET_URL="https://target.example.com/api"

# Run extraction
python extractor.py --config config.yml --job system_a_extractor
```

### With Custom Configuration

```bash
# Override default values
export SYSTEM_A_START_OFFSET=7
export SYSTEM_A_CHUNK_SIZE=14

# Run with verbose logging
python extractor.py --config config.yml --job system_a_extractor --verbose
```

### Scheduler Integration

```yaml
# In your scheduler configuration
jobs:
  - id: daily_extraction
    name: Daily Data Extraction
    type: python
    enabled: true
    script: "/opt/victoria_metrics_jobs/jobs/extractor/extractor.py"
    args: ["--config", "/etc/victoria_metrics_jobs/jobs/extractor.yml", "--job", "system_a_extractor"]
    schedule:
      type: cron
      args:
        hour: 2
        minute: 0
```