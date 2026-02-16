# Apex Collector Job

A simplified apex number collection job with common configuration and job-specific parameters.

## Features

- **Common Configuration**: Shared settings for target URL and token
- **Job-specific Parameters**: Each job has configurable source URL and token
- **Environment Variable Support**: Configuration files support environment variable substitution
- **Comprehensive Logging**: Structured logging with configurable levels
- **JSON Output**: Results output in JSON format for easy parsing

## Usage

### Command Line

```bash
# List available jobs
python apex_collector.py --config config.yml --list-jobs

# Run apex collection for apex_collector
python apex_collector.py --config config.yml --job apex_collector

# Run with verbose logging
python apex_collector.py --config config.yml --job apex_collector --verbose
```

### Configuration File

Create a `config.yml` file (YAML format with `.yml` extension) with common configuration and job-specific parameters:

```yaml
# Common configuration
common:
  target_url: ${TARGET_URL:?Target URL is required}
  target_token: ${TARGET_TOKEN:?Target token is required}

# Job configurations
# Each job_id has source_url and source_token parameters
jobs:
  apex_collector:
    source_url: ${APEX_SOURCE_URL:?Apex source URL is required}
    source_token: ${APEX_SOURCE_TOKEN:?Apex source token is required}
```

#### Environment Variable Syntax:

- **Basic substitution**: `${VAR}` or `$VAR`
- **Default values**: `${VAR:-default_value}`
- **Required variables**: `${VAR:?error_message}`

#### Common Environment Variables:

- `TARGET_URL`: Required target URL for data delivery
- `TARGET_TOKEN`: Required authentication token for target system
- `APEX_SOURCE_URL`: Required source URL for apex data collection
- `APEX_SOURCE_TOKEN`: Required authentication token for apex source system

### Scheduler Integration

Add apex collector job to your scheduler configuration:

```yaml
jobs:
  # Apex collection job
  - id: apex_collector
    name: Apex Number Collection
    type: python
    enabled: true
    script: "/opt/victoria_metrics_jobs/jobs/apex-collector/apex_collector.py"
    args: ["--config", "/etc/victoria_metrics_jobs/jobs/apex-collector.yml", "--job", "apex_collector"]
    schedule:
      type: cron
      args:
        hour: 1
        minute: 0
```

## Implementation

### ApexCollectorJob Class

The `ApexCollectorJob` class provides a simplified interface:

```python
from apex_collector import ApexCollectorJob

# Initialize with configuration
apex_collector = ApexCollectorJob(config_path='config.yml')

# Run apex collection job
results = apex_collector.run_job('apex_collector')
```

### run_job Method

The `run_job` method is the main entry point for apex collection:

```python
def run_job(self, job_id: str) -> Dict[str, Any]:
    """Run the apex collector job for a specific job_id.
    
    This method should be implemented as needed for specific apex collection requirements.
    
    Args:
        job_id: Name of the apex collection job to use
        
    Returns:
        Job execution results
    """
```

**Note**: The `run_job` method is a placeholder that should be customized as needed for specific apex collection requirements.

### Parameters

#### Common Configuration

The common configuration section defines shared settings:

- `target_url`: Required target URL for data delivery
- `target_token`: Required authentication token for target system

#### Job Configuration

Each job in the configuration can define:

- `source_url`: Required source URL for apex data collection
- `source_token`: Required authentication token for apex source system

#### Environment Variables

All configuration values support environment variable substitution:

- `${VAR}`: Basic substitution
- `${VAR:-default}`: With default value
- `${VAR:?error}`: Required variable with error message

### Output

The apex collector outputs JSON results:

```json
{
  "job_name": "apex_collector",
  "job_id": "apex_collector",
  "started_at": "2024-01-01T12:00:00",
  "completed_at": "2024-01-01T12:00:05",
  "status": "success",
  "message": "Apex collection completed for job: apex_collector",
  "job_config": {
    "source_url": "https://api.apex.example.com/data",
    "source_token": "***"
  },
  "common_config": {
    "target_url": "https://target.example.com/api",
    "target_token": "***"
  }
}
```

### Error Handling

The apex collector handles errors gracefully:

```json
{
  "job_name": "apex_collector",
  "job_id": "apex_collector",
  "started_at": "2024-01-01T12:00:00",
  "completed_at": "2024-01-01T12:00:01",
  "status": "error",
  "error": "Required environment variable 'TARGET_URL' not set"
}
```

## Customization

### Implementing run_job

To customize the apex collection logic, modify the `run_job` method in the `ApexCollectorJob` class:

```python
def run_job(self, job_id: str) -> Dict[str, Any]:
    """Run the apex collector job for a specific job_id."""
    start_time = datetime.now()
    
    try:
        # Get common configuration
        common_config = self.get_common_config()
        job_config = self.get_job_config(job_id)
        
        # Implement your apex collection logic here
        source_url = job_config.get('source_url')
        source_token = job_config.get('source_token')
        target_url = common_config.get('target_url')
        target_token = common_config.get('target_token')
        
        # Example: Collect apex data from source
        # apex_data = collect_apex_data(source_url, source_token)
        
        # Example: Send data to target
        # send_to_target(apex_data, target_url, target_token)
        
        return {
            'job_name': 'apex_collector',
            'job_id': job_id,
            'status': 'success',
            'message': f'Apex collection completed for job: {job_id}',
            'started_at': start_time.isoformat(),
            'completed_at': datetime.now().isoformat(),
            'job_config': job_config,
            'common_config': common_config
        }
        
    except Exception as e:
        return {
            'job_name': 'apex_collector',
            'job_id': job_id,
            'status': 'error',
            'error': str(e),
            'started_at': start_time.isoformat(),
            'completed_at': datetime.now().isoformat()
        }
```

## Examples

### Basic Usage

```bash
# Set required environment variables
export TARGET_URL="https://target.example.com/api"
export TARGET_TOKEN="your-target-token"
export APEX_SOURCE_URL="https://api.apex.example.com/data"
export APEX_SOURCE_TOKEN="your-apex-source-token"

# Run apex collection
python apex_collector.py --config config.yml --job apex_collector
```

### With Custom Configuration

```bash
# Set environment variables
export TARGET_URL="https://custom-target.example.com/api"
export TARGET_TOKEN="custom-target-token"
export APEX_SOURCE_URL="https://custom-apex.example.com/data"
export APEX_SOURCE_TOKEN="custom-apex-token"

# Run with verbose logging
python apex_collector.py --config config.yml --job apex_collector --verbose
```

### Scheduler Integration

```yaml
# In your scheduler configuration
jobs:
  - id: daily_apex_collection
    name: Daily Apex Collection
    type: python
    enabled: true
    script: "/opt/victoria_metrics_jobs/jobs/apex-collector/apex_collector.py"
    args: ["--config", "/etc/victoria_metrics_jobs/jobs/apex-collector.yml", "--job", "apex_collector"]
    schedule:
      type: cron
      args:
        hour: 1
        minute: 0
```