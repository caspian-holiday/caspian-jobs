# Victoria Metrics Jobs

This directory contains custom Python job scripts that can be executed by the Victoria Metrics Jobs service. All jobs use a unified functional architecture with step-by-step workflows.

## Job Architecture

All job scripts inherit from a common `BaseJob` class that provides:
- **Step-by-Step Workflows**: Jobs are composed of a series of steps that transform a state object
- **Functional Error Handling**: Uses Result types for clean error propagation without exceptions
- **Unified Configuration Management**: Automatic config loading and validation
- **Standardized Logging**: Consistent logging setup across all jobs
- **Common CLI Interface**: Standardized argument parsing (`--config`, `--job-id`, `--list-jobs`, `--verbose`)
- **State Management**: Clear data flow and accumulation through state objects
- **Execution Timing**: Automatic execution time tracking and status reporting
- **Security**: Automatic sanitization of sensitive configuration data in output
- **Resource Management**: Automatic initialization and cleanup of database and Victoria Metrics managers

## Running Jobs Directly

**IMPORTANT: Working Directory**
All job commands must be run from the **project root directory** (`/path/to/victoria_metrics_jobs`), not from within the `jobs/` directory.

**Required Environment Variable**
All jobs require the `VM_JOBS_ENVIRONMENT` environment variable to be set:
- `local` - For local development
- `dev` - Development environment  
- `stg` - Staging environment
- `prod` - Production environment

**Basic Command Format**
```bash
# From project root directory (with Poetry)
VM_JOBS_ENVIRONMENT=<environment> poetry run python -m victoria_metrics_jobs.jobs.<job_name> --config victoria_metrics_jobs/victoria_metrics_jobs.yml --job-id <job_id>

# Or activate Poetry environment first, then use python directly
poetry shell
VM_JOBS_ENVIRONMENT=<environment> python -m victoria_metrics_jobs.jobs.<job_name> --config victoria_metrics_jobs/victoria_metrics_jobs.yml --job-id <job_id>
```

## Available Jobs

### 1. Extractor Job (`extractor/`)
Data extraction and processing script with step-by-step workflow.

**Workflow Steps:**
1. Derive current business date
2. Derive list of weekdays from start_date_offset_days and current business_date
3. Associate each weekday with timestamp saved in database for that date
4. Associate each weekday with max timestamp saved in Victoria Metrics for that date
5. Derive weekdays that need to be updated by comparing timestamps
6. For each weekday that needs updating, perform extraction

**Features:**
- Multiple job configurations per script
- Job-specific parameters via job ID
- Database and Victoria Metrics integration
- Configurable via YAML with environment variables

**Usage:**
```bash
# List available job configurations
VM_JOBS_ENVIRONMENT=local poetry run python -m victoria_metrics_jobs.jobs.extractor --config victoria_metrics_jobs/victoria_metrics_jobs.yml --list-jobs

# Run specific job configuration
VM_JOBS_ENVIRONMENT=local poetry run python -m victoria_metrics_jobs.jobs.extractor --config victoria_metrics_jobs/victoria_metrics_jobs.yml --job-id system_a_extractor

# Run with verbose logging
VM_JOBS_ENVIRONMENT=local poetry run python -m victoria_metrics_jobs.jobs.extractor --config victoria_metrics_jobs/victoria_metrics_jobs.yml --job-id system_a_extractor --verbose
```

### 2. Apex Collector Job (`apex_collector/`)
Apex number collection and analysis script with step-by-step workflow.

**Workflow Steps:**
1. Derive current business date
2. Derive list of weekdays
3. Derive weekdays that need updating
4. Process weekdays to update

**Features:**
- Job-specific configuration via job ID
- Apex number collection and validation
- Target system integration
- Configurable via YAML with environment variables

**Usage:**
```bash
# Run apex collection job
VM_JOBS_ENVIRONMENT=local poetry run python -m victoria_metrics_jobs.jobs.apex_collector --config victoria_metrics_jobs/victoria_metrics_jobs.yml --job-id apex_collector

# Run with verbose logging
VM_JOBS_ENVIRONMENT=dev poetry run python -m victoria_metrics_jobs.jobs.apex_collector --config victoria_metrics_jobs/victoria_metrics_jobs.yml --job-id apex_collector --verbose
```

### 3. Metrics Forecast Job (`metrics_forecast/`)
Prophet-based forecasting pipeline that projects business-day metrics forward and republishes predictions to Victoria Metrics.

**Workflow Steps:**
1. Derive current business date anchor
2. Collect historical series per configured job label and metric selector
3. Train Prophet models (business-day frequency) and publish forecast variants
4. Emit job status metrics for observability

**Features:**
- Prometheus client API for both queries and remote writes
- Supports multiple forecast types (trend, lower, upper) via `forecast` label
- Filters series by a configurable label (default `source`) populated via `source_job_names`
- Configurable history offset/window, horizon, and Prophet parameters
- `prophet_fit` block passes kwargs straight to `Prophet.fit()` for tuning iterations, algorithm, etc.
- Business-day aware timestamp fabrication (midnight start, +1s for reruns)

**Usage:**
```bash
# List available forecast job configurations
VM_JOBS_ENVIRONMENT=local poetry run python -m victoria_metrics_jobs.jobs.metrics_forecast --config victoria_metrics_jobs/victoria_metrics_jobs.yml --list-jobs

# Run metrics forecast job
VM_JOBS_ENVIRONMENT=local poetry run python -m victoria_metrics_jobs.jobs.metrics_forecast --config victoria_metrics_jobs/victoria_metrics_jobs.yml --job-id metrics_forecast --verbose
```

## Job Implementation Pattern

All jobs follow the same functional pattern:

### 1. Define Job-Specific State

Extend `BaseJobState` with job-specific fields:

```python
@dataclass
class MyJobState(BaseJobState):
    # Add job-specific fields
    custom_field: str
    processed_items: List[Any]
    success_count: int = 0
    
    def to_results(self) -> Dict[str, Any]:
        """Override to add job-specific results."""
        results = super().to_results()
        results.update({
            'custom_field': self.custom_field,
            'processed_items_count': len(self.processed_items),
            'success_count': self.success_count
        })
        return results
```

### 2. Implement Job Class

Extend `BaseJob` and implement required methods:

```python
class MyJob(BaseJob):
    def __init__(self, config_path: str = None, verbose: bool = False):
        super().__init__('my_job', config_path, verbose)
    
    def create_initial_state(self, job_id: str) -> Result[MyJobState, Exception]:
        """Create initial state for job execution."""
        try:
            job_config = self.get_job_config(job_id)
            initial_state = MyJobState(
                job_id=job_id,
                job_config=job_config,
                started_at=datetime.now(),
                custom_field="initial_value",
                processed_items=[]
            )
            return Ok(initial_state)
        except Exception as e:
            return Err(e)
    
    def get_workflow_steps(self) -> List[callable]:
        """Define workflow steps."""
        return [
            self._step1,
            self._step2,
            self._step3
        ]
    
    def finalize_state(self, state: MyJobState) -> MyJobState:
        """Finalize state before converting to results."""
        state.completed_at = datetime.now()
        state.status = 'success'
        state.message = f'Job completed: {state.job_id}'
        return state
    
    # Step implementations
    def _step1(self, state: MyJobState) -> Result[MyJobState, Exception]:
        """Step 1 implementation."""
        # TODO: Implement step logic
        # Modify state as needed
        return Ok(state)
    
    def _step2(self, state: MyJobState) -> Result[MyJobState, Exception]:
        """Step 2 implementation."""
        # TODO: Implement step logic
        # Modify state as needed
        return Ok(state)
    
    def _step3(self, state: MyJobState) -> Result[MyJobState, Exception]:
        """Step 3 implementation."""
        # TODO: Implement step logic
        # Modify state as needed
        return Ok(state)
```

## Core Components

### 1. Result Utilities (`common/result_utils.py`)

The `Result` class provides functional error handling:

```python
from victoria_metrics_jobs.jobs.common import Result, Ok, Err

# Create results
success_result = Ok("value")
error_result = Err(Exception("error"))

# Chain operations
result = (Ok(initial_value)
          .and_then(step1)
          .and_then(step2)
          .and_then(step3))

# Handle results
if result.is_ok:
    final_value = result.unwrap()
else:
    error = result.error
```

### 2. Base State Class (`common/base_job.py`)

`BaseJobState` provides common fields for all job states:

```python
@dataclass
class BaseJobState:
    job_id: str
    job_config: Dict[str, Any]
    started_at: datetime
    completed_at: Optional[datetime] = None
    status: str = 'running'
    message: str = ''
    error: Optional[str] = None
    db_manager: Optional[Any] = None
    vm_manager: Optional[Any] = None
```

### 3. Base Job Class (`common/base_job.py`)

`BaseJob` provides the workflow orchestration framework:

```python
class BaseJob(ABC):
    @abstractmethod
    def create_initial_state(self, job_id: str) -> Result[BaseJobState, Exception]:
        """Create initial state for job execution."""
        pass
    
    @abstractmethod
    def get_workflow_steps(self) -> List[callable]:
        """Get list of workflow steps."""
        pass
    
    @abstractmethod
    def finalize_state(self, state: BaseJobState) -> BaseJobState:
        """Finalize state before converting to results."""
        pass
```

## Adding New Jobs

To add a new job:

1. **Create a new directory** under `jobs/`
2. **Create job-specific state class** extending `BaseJobState`
3. **Create job class** extending `BaseJob`
4. **Implement required methods**: `create_initial_state`, `get_workflow_steps`, `finalize_state`
5. **Define step functions** that return `Result[State, Exception]`
6. **Create a `config.yml` file** with default settings
7. **Create a `README.md`** with documentation
8. **Update the scheduler configuration** to include the job
9. **Redeploy the entire application** to install the new job

## Configuration

Jobs can be configured in two ways:

1. **Command-line arguments**: Direct parameter specification
2. **Configuration files**: YAML files (`.yml` extension) with job settings

Configuration files are loaded first, then command-line arguments override file settings.

### Environment Variable Support

All configuration files support environment variable substitution:

- **Basic substitution**: `${VAR}` or `$VAR`
- **Default values**: `${VAR:-default_value}`
- **Required variables**: `${VAR:?error_message}`

#### Examples:

```yaml
# Basic environment variable
source: "${API_URL}"

# With default value
timeout: ${TIMEOUT:-30}

# Required variable (fails if not set)
api_key: "${API_KEY:?API key is required}"

# In nested structures
headers:
  Authorization: "Bearer ${TOKEN:-default-token}"
```

## Scheduler Integration

Jobs are integrated into the scheduler by adding them to the main configuration file:

```yaml
jobs:
  - id: my_job
    name: My Custom Job
    enabled: true
    script: "/opt/victoria_metrics_jobs/jobs/my-job/my_job.py"
    args: ["--config", "/etc/victoria_metrics_jobs/jobs/my-job/config.yml", "--job-id", "my_job"]
    schedule:
      type: cron
      args:
        hour: 2
        minute: 0
```

## Testing Jobs

Test jobs independently before adding to the scheduler:

```bash
# Test with job ID
VM_JOBS_ENVIRONMENT=local poetry run python -m victoria_metrics_jobs.jobs.extractor --config victoria_metrics_jobs/victoria_metrics_jobs.yml --job-id system_a_extractor

# Test with different environment
VM_JOBS_ENVIRONMENT=dev poetry run python -m victoria_metrics_jobs.jobs.extractor --config victoria_metrics_jobs/victoria_metrics_jobs.yml --job-id system_b_extractor

# Test with verbose output
VM_JOBS_ENVIRONMENT=local poetry run python -m victoria_metrics_jobs.jobs.extractor --config victoria_metrics_jobs/victoria_metrics_jobs.yml --job-id system_a_extractor --verbose
```

## Benefits

### 1. **Testability**
Each step function can be tested independently with mock states.

### 2. **Error Handling**
The Result type provides clean error propagation without exceptions.

### 3. **Composability**
Steps can be easily reordered, added, or removed.

### 4. **State Management**
State objects provide clear data flow and accumulation.

### 5. **Reusability**
Common functionality is shared through base classes.

### 6. **Maintainability**
The functional approach makes the code easier to understand and modify.

## Logging

All jobs use the Python logging module with:

- Configurable log levels
- Structured log messages
- Error and success reporting
- Performance metrics

## Error Handling

Jobs handle common error scenarios:

- Configuration file not found
- Invalid configuration parameters
- Network connectivity issues
- File system errors
- Data processing errors

## Security Features

### Configuration Sanitization

All job results automatically sanitize sensitive configuration data to prevent accidental exposure of passwords, tokens, and other credentials:

**Automatically Masked Fields:**
- `password`, `passwd`, `pwd`
- `token`, `key`, `secret`
- `auth`, `credential`, `cred`
- `api_key`, `apikey`, `access_key`
- `private_key`, `privatekey`
- `session`, `sessionid`
- `bearer`, `authorization`
- `jwt`, `oauth`, `refresh_token`
- `client_secret`, `client_id`
- `encryption_key`, `decryption_key`
- `salt`, `hash`, `checksum`

**Sanitization Behavior:**
- **Strings**: Shows first 2 and last 2 characters (e.g., `secret123` → `se******23`)
- **Short strings (≤4 chars)**: Completely masked (e.g., `pass` → `****`)
- **Non-strings**: Shows type only (e.g., `123` → `<int>`)
- **Nested objects**: Recursively sanitized
- **Custom patterns**: Jobs can add additional sensitive patterns via `_get_additional_sensitive_patterns()`

**Example:**
```python
# Original config
{
    "database_password": "secret123",
    "api_token": "abc123def456",
    "nested": {
        "auth_key": "xyz789"
    }
}

# Sanitized output
{
    "database_password": "se******23",
    "api_token": "ab**********56", 
    "nested": {
        "auth_key": "xy******89"
    }
}
```

## Resource Management

### Automatic Manager Initialization

The `BaseJob` class automatically initializes and manages database and Victoria Metrics connections:

**Database Manager:**
- Automatically initialized when `database` configuration is present
- Available in job state as `state.db_manager`
- Automatically cleaned up after job completion

**Victoria Metrics Manager:**
- Automatically initialized when `victoria_metrics` configuration is present
- Available in job state as `state.vm_manager`
- Automatically cleaned up after job completion

**Usage in Job Steps:**
```python
def _some_step(self, state: YourJobState) -> Result[YourJobState, Exception]:
    # Access database manager
    if state.db_manager:
        data = state.db_manager.query("SELECT * FROM table")
    
    # Access Victoria Metrics manager
    if state.vm_manager:
        metrics = state.vm_manager.query_range("metric_name")
    
    return Ok(state)
```

**Configuration Requirements:**
```yaml
jobs:
  your_job:
    database:
      host: localhost
      port: 5432
      database: your_db
      username: user
      password: secret
    victoria_metrics:
      url: http://localhost:8428
      timeout: 30
```

## Performance Considerations

- Use appropriate timeouts for network operations
- Implement progress reporting for long-running jobs
- Consider memory usage for large data processing
- Add performance benchmarking where applicable
