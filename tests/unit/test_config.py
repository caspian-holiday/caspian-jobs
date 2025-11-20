"""
Unit tests for configuration loading functionality.
"""

import os
import tempfile
import yaml
from pathlib import Path

import pytest

from victoria_metrics_jobs.scheduler.config import ConfigLoader


@pytest.mark.unit
def test_config_loader_initialization():
    """Test ConfigLoader initialization."""
    loader = ConfigLoader()
    assert loader is not None


@pytest.mark.unit
def test_load_valid_config(temp_config_file):
    """Test loading a valid configuration file."""
    loader = ConfigLoader()
    config = loader.load(temp_config_file)
    
    assert isinstance(config, dict)
    assert 'max_workers' in config
    assert 'database' in config
    assert 'jobs' in config
    assert config['max_workers'] == 5


@pytest.mark.unit
def test_load_nonexistent_config():
    """Test loading a non-existent configuration file."""
    loader = ConfigLoader()
    
    with pytest.raises(FileNotFoundError):
        loader.load('/nonexistent/path/config.yml')


@pytest.mark.unit
def test_load_invalid_yaml():
    """Test loading an invalid YAML file."""
    loader = ConfigLoader()
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
        f.write("invalid: yaml: content: [")
        temp_file = f.name
    
    try:
        with pytest.raises(Exception):  # yaml.YAMLError
            loader.load(temp_file)
    finally:
        os.unlink(temp_file)


@pytest.mark.unit
def test_validate_config_structure():
    """Test configuration structure validation."""
    loader = ConfigLoader()
    
    # Valid config
    valid_config = {
        'jobs': [
            {
                'id': 'test_job',
                'name': 'Test Job',
                'enabled': True,
                'script': '/path/to/script.py',
                'schedule': {
                    'type': 'cron',
                    'args': {'hour': 1, 'minute': 0}
                }
            }
        ]
    }
    
    # Should not raise an exception
    loader._validate_config(valid_config)
    
    # Invalid config - jobs not a list
    invalid_config = {
        'jobs': 'not_a_list'
    }
    
    with pytest.raises(ValueError, match="Jobs must be a list"):
        loader._validate_config(invalid_config)


@pytest.mark.unit
def test_validate_job_configuration():
    """Test individual job configuration validation."""
    loader = ConfigLoader()
    
    # Valid job
    valid_job = {
        'id': 'test_job',
        'name': 'Test Job',
        'enabled': True,
        'script': '/path/to/script.py',
        'schedule': {
            'type': 'cron',
            'args': {'hour': 1, 'minute': 0}
        }
    }
    
    # Should not raise an exception
    loader._validate_job(valid_job, 0)
    
    # Invalid job - missing required field
    invalid_job = {
        'name': 'Test Job',
        'enabled': True,
        'script': '/path/to/script.py'
    }
    
    with pytest.raises(ValueError, match="Job 0 missing required field: id"):
        loader._validate_job(invalid_job, 0)
    
    # Invalid job - enabled not boolean
    invalid_job2 = {
        'id': 'test_job',
        'name': 'Test Job',
        'enabled': 'true',  # Should be boolean
        'script': '/path/to/script.py',
        'schedule': {
            'type': 'cron',
            'args': {'hour': 1, 'minute': 0}
        }
    }
    
    with pytest.raises(ValueError, match="Job 0 'enabled' field must be a boolean"):
        loader._validate_job(invalid_job2, 0)


@pytest.mark.unit
def test_validate_schedule_configuration():
    """Test schedule configuration validation."""
    loader = ConfigLoader()
    
    # Valid cron schedule
    valid_schedule = {
        'type': 'cron',
        'args': {'hour': 1, 'minute': 0}
    }
    
    # Should not raise an exception
    loader._validate_schedule(valid_schedule, 0)
    
    # Invalid schedule - missing type
    invalid_schedule = {
        'args': {'hour': 1, 'minute': 0}
    }
    
    with pytest.raises(ValueError, match="Job 0 schedule missing required field: type"):
        loader._validate_schedule(invalid_schedule, 0)
    
    # Invalid schedule - invalid type
    invalid_schedule2 = {
        'type': 'invalid_type',
        'args': {'hour': 1, 'minute': 0}
    }
    
    with pytest.raises(ValueError, match="Job 0 schedule has invalid type 'invalid_type'"):
        loader._validate_schedule(invalid_schedule2, 0)


@pytest.mark.unit
def test_load_environment_config():
    """Test loading environment-specific configuration."""
    loader = ConfigLoader()
    
    # Create a temporary environment config
    env_config = {
        'max_workers': 3,
        'database': {
            'host': 'env-db.example.com',
            'port': 5432,
            'name': 'env_scheduler',
            'user': 'env_user',
            'password': 'env_password'
        },
        'jobs': []
    }
    
    # Create the directory structure
    test_dir = tempfile.mkdtemp()
    env_dir = os.path.join(test_dir, 'test')
    os.makedirs(env_dir, exist_ok=True)
    
    config_file = os.path.join(env_dir, 'config.yml')
    with open(config_file, 'w') as f:
        yaml.dump(env_config, f)
    
    try:
        config = loader.load_environment_config('test', base_path=test_dir)
        assert config['max_workers'] == 3
        assert config['database']['host'] == 'env-db.example.com'
        assert config['environment'] == 'test'
    finally:
        import shutil
        shutil.rmtree(test_dir)
