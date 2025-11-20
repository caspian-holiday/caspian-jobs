"""
Pytest configuration and shared fixtures for the scheduler tests.
"""

import os
import sys
import tempfile
from pathlib import Path
from typing import Dict, Any

import pytest
import yaml

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


@pytest.fixture
def temp_config_file():
    """Create a temporary configuration file for testing."""
    config_data = {
        'max_workers': 5,
        'database': {
            'host': 'localhost',
            'port': 5432,
            'name': 'test_scheduler',
            'user': 'test_user',
            'password': 'test_password',
            'ssl_mode': 'prefer',
            'connection_timeout': 10
        },
        'jobs': [
            {
                'id': 'test_job',
                'name': 'Test Job',
                'enabled': True,
                'script': '/path/to/test_script.py',
                'args': ['--test'],
                'schedule': {
                    'type': 'cron',
                    'args': {
                        'hour': 1,
                        'minute': 0
                    }
                }
            }
        ]
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
        yaml.dump(config_data, f)
        temp_file = f.name
    
    yield temp_file
    
    # Cleanup
    try:
        os.unlink(temp_file)
    except OSError:
        pass


@pytest.fixture
def temp_job_config_file():
    """Create a temporary job configuration file for testing."""
    config_data = {
        'common': {
            'source_url': 'http://test.example.com',
            'source_token': 'test_token',
            'target_url': 'http://target.example.com'
        },
        'jobs': {
            'test_job': {
                'start_date_offset_days': 0,
                'chunk_size_days': 7
            }
        }
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
        yaml.dump(config_data, f)
        temp_file = f.name
    
    yield temp_file
    
    # Cleanup
    try:
        os.unlink(temp_file)
    except OSError:
        pass


@pytest.fixture
def mock_environment():
    """Set up mock environment variables for testing."""
    env_vars = {
        'DB_HOST': 'localhost',
        'DB_PORT': '5432',
        'DB_NAME': 'test_scheduler',
        'DB_USER': 'test_user',
        'DB_PASSWORD': 'test_password',
        'SOURCE_URL': 'http://test.example.com',
        'SOURCE_TOKEN': 'test_token',
        'TARGET_URL': 'http://target.example.com'
    }
    
    # Store original values
    original_values = {}
    for key, value in env_vars.items():
        original_values[key] = os.environ.get(key)
        os.environ[key] = value
    
    yield env_vars
    
    # Restore original values
    for key, original_value in original_values.items():
        if original_value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = original_value


@pytest.fixture
def test_fixtures_dir():
    """Get the path to the test fixtures directory."""
    return Path(__file__).parent / 'fixtures'
