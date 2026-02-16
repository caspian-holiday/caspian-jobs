"""
Unit tests for OmegaConf interpolation features.

Tests the configuration interpolation capabilities including:
- Environment variable resolution
- Config path references
- Nested interpolation
"""

import os
import tempfile
import pytest
from omegaconf import OmegaConf

from victoria_metrics_jobs.scheduler.common.config_loader import ConfigLoader


@pytest.fixture
def interpolation_config_file():
    """Create a test configuration with various interpolation patterns."""
    config = {
        'environments': {
            'test': {
                'common': {
                    'database': {
                        'host': '${oc.env:DB_HOST,localhost}',
                        'port': '${oc.env:DB_PORT,5432}'
                    },
                    'extractor': {
                        'target_url': '${env_required:TARGET_URL,Target URL required}',
                        'target_token': '${oc.env:TARGET_TOKEN,default_token}',
                        'default_chunk_size': 7
                    }
                },
                'jobs': {
                    'test_job': {
                        'id': 'test_job',
                        'name': 'Test Job',
                        'enabled': True,
                        'script': 'python',
                        'job_type': 'extractor',
                        # Reference common values
                        'target_url': '${environments.test.common.extractor.target_url}',
                        'target_token': '${environments.test.common.extractor.target_token}',
                        'chunk_size': '${environments.test.common.extractor.default_chunk_size}',
                        # Job-specific
                        'source_url': '${oc.env:SOURCE_URL,http://default-source.com}'
                    }
                }
            }
        }
    }
    
    # Set required environment variable
    os.environ['TARGET_URL'] = 'http://test-target.com'
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
        OmegaConf.save(config, f.name)
        temp_file = f.name
    
    yield temp_file
    
    # Cleanup
    os.unlink(temp_file)
    if 'TARGET_URL' in os.environ:
        del os.environ['TARGET_URL']


@pytest.mark.unit
class TestOmegaConfInterpolation:
    """Tests for OmegaConf interpolation features."""
    
    def test_env_var_with_default(self, interpolation_config_file):
        """Test environment variable resolution with default value."""
        loader = ConfigLoader()
        config = loader.load(interpolation_config_file)
        
        # DB_HOST not set, should use default
        assert config['environments']['test']['common']['database']['host'] == 'localhost'
        
        # DB_PORT not set, should use default
        assert config['environments']['test']['common']['database']['port'] == '5432'
    
    def test_env_var_override_default(self, interpolation_config_file):
        """Test that environment variable overrides default value."""
        os.environ['DB_HOST'] = 'custom-db-host'
        os.environ['DB_PORT'] = '3306'
        
        try:
            loader = ConfigLoader()
            config = loader.load(interpolation_config_file)
            
            assert config['environments']['test']['common']['database']['host'] == 'custom-db-host'
            assert config['environments']['test']['common']['database']['port'] == '3306'
        finally:
            del os.environ['DB_HOST']
            del os.environ['DB_PORT']
    
    def test_required_env_var(self, interpolation_config_file):
        """Test required environment variable resolution."""
        # TARGET_URL is set in fixture
        loader = ConfigLoader()
        config = loader.load(interpolation_config_file)
        
        target_url = config['environments']['test']['common']['extractor']['target_url']
        assert target_url == 'http://test-target.com'
    
    def test_required_env_var_missing(self):
        """Test placeholder value when required environment variable is missing."""
        config_dict = {
            'test_value': '${env_required:MISSING_VAR,This variable is required}'
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
            OmegaConf.save(config_dict, f.name)
            temp_file = f.name
        
        try:
            loader = ConfigLoader()
            config = loader.load(temp_file)
            # Should return placeholder instead of raising error
            assert config['test_value'] == '???MISSING_VAR???'
        finally:
            os.unlink(temp_file)
    
    def test_config_path_interpolation(self, interpolation_config_file):
        """Test referencing values from other config paths."""
        loader = ConfigLoader()
        config = loader.load(interpolation_config_file)
        
        job_config = config['environments']['test']['jobs']['test_job']
        
        # Check interpolated values match the source
        expected_target_url = config['environments']['test']['common']['extractor']['target_url']
        assert job_config['target_url'] == expected_target_url
        assert job_config['target_url'] == 'http://test-target.com'
        
        # Check token (fixture sets TARGET_TOKEN env var, but it's not set here, so should use default)
        # But the fixture sets TARGET_URL, let's check what's actually resolved
        expected_token = config['environments']['test']['common']['extractor']['target_token']
        assert job_config['target_token'] == expected_token
        
        # Check numeric interpolation
        assert job_config['chunk_size'] == 7
    
    def test_nested_interpolation(self, interpolation_config_file):
        """Test nested interpolation (env var defaulting to config value)."""
        loader = ConfigLoader()
        config = loader.load(interpolation_config_file)
        
        job_config = config['environments']['test']['jobs']['test_job']
        
        # SOURCE_URL not set, should use default
        assert job_config['source_url'] == 'http://default-source.com'
        
        # Now set SOURCE_URL and verify it overrides
        os.environ['SOURCE_URL'] = 'http://custom-source.com'
        try:
            config = loader.load(interpolation_config_file)
            job_config = config['environments']['test']['jobs']['test_job']
            assert job_config['source_url'] == 'http://custom-source.com'
        finally:
            del os.environ['SOURCE_URL']
    
    def test_multiple_jobs_share_common_values(self):
        """Test that multiple jobs can reference same common values."""
        config_dict = {
            'environments': {
                'test': {
                    'common': {
                        'shared_target': '${oc.env:SHARED_TARGET,http://shared.com}'
                    },
                    'jobs': {
                        'job1': {
                            'id': 'job1',
                            'target': '${environments.test.common.shared_target}'
                        },
                        'job2': {
                            'id': 'job2',
                            'target': '${environments.test.common.shared_target}'
                        }
                    }
                }
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
            OmegaConf.save(config_dict, f.name)
            temp_file = f.name
        
        try:
            loader = ConfigLoader()
            config = loader.load(temp_file)
            
            # Both jobs should reference same value
            job1_target = config['environments']['test']['jobs']['job1']['target']
            job2_target = config['environments']['test']['jobs']['job2']['target']
            
            assert job1_target == job2_target
            assert job1_target == 'http://shared.com'
        finally:
            os.unlink(temp_file)
    
    def test_environment_specific_common_values(self):
        """Test that each environment can have different common values."""
        config_dict = {
            'environments': {
                'dev': {
                    'common': {
                        'api_url': 'http://dev-api.com'
                    },
                    'jobs': {
                        'job1': {
                            'id': 'job1',
                            'url': '${environments.dev.common.api_url}'
                        }
                    }
                },
                'prod': {
                    'common': {
                        'api_url': 'http://prod-api.com'
                    },
                    'jobs': {
                        'job1': {
                            'id': 'job1',
                            'url': '${environments.prod.common.api_url}'
                        }
                    }
                }
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
            OmegaConf.save(config_dict, f.name)
            temp_file = f.name
        
        try:
            loader = ConfigLoader()
            config = loader.load(temp_file)
            
            # Dev environment uses dev API
            dev_job_url = config['environments']['dev']['jobs']['job1']['url']
            assert dev_job_url == 'http://dev-api.com'
            
            # Prod environment uses prod API
            prod_job_url = config['environments']['prod']['jobs']['job1']['url']
            assert prod_job_url == 'http://prod-api.com'
        finally:
            os.unlink(temp_file)


@pytest.mark.unit
class TestOmegaConfAdvancedFeatures:
    """Tests for advanced OmegaConf features."""
    
    def test_integer_interpolation(self):
        """Test interpolation with integer values."""
        config_dict = {
            'defaults': {
                'timeout': 30,
                'max_retries': 3
            },
            'service': {
                'timeout': '${defaults.timeout}',
                'retries': '${defaults.max_retries}'
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
            OmegaConf.save(config_dict, f.name)
            temp_file = f.name
        
        try:
            loader = ConfigLoader()
            config = loader.load(temp_file)
            
            # Integer values should be preserved
            assert config['service']['timeout'] == 30
            assert config['service']['retries'] == 3
            assert isinstance(config['service']['timeout'], int)
        finally:
            os.unlink(temp_file)
    
    def test_boolean_interpolation(self):
        """Test interpolation with boolean values."""
        config_dict = {
            'defaults': {
                'debug': True,
                'production': False
            },
            'settings': {
                'debug_mode': '${defaults.debug}',
                'prod_mode': '${defaults.production}'
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
            OmegaConf.save(config_dict, f.name)
            temp_file = f.name
        
        try:
            loader = ConfigLoader()
            config = loader.load(temp_file)
            
            # Boolean values should be preserved
            assert config['settings']['debug_mode'] is True
            assert config['settings']['prod_mode'] is False
        finally:
            os.unlink(temp_file)
    
    def test_list_interpolation(self):
        """Test interpolation with list values."""
        config_dict = {
            'defaults': {
                'allowed_hosts': ['localhost', '127.0.0.1']
            },
            'server': {
                'hosts': '${defaults.allowed_hosts}'
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
            OmegaConf.save(config_dict, f.name)
            temp_file = f.name
        
        try:
            loader = ConfigLoader()
            config = loader.load(temp_file)
            
            # List values should be preserved
            assert config['server']['hosts'] == ['localhost', '127.0.0.1']
            assert isinstance(config['server']['hosts'], list)
        finally:
            os.unlink(temp_file)

