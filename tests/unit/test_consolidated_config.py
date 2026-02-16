"""
Unit tests for consolidated configuration structure with OmegaConf.

Tests the refactored configuration where all job-related settings
are consolidated under a single branch per job ID, using OmegaConf
for interpolation.
"""

import os
import tempfile
from pathlib import Path

import pytest
from omegaconf import OmegaConf

from victoria_metrics_jobs.scheduler.config import ConfigLoader
from victoria_metrics_jobs.jobs.common.config import JobConfigManager


@pytest.fixture
def consolidated_config():
    """Create a sample consolidated configuration."""
    return {
        'environments': {
            'dev': {
                'max_workers': 5,
                'database': {
                    'host': 'localhost',
                    'port': 5432,
                    'name': 'scheduler_dev',
                    'user': 'scheduler',
                    'password': 'testpass'
                },
                'jobs': {
                    'test_extractor': {
                        # Scheduler attributes
                        'id': 'test_extractor',
                        'name': 'Test Extractor Job',
                        'enabled': True,
                        'script': 'python',
                        'args': ['-m', 'victoria_metrics_jobs.jobs.extractor', '--job-id', 'test_extractor'],
                        'schedule': {
                            'type': 'cron',
                            'args': {'hour': 2, 'minute': 0}
                        },
                        # Job-specific attributes
                        'job_type': 'extractor',
                        'source_url': 'http://test.com',
                        'source_token': 'token123',
                        'target_url': 'http://target.com',
                        'start_date_offset_days': 0,
                        'chunk_size_days': 7
                    },
                    'test_collector': {
                        # Scheduler attributes
                        'id': 'test_collector',
                        'name': 'Test Collector Job',
                        'enabled': False,
                        'script': 'python',
                        'args': ['-m', 'victoria_metrics_jobs.jobs.apex_collector', '--job-id', 'test_collector'],
                        'schedule': {
                            'type': 'cron',
                            'args': {'hour': 1, 'minute': 0}
                        },
                        # Job-specific attributes
                        'job_type': 'apex_collector',
                        'target_url': 'http://target.com',
                        'target_token': 'target_token',
                        'source_url': 'http://source.com',
                        'source_token': 'source_token'
                    }
                }
            },
            'prod': {
                'max_workers': 10,
                'database': {
                    'host': 'prod-db.example.com',
                    'port': 5432,
                    'name': 'scheduler',
                    'user': 'scheduler',
                    'password': 'prodpass'
                },
                'jobs': {
                    'test_extractor': {
                        'id': 'test_extractor',
                        'name': 'Test Extractor Job (Production)',
                        'enabled': True,
                        'script': 'python',
                        'args': ['-m', 'victoria_metrics_jobs.jobs.extractor', '--job-id', 'test_extractor'],
                        'schedule': {
                            'type': 'cron',
                            'args': {'hour': 2, 'minute': 0}
                        },
                        'job_type': 'extractor',
                        'source_url': 'http://prod.com',
                        'source_token': 'prod_token',
                        'target_url': 'http://prod-target.com',
                        'start_date_offset_days': 7,
                        'chunk_size_days': 30
                    }
                }
            }
        }
    }


@pytest.fixture
def consolidated_config_file(consolidated_config):
    """Create a temporary consolidated configuration file using OmegaConf."""
    # Set environment variables for the test
    os.environ['TARGET_URL'] = 'http://target.com'
    os.environ['TARGET_TOKEN'] = 'test_token'
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
        # Use OmegaConf to save (preserves interpolation syntax)
        OmegaConf.save(consolidated_config, f.name)
        temp_file = f.name
    
    yield temp_file
    
    # Cleanup
    os.unlink(temp_file)


@pytest.mark.unit
class TestSchedulerConfigLoader:
    """Tests for scheduler configuration loading with consolidated structure."""
    
    def test_dict_to_list_conversion(self, consolidated_config):
        """Test that dict-based jobs are converted to list format for scheduler."""
        loader = ConfigLoader()
        config = consolidated_config['environments']['dev'].copy()
        
        # Add environment info
        config['environment'] = 'dev'
        
        # Validate (this triggers conversion)
        loader._validate_config(config)
        
        # Check that jobs is now a list
        assert isinstance(config['jobs'], list)
        assert len(config['jobs']) == 2
        
        # Check that only scheduler attributes are present
        job = config['jobs'][0]
        assert 'id' in job
        assert 'name' in job
        assert 'enabled' in job
        assert 'script' in job
        assert 'args' in job
        assert 'schedule' in job
        
        # Check that job-specific attributes are NOT present
        assert 'job_type' not in job
        assert 'source_url' not in job
        assert 'target_url' not in job
    
    def test_scheduler_only_sees_scheduling_info(self, consolidated_config_file):
        """Test that scheduler loader filters out job-specific attributes."""
        os.environ['VM_JOBS_ENVIRONMENT'] = 'dev'
        loader = ConfigLoader()
        
        # Load full config first
        full_config = loader.load(consolidated_config_file)
        
        # Extract environment config and filter for scheduler
        env_config = full_config['environments']['dev'].copy()
        
        # Filter jobs to include only scheduler-relevant attributes
        if 'jobs' in env_config and isinstance(env_config['jobs'], dict):
            scheduler_attributes = {'id', 'name', 'enabled', 'script', 'args', 'schedule'}
            filtered_jobs = []
            
            for job_id, job_config in env_config['jobs'].items():
                filtered_job = {attr: job_config.get(attr) for attr in scheduler_attributes if attr in job_config}
                filtered_jobs.append(filtered_job)
            
            env_config['jobs'] = filtered_jobs
        
        # Check that jobs are in list format
        assert isinstance(env_config['jobs'], list)
        assert len(env_config['jobs']) == 2
        
        # Find the extractor job (dict ordering not guaranteed)
        extractor_job = next((j for j in env_config['jobs'] if j['id'] == 'test_extractor'), None)
        assert extractor_job is not None
        assert extractor_job['name'] == 'Test Extractor Job'
        assert extractor_job['enabled'] is True
        
        # Verify job-specific attributes are NOT in scheduler config
        assert 'job_type' not in extractor_job
        assert 'source_url' not in extractor_job
        assert 'target_url' not in extractor_job
        assert 'start_date_offset_days' not in extractor_job
    
    def test_multiple_environments(self, consolidated_config_file):
        """Test loading different environments."""
        # Load dev environment
        os.environ['VM_JOBS_ENVIRONMENT'] = 'dev'
        loader_dev = ConfigLoader()
        full_config_dev = loader_dev.load(consolidated_config_file)
        dev_config = full_config_dev['environments']['dev']
        assert dev_config['max_workers'] == 5
        assert len(dev_config['jobs']) == 2
        
        # Load prod environment
        os.environ['VM_JOBS_ENVIRONMENT'] = 'prod'
        loader_prod = ConfigLoader()
        full_config_prod = loader_prod.load(consolidated_config_file)
        prod_config = full_config_prod['environments']['prod']
        assert prod_config['max_workers'] == 10
        assert len(prod_config['jobs']) == 1


@pytest.mark.unit
class TestJobConfigManagerLegacy:
    """Tests for job-specific configuration loading using JobConfigManager (legacy tests)."""
    
    def test_job_config_excludes_scheduler_attributes(self, consolidated_config_file):
        """Test that job config loader excludes scheduler attributes."""
        import logging
        os.environ['VM_JOBS_ENVIRONMENT'] = 'dev'
        
        logger = logging.getLogger('test_loader')
        manager = JobConfigManager('test_job', logger)
        manager.load_config(consolidated_config_file)
        
        # Get job-specific config
        job_config = manager.get_job_config('test_extractor')
        
        # Check that job-specific attributes are present
        assert job_config['job_type'] == 'extractor'
        assert job_config['source_url'] == 'http://test.com'
        assert job_config['target_url'] == 'http://target.com'
        assert job_config['start_date_offset_days'] == 0
        assert job_config['chunk_size_days'] == 7
        
        # Check that scheduler attributes are NOT present
        assert 'script' not in job_config
        assert 'args' not in job_config
        assert 'schedule' not in job_config
        
        # Check that metadata is added
        assert job_config['job_id'] == 'test_extractor'
        assert job_config['job_name'] == 'Test Extractor Job'
        assert job_config['environment'] == 'dev'
    
    def test_different_job_types(self, consolidated_config_file):
        """Test loading configuration for different job types."""
        import logging
        os.environ['VM_JOBS_ENVIRONMENT'] = 'dev'
        
        logger = logging.getLogger('test_loader')
        manager = JobConfigManager('test_job', logger)
        manager.load_config(consolidated_config_file)
        
        # Test extractor job
        extractor_config = manager.get_job_config('test_extractor')
        assert extractor_config['job_type'] == 'extractor'
        assert 'source_url' in extractor_config
        assert 'chunk_size_days' in extractor_config
        
        # Test collector job
        collector_config = manager.get_job_config('test_collector')
        assert collector_config['job_type'] == 'apex_collector'
        assert 'source_url' in collector_config
        assert 'target_token' in collector_config
        assert 'chunk_size_days' not in collector_config  # Collector doesn't have this
    
    def test_job_not_found(self, consolidated_config_file):
        """Test error handling for non-existent job."""
        import logging
        os.environ['VM_JOBS_ENVIRONMENT'] = 'dev'
        
        logger = logging.getLogger('test_loader')
        manager = JobConfigManager('test_job', logger)
        manager.load_config(consolidated_config_file)
        
        with pytest.raises(ValueError, match="Job 'nonexistent_job' not found"):
            manager.get_job_config('nonexistent_job')
    
    def test_environment_specific_config(self, consolidated_config_file):
        """Test that different environments have different configs."""
        import logging
        
        # Dev environment
        os.environ['VM_JOBS_ENVIRONMENT'] = 'dev'
        logger_dev = logging.getLogger('test_loader_dev')
        manager_dev = JobConfigManager('test_job', logger_dev)
        manager_dev.load_config(consolidated_config_file)
        dev_config = manager_dev.get_job_config('test_extractor')
        
        assert dev_config['source_url'] == 'http://test.com'
        assert dev_config['start_date_offset_days'] == 0
        assert dev_config['chunk_size_days'] == 7
        
        # Prod environment
        os.environ['VM_JOBS_ENVIRONMENT'] = 'prod'
        logger_prod = logging.getLogger('test_loader_prod')
        manager_prod = JobConfigManager('test_job', logger_prod)
        manager_prod.load_config(consolidated_config_file)
        prod_config = manager_prod.get_job_config('test_extractor')
        
        assert prod_config['source_url'] == 'http://prod.com'
        assert prod_config['start_date_offset_days'] == 7
        assert prod_config['chunk_size_days'] == 30


@pytest.mark.unit
class TestJobConfigManager:
    """Tests for JobConfigManager with consolidated structure."""
    
    def test_get_job_config(self, consolidated_config_file):
        """Test JobConfigManager.get_job_config with consolidated structure."""
        import logging
        os.environ['VM_JOBS_ENVIRONMENT'] = 'dev'
        
        logger = logging.getLogger('test_manager')
        manager = JobConfigManager('extractor', logger)
        manager.load_config(consolidated_config_file)
        
        # Get job config
        job_config = manager.get_job_config('test_extractor')
        
        # Check job-specific attributes
        assert job_config['job_type'] == 'extractor'
        assert job_config['source_url'] == 'http://test.com'
        assert job_config['target_url'] == 'http://target.com'
        
        # Check metadata
        assert job_config['job_id'] == 'test_extractor'
        assert job_config['job_name'] == 'Test Extractor Job'
    
    def test_list_jobs(self, consolidated_config_file):
        """Test listing all jobs."""
        import logging
        os.environ['VM_JOBS_ENVIRONMENT'] = 'dev'
        
        logger = logging.getLogger('test_manager')
        manager = JobConfigManager('extractor', logger)
        manager.load_config(consolidated_config_file)
        
        jobs = manager.list_jobs()
        assert len(jobs) == 2
        assert 'test_extractor' in jobs
        assert 'test_collector' in jobs
    
    def test_get_available_jobs_info(self, consolidated_config_file):
        """Test getting information about all jobs."""
        import logging
        os.environ['VM_JOBS_ENVIRONMENT'] = 'dev'
        
        logger = logging.getLogger('test_manager')
        manager = JobConfigManager('extractor', logger)
        manager.load_config(consolidated_config_file)
        
        jobs_info = manager.get_available_jobs_info()
        assert len(jobs_info) == 2
        assert jobs_info['test_extractor']['name'] == 'Test Extractor Job'
        assert jobs_info['test_collector']['name'] == 'Test Collector Job'


@pytest.mark.unit
class TestConsolidatedConfigIntegration:
    """Integration tests for the entire consolidated config flow."""
    
    def test_scheduler_and_job_read_same_file(self, consolidated_config_file):
        """Test that scheduler and job loaders can both read from same file."""
        import logging
        os.environ['VM_JOBS_ENVIRONMENT'] = 'dev'
        
        # Scheduler reads the file
        scheduler_loader = ConfigLoader()
        full_config = scheduler_loader.load(consolidated_config_file)
        scheduler_config = full_config['environments']['dev'].copy()
        
        # Filter for scheduler
        if 'jobs' in scheduler_config and isinstance(scheduler_config['jobs'], dict):
            scheduler_attributes = {'id', 'name', 'enabled', 'script', 'args', 'schedule'}
            filtered_jobs = []
            for job_id, job_config in scheduler_config['jobs'].items():
                filtered_job = {attr: job_config.get(attr) for attr in scheduler_attributes if attr in job_config}
                filtered_jobs.append(filtered_job)
            scheduler_config['jobs'] = filtered_jobs
        
        # Job reads the file
        logger = logging.getLogger('test_manager')
        job_manager = JobConfigManager('extractor', logger)
        job_manager.load_config(consolidated_config_file)
        job_config = job_manager.get_job_config('test_extractor')
        
        # Verify scheduler sees only scheduling info
        assert 'jobs' in scheduler_config
        assert isinstance(scheduler_config['jobs'], list)
        
        # Find the extractor job in scheduler config
        scheduler_job = next((j for j in scheduler_config['jobs'] if j['id'] == 'test_extractor'), None)
        assert scheduler_job is not None
        assert 'schedule' in scheduler_job
        assert 'source_url' not in scheduler_job
        
        # Verify job sees only job-specific info
        assert 'source_url' in job_config
        assert 'schedule' not in job_config
        
        # Verify both reference same job
        assert scheduler_job['id'] == job_config['job_id']
    
    def test_complete_job_workflow(self, consolidated_config_file):
        """Test complete workflow from config to job execution."""
        import logging
        os.environ['VM_JOBS_ENVIRONMENT'] = 'dev'
        
        # 1. Scheduler loads config and sees jobs to schedule
        scheduler_loader = ConfigLoader()
        full_config = scheduler_loader.load(consolidated_config_file)
        scheduler_config = full_config['environments']['dev'].copy()
        
        # Filter for scheduler
        if 'jobs' in scheduler_config and isinstance(scheduler_config['jobs'], dict):
            scheduler_attributes = {'id', 'name', 'enabled', 'script', 'args', 'schedule'}
            filtered_jobs = []
            for job_id, job_config in scheduler_config['jobs'].items():
                filtered_job = {attr: job_config.get(attr) for attr in scheduler_attributes if attr in job_config}
                filtered_jobs.append(filtered_job)
            scheduler_config['jobs'] = filtered_jobs
        
        # Find the extractor job specifically
        job_to_run = next((j for j in scheduler_config['jobs'] if j['id'] == 'test_extractor'), None)
        assert job_to_run is not None
        job_id = job_to_run['id']
        
        # 2. Job process starts and loads job-specific config
        logger = logging.getLogger('test_manager')
        job_manager = JobConfigManager('extractor', logger)
        job_manager.load_config(consolidated_config_file)
        job_config = job_manager.get_job_config(job_id)
        
        # 3. Verify job can access all needed configuration
        assert job_config['job_type'] == 'extractor'
        assert job_config['source_url'] == 'http://test.com'
        assert job_config['source_token'] == 'token123'
        assert job_config['target_url'] == 'http://target.com'
        assert job_config['start_date_offset_days'] == 0
        assert job_config['chunk_size_days'] == 7
        
        # 4. Verify scheduler attributes are not leaked to job
        assert 'schedule' not in job_config
        assert 'args' not in job_config
        assert 'script' not in job_config


@pytest.mark.unit
class TestBackwardCompatibility:
    """Tests for backward compatibility with list-based job format."""
    
    def test_list_format_still_works(self):
        """Test that old list-based format still works."""
        old_format_config = {
            'environments': {
                'dev': {
                    'max_workers': 5,
                    'jobs': [
                        {
                            'id': 'old_job',
                            'name': 'Old Format Job',
                            'enabled': True,
                            'script': 'python',
                            'args': ['-m', 'job'],
                            'schedule': {
                                'type': 'cron',
                                'args': {'hour': 1}
                            }
                        }
                    ]
                }
            }
        }
        
        loader = ConfigLoader()
        config = old_format_config['environments']['dev'].copy()
        config['environment'] = 'dev'
        
        # Should not raise an error
        loader._validate_config(config)
        
        # Jobs should remain as list
        assert isinstance(config['jobs'], list)

