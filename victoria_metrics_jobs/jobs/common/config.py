"""
Common configuration loading utilities for job scripts.
"""

import logging
import os
from typing import Dict, Any, List, Optional

# Add the scheduler module to the path for imports
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from victoria_metrics_jobs.scheduler.common import ConfigLoader


class JobConfigManager:
    """Common configuration manager for job scripts."""
    
    def __init__(self, job_name: str, logger: logging.Logger):
        """Initialize the job configuration manager.
        
        Args:
            job_name: Name of the job (for logging)
            logger: Logger instance to use
        """
        self.job_name = job_name
        self.logger = logger
        self.config_loader = ConfigLoader()
        self.config: Dict[str, Any] = {}
    
    def load_config(self, config_path: str) -> None:
        """Load configuration from YAML file.
        
        Args:
            config_path: Path to YAML configuration file (scheduler.yml)
        """
        try:
            # Use the common config loader which handles environment variables
            self.config = self.config_loader.load(config_path)
            
            self.logger.info(f"Loaded configuration from {config_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to load configuration: {e}")
            raise
    
    def get_job_config(self, job_id: str) -> Dict[str, Any]:
        """Get configuration for a specific job ID from consolidated scheduler config.
        
        Args:
            job_id: Job ID
            
        Returns:
            Job-specific configuration with metadata
        """
        if not self.config:
            raise ValueError("Configuration not loaded")
        
        # Get environment - REQUIRED, no default
        environment = os.getenv('VM_JOBS_ENVIRONMENT')
        if environment is None:
            raise ValueError(
                "Environment must be specified. Set VM_JOBS_ENVIRONMENT environment variable to 'local', 'dev', 'stg', or 'prod'"
            )
        
        # Extract environment-specific configuration
        if 'environments' not in self.config:
            raise ValueError("Configuration does not contain 'environments' section")
        
        if environment not in self.config['environments']:
            raise ValueError(f"Environment '{environment}' not found in configuration. Available environments: {list(self.config['environments'].keys())}")
        
        env_config = self.config['environments'][environment]
        
        # Find the specific job configuration in jobs section (consolidated structure)
        if 'jobs' not in env_config:
            raise ValueError(f"No jobs section found in environment '{environment}'")
        
        jobs = env_config['jobs']
        
        # Support both dict and list formats
        if isinstance(jobs, dict):
            if job_id not in jobs:
                available_jobs = list(jobs.keys())
                raise ValueError(f"Job '{job_id}' not found in environment '{environment}'. Available jobs: {available_jobs}")
            
            job_config = jobs[job_id]
        else:
            # List format
            job_config = None
            for job in jobs:
                if job.get('id') == job_id:
                    job_config = job
                    break
            
            if job_config is None:
                available_jobs = [job.get('id') for job in jobs]
                raise ValueError(f"Job '{job_id}' not found in environment '{environment}'. Available jobs: {available_jobs}")
        
        # Define scheduler-only attributes (these are excluded from job-specific config)
        scheduler_attributes = {'id', 'name', 'enabled', 'script', 'args', 'schedule'}
        
        # Extract job-specific configuration (everything except scheduler attributes)
        result = {}
        for key, value in job_config.items():
            if key not in scheduler_attributes:
                result[key] = value
        
        # Add job metadata (generic for all job types)
        result['job_id'] = job_id
        result['job_name'] = job_config.get('name', job_id)
        result['job_description'] = job_config.get('description', '')
        result['environment'] = environment
        
        # Add Victoria Metrics configuration to job config
        if 'victoria_metrics' in env_config:
            result['victoria_metrics'] = env_config['victoria_metrics']
        
        # Add metrics configuration to job config (for cleanup job and other metrics-aware jobs)
        if 'metrics' in env_config:
            result['metrics'] = env_config['metrics']
        
        self.logger.info(f"Loaded configuration for job '{job_id}': {result['job_name']}")
        return result
    
    def list_jobs(self) -> List[str]:
        """List available job configurations for current environment.
        
        Returns:
            List of job IDs
        """
        if not self.config:
            return []
        
        # Get environment - REQUIRED, no default
        environment = os.getenv('VM_JOBS_ENVIRONMENT')
        if environment is None:
            raise ValueError(
                "Environment must be specified. Set VM_JOBS_ENVIRONMENT environment variable to 'local', 'dev', 'stg', or 'prod'"
            )
        
        # Extract environment-specific configuration
        if 'environments' not in self.config:
            return []
        
        if environment not in self.config['environments']:
            return []
        
        env_config = self.config['environments'][environment]
        
        # Get job IDs from the jobs section
        if 'jobs' not in env_config:
            return []
        
        jobs = env_config['jobs']
        
        # Support both dict and list formats
        if isinstance(jobs, dict):
            return list(jobs.keys())
        else:
            return [job.get('id') for job in jobs if job.get('id')]
    
    def get_available_jobs_info(self) -> Dict[str, Dict[str, str]]:
        """Get information about all available jobs for current environment.
        
        Returns:
            Dictionary mapping job_id to job info (name, description)
        """
        if not self.config:
            return {}
        
        # Get environment - REQUIRED, no default
        environment = os.getenv('VM_JOBS_ENVIRONMENT')
        if environment is None:
            raise ValueError(
                "Environment must be specified. Set VM_JOBS_ENVIRONMENT environment variable to 'local', 'dev', 'stg', or 'prod'"
            )
        
        # Extract environment-specific configuration
        if 'environments' not in self.config:
            return {}
        
        if environment not in self.config['environments']:
            return {}
        
        env_config = self.config['environments'][environment]
        
        # Get job information from jobs section
        if 'jobs' not in env_config:
            return {}
        
        jobs = env_config['jobs']
        job_info = {}
        
        # Support both dict and list formats
        if isinstance(jobs, dict):
            for job_id, job_config in jobs.items():
                job_info[job_id] = {
                    'name': job_config.get('name', job_id),
                    'description': job_config.get('description', 'No description')
                }
        else:
            for job in jobs:
                job_id = job.get('id')
                if job_id:
                    job_info[job_id] = {
                        'name': job.get('name', job_id),
                        'description': job.get('description', 'No description')
                    }
        
        return job_info


def create_job_config_manager(job_name: str, config_path: Optional[str] = None, 
                            logger: Optional[logging.Logger] = None) -> JobConfigManager:
    """Create and optionally initialize a job configuration manager.
    
    Args:
        job_name: Name of the job
        config_path: Optional path to configuration file to load
        logger: Optional logger instance (will create one if not provided)
        
    Returns:
        Initialized JobConfigManager instance
    """
    if logger is None:
        from .logging import setup_job_logging
        logger = setup_job_logging(job_name, verbose=False)
    
    manager = JobConfigManager(job_name, logger)
    
    if config_path and os.path.exists(config_path):
        manager.load_config(config_path)
    
    return manager
