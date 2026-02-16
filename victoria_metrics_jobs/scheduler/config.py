#!/usr/bin/env python3
"""
Configuration loader for YAML-based job definitions with environment support.
"""

import logging
import os
from typing import Dict, Any, List, Optional
from .common import ConfigLoader as BaseConfigLoader


class ConfigLoader(BaseConfigLoader):
    """Loads and validates scheduler configuration from YAML files with environment support."""
    
    def load(self, config_path: str, environment: Optional[str] = None) -> Dict[str, Any]:
        """Load configuration from YAML file with environment support.
        
        Args:
            config_path: Path to the YAML configuration file
            environment: Environment name (dev, stg, prod). If None, uses ENVIRONMENT env var
            
        Returns:
            Dictionary containing the environment-specific configuration
            
        Raises:
            FileNotFoundError: If the configuration file doesn't exist
            yaml.YAMLError: If the YAML file is malformed
            ValueError: If the configuration is invalid or environment is not specified
        """
        # Determine environment - REQUIRED, no default
        if environment is None:
            environment = os.getenv('VM_JOBS_ENVIRONMENT')
            if environment is None:
                raise ValueError(
                    "Environment must be specified. Set VM_JOBS_ENVIRONMENT environment variable to 'local', 'dev', 'stg', or 'prod'"
                )
        
        # Validate environment
        valid_environments = ['local', 'dev', 'stg', 'prod']
        if environment not in valid_environments:
            raise ValueError(f"Invalid environment '{environment}'. Must be one of: {valid_environments}")
        
        # Use the base class load method which handles environment variables
        full_config = super().load(config_path)
        
        # Extract environment-specific configuration
        if 'environments' not in full_config:
            raise ValueError("Configuration does not contain 'environments' section")
        
        if environment not in full_config['environments']:
            available_envs = list(full_config['environments'].keys())
            raise ValueError(f"Environment '{environment}' not found in configuration. Available environments: {available_envs}")
        
        # Get environment-specific configuration
        env_config = full_config['environments'][environment]
        
        # Add environment information to config
        env_config['environment'] = environment
        
        # Validate configuration
        self._validate_config(env_config)
        
        return env_config
    
    def load_environment_config(self, environment: str, base_path: str = "scheduler") -> Dict[str, Any]:
        """Load environment-specific configuration.
        
        Args:
            environment: Environment name (dev, stg, prod)
            base_path: Base path to scheduler directory
            
        Returns:
            Dictionary containing the environment-specific configuration
            
        Raises:
            FileNotFoundError: If the configuration file doesn't exist
            yaml.YAMLError: If the YAML file is malformed
            ValueError: If the configuration is invalid
        """
        config_path = os.path.join(base_path, "scheduler.yml")
        return self.load(config_path, environment)
    
    def _validate_config(self, config: Dict[str, Any]):
        """Validate the configuration structure.
        
        Args:
            config: Configuration dictionary to validate
            
        Raises:
            ValueError: If the configuration is invalid
        """
        if not isinstance(config, dict):
            raise ValueError("Configuration must be a dictionary")
        
        # Validate metrics section (optional)
        if 'metrics' in config:
            self._validate_metrics(config['metrics'])
        else:
            # Set default metrics configuration if not provided
            config['metrics'] = {
                'directory': '/var/lib/scheduler/metrics',
                'archive_directory': '/var/lib/scheduler/metrics_archive',
                'enable_archive': True,
                'port': 8000,
                'host': '0.0.0.0',
                'retention_days': 14
            }
        
        # Validate jobs section
        if 'jobs' in config:
            # Support both dict and list formats
            if isinstance(config['jobs'], dict):
                # Convert dict format to list format for scheduler compatibility
                jobs_list = []
                for job_id, job_config in config['jobs'].items():
                    # Extract only scheduler-relevant attributes
                    scheduler_job = {
                        'id': job_config.get('id', job_id),
                        'name': job_config.get('name', job_id),
                        'enabled': job_config.get('enabled', True),
                        'script': job_config.get('script'),
                        'args': job_config.get('args', []),
                        'schedule': job_config.get('schedule', {})
                    }
                    jobs_list.append(scheduler_job)
                config['jobs'] = jobs_list
            
            if not isinstance(config['jobs'], list):
                raise ValueError("Jobs must be a list or dict")
            
            for i, job in enumerate(config['jobs']):
                self._validate_job(job, i)
    
    def _validate_job(self, job: Dict[str, Any], index: int):
        """Validate a single job configuration.
        
        Args:
            job: Job configuration dictionary
            index: Job index in the jobs list
            
        Raises:
            ValueError: If the job configuration is invalid
        """
        if not isinstance(job, dict):
            raise ValueError(f"Job {index} must be a dictionary")
        
        # Required fields
        required_fields = ['id', 'name', 'enabled', 'script']
        for field in required_fields:
            if field not in job:
                raise ValueError(f"Job {index} missing required field: {field}")
        
        # Validate enabled field
        if not isinstance(job['enabled'], bool):
            raise ValueError(f"Job {index} 'enabled' field must be a boolean")
        
        # Validate script field
        if not isinstance(job['script'], str) or not job['script'].strip():
            raise ValueError(f"Job {index} 'script' field must be a non-empty string")
        
        # Validate schedule
        if 'schedule' not in job:
            raise ValueError(f"Job {index} missing required field: schedule")
        
        self._validate_schedule(job['schedule'], index)
    
    def _validate_schedule(self, schedule: Dict[str, Any], job_index: int):
        """Validate a job schedule configuration.
        
        Args:
            schedule: Schedule configuration dictionary
            job_index: Job index for error messages
            
        Raises:
            ValueError: If the schedule configuration is invalid
        """
        if not isinstance(schedule, dict):
            raise ValueError(f"Job {job_index} schedule must be a dictionary")
        
        if 'type' not in schedule:
            raise ValueError(f"Job {job_index} schedule missing required field: type")
        
        valid_schedule_types = ['cron', 'interval', 'date']
        if schedule['type'] not in valid_schedule_types:
            raise ValueError(f"Job {job_index} schedule has invalid type '{schedule['type']}'. Must be one of: {valid_schedule_types}")
        
        if 'args' not in schedule:
            raise ValueError(f"Job {job_index} schedule missing required field: args")
        
        if not isinstance(schedule['args'], dict):
            raise ValueError(f"Job {job_index} schedule args must be a dictionary")
    
    def _validate_metrics(self, metrics: Dict[str, Any]):
        """Validate metrics configuration section.
        
        Args:
            metrics: Metrics configuration dictionary
            
        Raises:
            ValueError: If the metrics configuration is invalid
        """
        if not isinstance(metrics, dict):
            raise ValueError("Metrics configuration must be a dictionary")
        
        # Validate directory
        if 'directory' in metrics:
            directory = metrics['directory']
            if not isinstance(directory, str) or not directory.strip():
                raise ValueError("Metrics directory must be a non-empty string")
        else:
            # Set default directory
            metrics['directory'] = '/var/lib/scheduler/metrics'
        
        # Validate archive_directory (optional)
        if 'archive_directory' in metrics:
            archive_dir = metrics['archive_directory']
            if archive_dir is not None and (not isinstance(archive_dir, str) or not archive_dir.strip()):
                raise ValueError("Metrics archive_directory must be a non-empty string or None")
        else:
            # Set default archive directory
            metrics['archive_directory'] = '/var/lib/scheduler/metrics_archive'
        
        # Validate enable_archive
        if 'enable_archive' in metrics:
            enable_archive = metrics['enable_archive']
            if not isinstance(enable_archive, bool):
                raise ValueError("Metrics enable_archive must be a boolean")
        else:
            # Set default
            metrics['enable_archive'] = True
        
        # Validate port
        if 'port' in metrics:
            port = metrics['port']
            if not isinstance(port, int) or port < 1 or port > 65535:
                raise ValueError("Metrics port must be an integer between 1 and 65535")
        else:
            # Set default port
            metrics['port'] = 8000
        
        # Validate host
        if 'host' in metrics:
            host = metrics['host']
            if not isinstance(host, str) or not host.strip():
                raise ValueError("Metrics host must be a non-empty string")
        else:
            # Set default host
            metrics['host'] = '0.0.0.0'
        
        # Validate retention_days (optional)
        if 'retention_days' in metrics:
            retention = metrics['retention_days']
            if not isinstance(retention, int) or retention < 1:
                raise ValueError("Metrics retention_days must be a positive integer")
        else:
            # Set default
            metrics['retention_days'] = 14
    