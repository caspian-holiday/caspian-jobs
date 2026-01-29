#!/usr/bin/env python3
"""
Base classes for job execution with step-by-step workflows.
"""

from __future__ import annotations

import argparse
import json
import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional, Callable, TypeVar
from functools import reduce

from .result_utils import Result, Ok, Err
from .config import JobConfigManager
from .logging import setup_job_logging

T = TypeVar('T')


@dataclass
class BaseJobState:
    """Base state class for job execution.
    
    This is the base class for all job states. Specific job types should
    inherit from this and add their own fields.
    """
    job_id: str
    job_config: Dict[str, Any]
    started_at: datetime
    completed_at: Optional[datetime] = None
    status: str = 'running'
    message: str = ''
    error: Optional[str] = None
    db_manager: Optional[Any] = None
    vm_manager: Optional[Any] = None
    
    def to_results(self) -> Dict[str, Any]:
        """Convert state to job results dictionary.
        
        Returns:
            Dictionary containing job execution results
        """
        return {
            'job_id': self.job_id,
            'job_display_name': self.job_config.get('job_name', self.job_id),
            'job_description': self.job_config.get('job_description', ''),
            'status': self.status,
            'message': self.message,
            'started_at': self.started_at.isoformat(),
            'completed_at': self.completed_at.isoformat() if self.completed_at else None,
            'job_config': self._sanitize_config_for_output(self.job_config)
        }
    
    def _sanitize_config_for_output(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Sanitize configuration by masking sensitive fields.
        
        Args:
            config: Configuration dictionary to sanitize
            
        Returns:
            Sanitized configuration with sensitive fields masked
        """
        # Define sensitive field patterns that should be masked
        sensitive_patterns = [
            'password', 'passwd', 'pwd',
            'token', 'key', 'secret',
            'auth', 'credential', 'cred',
            'api_key', 'apikey', 'access_key',
            'private_key', 'privatekey',
            'session', 'sessionid',
            'bearer', 'authorization',
            'jwt', 'oauth', 'refresh_token',
            'client_secret', 'client_id',
            'encryption_key', 'decryption_key',
            'salt', 'hash', 'checksum'
        ]
        
        # Allow subclasses to add additional sensitive patterns
        additional_patterns = self._get_additional_sensitive_patterns()
        sensitive_patterns.extend(additional_patterns)
        
        def is_sensitive_key(key: str) -> bool:
            """Check if a key contains sensitive information."""
            key_lower = key.lower()
            return any(pattern in key_lower for pattern in sensitive_patterns)
        
        def sanitize_value(value: Any, key: str = '') -> Any:
            """Recursively sanitize values in nested dictionaries."""
            if isinstance(value, dict):
                return {k: sanitize_value(v, k) for k, v in value.items()}
            elif isinstance(value, list):
                return [sanitize_value(item, key) for item in value]
            else:
                # Check if this value should be masked based on its key
                if is_sensitive_key(key):
                    if isinstance(value, str) and len(value) > 0:
                        # Show first 2 and last 2 characters for strings
                        if len(value) <= 4:
                            return '*' * len(value)
                        else:
                            return value[:2] + '*' * (len(value) - 4) + value[-2:]
                    else:
                        # For non-strings, just show the type
                        return f'<{type(value).__name__}>'
                else:
                    return value
        
        sanitized = {}
        for key, value in config.items():
            sanitized[key] = sanitize_value(value, key)
        
        return sanitized
    
    def _get_additional_sensitive_patterns(self) -> List[str]:
        """Get additional sensitive field patterns specific to this job type.
        
        Subclasses can override this method to add job-specific sensitive patterns.
        
        Returns:
            List of additional sensitive field patterns to mask
        """
        return []


class BaseJob(ABC):
    """Base class for job execution with step-by-step workflows.
    
    This class provides a functional approach to job execution where jobs
    are composed of a series of steps that transform a state object.
    Each step returns a Result[State, Exception] for proper error handling.
    """
    
    def __init__(self, job_name: str, config_path: Optional[str] = None, verbose: bool = False):
        """Initialize the job.
        
        Args:
            job_name: Name of the job (used for logging and identification)
            config_path: Path to configuration file
            verbose: Whether to enable verbose logging
        """
        self.job_name = job_name
        self.config_path = config_path  # Path to YAML config (e.g. for passing to notebooks when run as service)
        self.logger = setup_job_logging(job_name, verbose)
        self.config_manager = JobConfigManager(job_name, self.logger)
        self._db_manager = None  # Lazy initialization
        self._vm_manager = None  # Lazy initialization
        
        if config_path and Path(config_path).exists():
            self.config_manager.load_config(config_path)
    
    def load_config(self, config_path: str):
        """Load configuration from YAML file.
        
        Args:
            config_path: Path to YAML configuration file
        """
        self.config_path = config_path
        self.config_manager.load_config(config_path)
    
    def get_job_config(self, job_id: str) -> Dict[str, Any]:
        """Get configuration for a specific job ID.
        
        Args:
            job_id: Job ID
            
        Returns:
            Job-specific configuration with metadata
        """
        return self.config_manager.get_job_config(job_id)
    
    def list_jobs(self) -> list:
        """List available job configurations.
        
        Returns:
            List of job IDs
        """
        return self.config_manager.list_jobs()
    
    def get_available_jobs_info(self) -> Dict[str, Dict[str, str]]:
        """Get information about all available jobs.
        
        Returns:
            Dictionary mapping job_id to job info (name, description)
        """
        return self.config_manager.get_available_jobs_info()
    
    def _ensure_managers_initialized(self, job_config: Dict[str, Any]) -> None:
        """Ensure database and Victoria Metrics managers are initialized.
        
        This method initializes both managers if they don't already exist,
        based on the job configuration.
        
        Args:
            job_config: Job configuration dictionary
        """
        # Initialize database manager if not already initialized
        if self._db_manager is None:
            self.init_database_manager(job_config)
        
        # Initialize Victoria Metrics manager if not already initialized
        if self._vm_manager is None:
            self.init_victoria_metrics_manager(job_config)
    
    def init_database_manager(self, job_config: Dict[str, Any]) -> bool:
        """Initialize database manager from job configuration.
        
        Args:
            job_config: Job configuration dictionary (should contain 'database' key)
            
        Returns:
            True if database manager was successfully initialized, False otherwise
        """
        if self._db_manager is not None:
            self.logger.debug("Database manager already initialized")
            return True
        
        if 'database' not in job_config:
            self.logger.debug("No database configuration found in job config")
            return False
        
        try:
            from victoria_metrics_jobs.scheduler.database import DatabaseManager
            self._db_manager = DatabaseManager(job_config['database'])
            self._db_manager.connect()
            self.logger.info("Database manager initialized for job")
            return True
        except Exception as e:
            self.logger.warning(f"Failed to initialize database manager: {e}")
            return False
    
    def get_database_manager(self):
        """Get the database manager instance.
        
        Returns:
            DatabaseManager instance or None if not initialized
        """
        return self._db_manager
    
    def close_database(self):
        """Close database connection and cleanup.
        
        Should be called in finally block to ensure proper cleanup.
        """
        if self._db_manager:
            try:
                self._db_manager.disconnect()
                self.logger.info("Database manager disconnected")
                self._db_manager = None
            except Exception as e:
                self.logger.warning(f"Error disconnecting database: {e}")
    
    def init_victoria_metrics_manager(self, job_config: Dict[str, Any]) -> bool:
        """Initialize Victoria Metrics manager from job configuration.
        
        Args:
            job_config: Job configuration dictionary (should contain 'victoria_metrics' key)
            
        Returns:
            True if Victoria Metrics manager was successfully initialized, False otherwise
        """
        if self._vm_manager is not None:
            self.logger.debug("Victoria Metrics manager already initialized")
            return True
        
        if 'victoria_metrics' not in job_config:
            self.logger.debug("No Victoria Metrics configuration found in job config")
            return False
        
        try:
            from victoria_metrics_jobs.scheduler.victoria_metrics import VictoriaMetricsManager
            self._vm_manager = VictoriaMetricsManager(job_config['victoria_metrics'])
            self.logger.info("Victoria Metrics manager initialized for job")
            return True
        except Exception as e:
            self.logger.warning(f"Failed to initialize Victoria Metrics manager: {e}")
            return False
    
    def get_victoria_metrics_manager(self):
        """Get the Victoria Metrics manager instance.
        
        Returns:
            VictoriaMetricsManager instance or None if not initialized
        """
        return self._vm_manager
    
    def close_victoria_metrics(self):
        """Close Victoria Metrics manager.
        
        Should be called in finally block to ensure proper cleanup.
        """
        if self._vm_manager:
            try:
                self.logger.info("Victoria Metrics manager disconnected")
                self._vm_manager = None
            except Exception as e:
                self.logger.warning(f"Error disconnecting Victoria Metrics: {e}")
    
    @abstractmethod
    def create_initial_state(self, job_id: str) -> Result[BaseJobState, Exception]:
        """Create the initial state for job execution.
        
        Args:
            job_id: Job ID to use for configuration selection
            
        Returns:
            Result containing the initial state or an error
        """
        pass
    
    @abstractmethod
    def get_workflow_steps(self) -> List[Callable[[BaseJobState], Result[BaseJobState, Exception]]]:
        """Get the list of workflow steps for this job.
        
        Returns:
            List of step functions that take a state and return Result[State, Exception]
        """
        pass
    
    @abstractmethod
    def finalize_state(self, state: BaseJobState) -> BaseJobState:
        """Finalize the state before converting to results.
        
        Args:
            state: The final state after all steps
            
        Returns:
            Finalized state ready for conversion to results
        """
        pass
    
    def execute_workflow(self, job_id: str) -> Result[BaseJobState, Exception]:
        """Execute the complete workflow using functional composition.
        
        Args:
            job_id: Job ID to execute
            
        Returns:
            Result containing the final state or an error
        """
        try:
            # Get job configuration
            job_config = self.get_job_config(job_id)
            
            # Initialize managers if they don't exist
            self._ensure_managers_initialized(job_config)
            
            # Create initial state
            initial_state_result = self.create_initial_state(job_id)
            if initial_state_result.is_err:
                return initial_state_result
            
            # Set managers in the state
            state = initial_state_result.unwrap()
            state.db_manager = self._db_manager
            state.vm_manager = self._vm_manager
            
            # Get workflow steps
            steps = self.get_workflow_steps()
            
            # Execute workflow using functional composition
            workflow_result = reduce(
                lambda result, step: result.and_then(step),
                steps,
                Ok(state)
            )
            
            if workflow_result.is_err:
                return workflow_result
            
            # Finalize state
            final_state = self.finalize_state(workflow_result.unwrap())
            return Ok(final_state)
            
        except Exception as e:
            return Err(e)
    
    def run_job(self, job_id: str) -> Dict[str, Any]:
        """Run the job using functional approach.
        
        Args:
            job_id: Job ID to execute
            
        Returns:
            Job execution results
        """
        start_time = datetime.now()
        
        try:
            self.logger.info(f"Starting {self.job_name} job for job ID: {job_id}")
            
            # Execute the functional workflow
            workflow_result = self.execute_workflow(job_id)
            
            if workflow_result.is_err:
                # Handle workflow error
                error = workflow_result.error
                self.logger.error(f"Job workflow failed: {error}")
                return {
                    'job_id': job_id,
                    'job_display_name': job_id,
                    'job_description': '',
                    'started_at': start_time.isoformat(),
                    'completed_at': datetime.now().isoformat(),
                    'status': 'error',
                    'message': f'Job workflow failed for job: {job_id}',
                    'error': str(error)
                }
            
            # Convert successful state to results
            final_state = workflow_result.unwrap()
            results = final_state.to_results()
            
            # Add execution metadata
            results.update({
                'execution_time_seconds': (datetime.now() - start_time).total_seconds()
            })
            
            self.logger.info(f"Job completed successfully: {results}")
            return results
            
        except Exception as e:
            self.logger.error(f"Job failed: {e}")
            return {
                'job_id': job_id,
                'job_display_name': job_id,
                'job_description': '',
                'started_at': start_time.isoformat(),
                'completed_at': datetime.now().isoformat(),
                'status': 'error',
                'message': f'Job failed for job: {job_id}',
                'error': str(e)
            }
        finally:
            # Clean up database connection
            self.close_database()
            # Clean up Victoria Metrics connection
            self.close_victoria_metrics()
    
    @classmethod
    def create_argument_parser(cls, description: str, epilog: Optional[str] = None) -> argparse.ArgumentParser:
        """Create a standardized argument parser for job scripts.
        
        Args:
            description: Description of the job
            epilog: Optional epilog text with examples
            
        Returns:
            Configured ArgumentParser instance
        """
        parser = argparse.ArgumentParser(
            description=description,
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog=epilog
        )
        
        parser.add_argument(
            '--config',
            required=True,
            help='Configuration file path'
        )
        
        parser.add_argument(
            '--job-id',
            help='Job ID to use for configuration selection'
        )
        
        parser.add_argument(
            '--list-jobs',
            action='store_true',
            help='List available job configurations'
        )
        
        parser.add_argument(
            '--verbose', '-v',
            action='store_true',
            help='Enable verbose logging'
        )
        
        return parser
    
    @classmethod
    def main(cls, description: str, epilog: Optional[str] = None, default_job_id: Optional[str] = None):
        """Main function for command-line execution.
        
        This method provides a complete CLI interface for any job that inherits from BaseJob.
        
        Args:
            description: Description of the job
            epilog: Optional epilog text with examples
            default_job_id: Default job ID if none specified
        """
        parser = cls.create_argument_parser(description, epilog)
        args = parser.parse_args()
        
        try:
            # Create job instance
            job = cls(config_path=args.config, verbose=args.verbose)
            
            # Handle list jobs command
            if args.list_jobs:
                jobs_info = job.get_available_jobs_info()
                if not jobs_info:
                    print("No job configurations found.")
                    return 1
                
                print("Available job configurations:")
                for job_id, job_info in jobs_info.items():
                    print(f"  {job_id}: {job_info['name']}")
                    print(f"    Description: {job_info['description']}")
                
                return 0
            
            # Determine job ID
            job_id = args.job_id or default_job_id
            if not job_id:
                print("Error: --job-id parameter is required")
                print("Use --list-jobs to see available job configurations")
                return 1
            
            # Run the job
            results = job.run_job(job_id=job_id)
            
            # Output results as JSON
            print(json.dumps(results, indent=2))
            
            # Return appropriate exit code
            return 0 if results.get('status') == 'success' else 1
            
        except Exception as e:
            print(f"Error: {e}")
            return 1
