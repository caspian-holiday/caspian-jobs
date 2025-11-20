#!/usr/bin/env python3
"""
Job execution handlers for Python script jobs.
"""

import logging
import subprocess
import sys
import os
from typing import Dict, Any, Optional

from .database import DatabaseManager


class JobExecutor:
    """Executes Python script jobs with advisory locking."""
    
    def __init__(self, database_manager: Optional[DatabaseManager], config_path: str):
        """Initialize the job executor.
        
        Args:
            database_manager: Database manager for advisory locks (optional)
            config_path: Path to the configuration file (required)
        """
        self.logger = logging.getLogger(__name__)
        self.database_manager = database_manager
        self.config_path = config_path
    
    def execute_job(self, job_config: Dict[str, Any]):
        """Execute a job script based on its configuration with advisory locking.
        
        Args:
            job_config: Job configuration dictionary
        """
        job_id = job_config.get('id', 'unknown')
        script = job_config.get('script')
        
        self.logger.info(f"Executing job: {job_id} (script: {script})")
        
        try:
            if not script:
                raise ValueError("Job missing 'script' field")
            
            # Use advisory lock if database manager is available
            if self.database_manager:
                with self.database_manager.advisory_lock(job_id) as lock_acquired:
                    if not lock_acquired:
                        self.logger.warning(f"Job {job_id} is already running, skipping execution")
                        return
                    
                    # Execute the job within the lock
                    self._execute_python_job(job_config)
            else:
                # Execute without locking if no database manager
                self.logger.warning(f"No database manager available, executing job {job_id} without locking")
                self._execute_python_job(job_config)
            
            self.logger.info(f"Job {job_id} completed successfully")
            
        except Exception as e:
            self.logger.error(f"Job {job_id} failed: {e}")
            raise
    
    def _execute_python_job(self, job_config: Dict[str, Any]):
        """Execute a Python script job.
        
        Args:
            job_config: Job configuration dictionary
        """
        script = job_config.get('script')
        args = job_config.get('args', [])
        job_id = job_config.get('id')
        
        if not script:
            raise ValueError("Python job missing 'script' field")
        
        # Add config path to args if not already present
        if '--config' not in args:
            args = args + ['--config', self.config_path]
        
        # Add job_id to args if not already present
        if job_id and '--job-id' not in args:
            args = args + ['--job-id', job_id]
        
        self._execute_python_script(script, args)
    
    def _execute_python_script(self, script_path: str, args: list):
        """Execute a Python script or module.
        
        Args:
            script_path: Path to the Python script or module name
            args: Command line arguments to pass to the script
        """
        self.logger.debug(f"Executing Python script/module: {script_path}")
        
        try:
            # Prepare command
            if script_path == "python":
                # Module execution: args should start with "-m", "module_name"
                cmd = [sys.executable] + args
            else:
                # Script execution: check if file exists
                if not os.path.exists(script_path):
                    raise FileNotFoundError(f"Python script not found: {script_path}")
                cmd = [sys.executable, script_path] + args
            
            # Execute the script/module
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout
            )
            
            if result.stdout:
                self.logger.info(f"Script stdout: {result.stdout}")
            
            if result.stderr:
                self.logger.warning(f"Script stderr: {result.stderr}")
            
            if result.returncode != 0:
                raise subprocess.CalledProcessError(
                    result.returncode,
                    cmd,
                    result.stdout,
                    result.stderr
                )
                
        except subprocess.TimeoutExpired as e:
            raise TimeoutError(f"Script timed out: {e}")
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Script failed with return code {e.returncode}: {e.stderr}")