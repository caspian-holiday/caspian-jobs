#!/usr/bin/env python3
"""
Job execution handlers for Python script jobs.
"""

import logging
import subprocess
import sys
import os
import json
import time
import re
from typing import Dict, Any, Optional
from datetime import date, datetime, timedelta

from .database import DatabaseManager


class JobExecutor:
    """Executes Python script jobs with advisory locking."""
    
    def __init__(
        self,
        database_manager: Optional[DatabaseManager],
        config_path: str,
        metrics_manager: Optional[Any] = None
    ):
        """Initialize the job executor.
        
        Args:
            database_manager: Database manager for advisory locks (optional)
            config_path: Path to the configuration file (required)
            metrics_manager: ScrapeOnceMetricsManager instance for writing metrics (optional)
        """
        self.logger = logging.getLogger(__name__)
        self.database_manager = database_manager
        self.config_path = config_path
        self.metrics_manager = metrics_manager
    
    def execute_job(self, job_config: Dict[str, Any]):
        """Execute a job script based on its configuration with advisory locking.
        
        Args:
            job_config: Job configuration dictionary
        """
        job_id = job_config.get('id', 'unknown')
        script = job_config.get('script')
        job_type = job_config.get('job_type', '')
        
        start_time = time.time()
        start_timestamp = start_time
        end_timestamp = start_time
        status = 'failure'
        stdout_output = ''
        stderr_output = ''
        
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
                    stdout_output, stderr_output = self._execute_python_job(job_config)
            else:
                # Execute without locking if no database manager
                self.logger.warning(f"No database manager available, executing job {job_id} without locking")
                stdout_output, stderr_output = self._execute_python_job(job_config)
            
            end_timestamp = time.time()
            status = 'success'
            self.logger.info(f"Job {job_id} completed successfully")
            
        except Exception as e:
            end_timestamp = time.time()
            status = 'failure'
            self.logger.error(f"Job {job_id} failed: {e}")
            raise
        finally:
            # Extract and write metrics
            if self.metrics_manager:
                try:
                    business_date = self._get_current_business_date()
                    
                    # Parse job results from JSON output
                    job_results = self._parse_job_results(stdout_output, stderr_output)
                    
                    # Extract timestamps and runtime from JSON (if available)
                    # Jobs return started_at and completed_at as ISO strings
                    job_start_timestamp = start_timestamp  # Fallback to our tracked time
                    job_end_timestamp = end_timestamp  # Fallback to our tracked time
                    runtime_seconds = end_timestamp - start_timestamp  # Fallback
                    
                    if job_results:
                        # Try to get timestamps from JSON (ISO format strings)
                        if 'started_at' in job_results and job_results['started_at']:
                            try:
                                job_start_timestamp = datetime.fromisoformat(
                                    job_results['started_at'].replace('Z', '+00:00')
                                ).timestamp()
                            except (ValueError, AttributeError):
                                pass
                        
                        if 'completed_at' in job_results and job_results['completed_at']:
                            try:
                                job_end_timestamp = datetime.fromisoformat(
                                    job_results['completed_at'].replace('Z', '+00:00')
                                ).timestamp()
                            except (ValueError, AttributeError):
                                pass
                        
                        # Use execution_time_seconds from JSON if available
                        if 'execution_time_seconds' in job_results:
                            runtime_seconds = job_results['execution_time_seconds']
                        
                        # Get status from JSON if available (more accurate)
                        if 'status' in job_results:
                            status = job_results['status']
                    
                    # Extract job-specific metrics
                    processed_entries = self._extract_job_metrics(job_results, job_type, job_id)
                    
                    # Write metrics to file
                    self.metrics_manager.append_job_metrics(
                        job_id=job_id,
                        business_date=business_date,
                        start_timestamp=job_start_timestamp,
                        end_timestamp=job_end_timestamp,
                        runtime_seconds=runtime_seconds,
                        status=status,
                        processed_entries=processed_entries
                    )
                except Exception as metrics_error:
                    self.logger.warning(f"Failed to write metrics for job {job_id}: {metrics_error}")
    
    def _execute_python_job(self, job_config: Dict[str, Any]):
        """Execute a Python script job.
        
        Args:
            job_config: Job configuration dictionary
            
        Returns:
            Tuple of (stdout_output, stderr_output) strings
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
        
        return self._execute_python_script(script, args)
    
    def _execute_python_script(self, script_path: str, args: list):
        """Execute a Python script or module.
        
        Args:
            script_path: Path to the Python script or module name
            args: Command line arguments to pass to the script
            
        Returns:
            Tuple of (stdout_output, stderr_output) strings
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
            
            return result.stdout, result.stderr
                
        except subprocess.TimeoutExpired as e:
            raise TimeoutError(f"Script timed out: {e}")
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Script failed with return code {e.returncode}: {e.stderr}")
    
    def _parse_job_results(self, stdout: str, stderr: str) -> Optional[Dict[str, Any]]:
        """Parse JSON output from job execution.
        
        Jobs output JSON results to stdout. This method extracts and parses it.
        
        Args:
            stdout: Standard output from job execution
            stderr: Standard error output (not used for JSON parsing)
            
        Returns:
            Parsed JSON dictionary or None if parsing fails
        """
        if not stdout or not stdout.strip():
            return None
        
        try:
            # Try to parse JSON from stdout
            # Jobs output JSON, so we should be able to parse it directly
            job_results = json.loads(stdout.strip())
            return job_results
        except json.JSONDecodeError:
            # Try to find JSON in the output (might have log messages before/after)
            try:
                # Look for JSON object in output
                json_match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', stdout, re.DOTALL)
                if json_match:
                    job_results = json.loads(json_match.group(0))
                    return job_results
            except (json.JSONDecodeError, AttributeError):
                pass
        
        self.logger.warning("Failed to parse JSON from job output")
        return None
    
    def _extract_job_metrics(
        self,
        job_results: Optional[Dict[str, Any]],
        job_type: str,
        job_id: str
    ) -> Optional[int]:
        """Extract job-specific processed entries metric from job results.
        
        Based on actual JSON structure returned by each job type.
        
        Args:
            job_results: Parsed JSON results from job execution
            job_type: Type of job (apex_collector, extractor, metrics_forecast, metrics_extract)
            job_id: Job identifier (for logging)
            
        Returns:
            Number of processed entries or None if not available
        """
        if not job_results:
            return None
        
        try:
            # Extract based on job_type and actual JSON fields
            if job_type == 'apex_collector':
                # apex_collector returns:
                # - apex_data_collected: number of apex data items collected
                # - processed_count: number of successful processing operations
                # We want: apex_data_collected (metrics extracted and saved to VM)
                value = job_results.get('apex_data_collected')
                if value is not None:
                    return int(value) if isinstance(value, (int, float)) else None
            
            elif job_type == 'extractor':
                # extractor returns:
                # - metrics_saved_count: metrics saved to database
                # We want: metrics_saved_count
                value = job_results.get('metrics_saved_count')
                if value is not None:
                    return int(value) if isinstance(value, (int, float)) else None
            
            elif job_type == 'metrics_forecast':
                # metrics_forecast returns:
                # - series_processed: number of series forecasted
                # - forecasts_written: number of forecast samples written
                # We want: series_processed (number of metric series that were forecasted)
                value = job_results.get('series_processed')
                if value is not None:
                    return int(value) if isinstance(value, (int, float)) else None
            
            elif job_type == 'metrics_extract':
                # metrics_extract returns:
                # - metrics_saved_count: metrics saved to database
                # - series_processed: number of series processed
                # We want: metrics_saved_count (number of metrics saved to database)
                value = job_results.get('metrics_saved_count')
                if value is not None:
                    return int(value) if isinstance(value, (int, float)) else None
            
            else:
                # Unknown job type, try common field names in order of preference
                for field in ['metrics_saved_count', 'series_processed', 'processed_count', 'apex_data_collected', 'processed_entries']:
                    if field in job_results:
                        value = job_results.get(field)
                        if value is not None:
                            return int(value) if isinstance(value, (int, float)) else None
        
        except (AttributeError, TypeError, ValueError) as e:
            self.logger.warning(f"Error extracting metrics for job {job_id}: {e}")
        
        return None
    
    def _get_current_business_date(self) -> str:
        """Get current business date (today, or last weekday if weekend).
        
        Returns:
            ISO format date string (YYYY-MM-DD)
        """
        today = date.today()
        # If weekend, use last Friday
        while today.weekday() >= 5:  # Saturday=5, Sunday=6
            today -= timedelta(days=1)
        return today.isoformat()