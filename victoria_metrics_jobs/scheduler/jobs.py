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
from typing import Dict, Any, Optional, Tuple
from datetime import date, datetime, timedelta

from .database import DatabaseManager


class JobExecutor:
    """Executes Python script jobs with advisory locking."""
    
    def __init__(
        self,
        database_manager: Optional[DatabaseManager],
        config_path: str
    ):
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
            # Extract and write metrics to Victoria Metrics
            try:
                # Parse job results from JSON output
                job_results = self._parse_job_results(stdout_output, stderr_output)
                
                # Calculate runtime in milliseconds
                run_time_ms = int((end_timestamp - start_timestamp) * 1000)
                
                # Convert timestamps to milliseconds
                start_time_ms = int(start_timestamp * 1000)
                end_time_ms = int(end_timestamp * 1000)
                
                # Extract job-specific metrics
                processed_metrics, failed_metrics = self._extract_job_metrics(job_results, job_type, job_id)
                
                # Write metrics to Victoria Metrics
                self._write_metrics_to_vm(
                    job_config=job_config,
                    job_id=job_id,
                    start_time_ms=start_time_ms,
                    end_time_ms=end_time_ms,
                    run_time_ms=run_time_ms,
                    processed_metrics=processed_metrics,
                    failed_metrics=failed_metrics
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
    ) -> Tuple[Optional[int], Optional[int]]:
        """Extract job-specific processed and failed metrics from job results.
        
        Based on actual JSON structure returned by each job type.
        
        Args:
            job_results: Parsed JSON results from job execution
            job_type: Type of job (apex_collector, extractor, metrics_forecast, metrics_extract, etc.)
            job_id: Job identifier (for logging)
            
        Returns:
            Tuple of (processed_metrics, failed_metrics) - both can be None if not available
        """
        processed_metrics = None
        failed_metrics = None
        
        if not job_results:
            return None, None
        
        try:
            # Extract processed metrics based on job_type
            if job_type == 'apex_collector':
                # apex_collector returns:
                # - apex_data_collected: number of apex data items collected
                value = job_results.get('apex_data_collected')
                if value is not None:
                    processed_metrics = int(value) if isinstance(value, (int, float)) else None
                # failed_count: number of failed processing operations
                failed_value = job_results.get('failed_count')
                if failed_value is not None:
                    failed_metrics = int(failed_value) if isinstance(failed_value, (int, float)) else None
            
            elif job_type == 'extractor':
                # extractor returns:
                # - metrics_saved_count: metrics saved to database
                value = job_results.get('metrics_saved_count')
                if value is not None:
                    processed_metrics = int(value) if isinstance(value, (int, float)) else None
            
            elif job_type == 'metrics_forecast':
                # metrics_forecast returns:
                # - series_processed: number of series forecasted
                value = job_results.get('series_processed')
                if value is not None:
                    processed_metrics = int(value) if isinstance(value, (int, float)) else None
                # failed_series: number of failed series
                failed_value = job_results.get('failed_series')
                if failed_value is not None:
                    failed_metrics = int(failed_value) if isinstance(failed_value, (int, float)) else None
            
            elif job_type == 'metrics_extract':
                # metrics_extract returns:
                # - metrics_saved_count: metrics saved to database
                value = job_results.get('metrics_saved_count')
                if value is not None:
                    processed_metrics = int(value) if isinstance(value, (int, float)) else None
                # failed_series: number of failed series
                failed_value = job_results.get('failed_series')
                if failed_value is not None:
                    failed_metrics = int(failed_value) if isinstance(failed_value, (int, float)) else None
            
            elif job_type == 'business_date_converter':
                # business_date_converter returns:
                # - metrics_converted: number of metrics converted
                value = job_results.get('metrics_converted')
                if value is not None:
                    processed_metrics = int(value) if isinstance(value, (int, float)) else None
                # failed_count: number of failed conversions
                failed_value = job_results.get('failed_count')
                if failed_value is not None:
                    failed_metrics = int(failed_value) if isinstance(failed_value, (int, float)) else None
            
            else:
                # Unknown job type, try common field names in order of preference
                for field in ['number_of_processed_metrics', 'metrics_saved_count', 'series_processed', 'processed_count', 'apex_data_collected', 'processed_entries', 'metrics_converted']:
                    if field in job_results:
                        value = job_results.get(field)
                        if value is not None:
                            processed_metrics = int(value) if isinstance(value, (int, float)) else None
                            break
                
                # Try to find failed metrics
                for field in ['number_of_failed_metrics', 'failed_count', 'failed_series']:
                    if field in job_results:
                        failed_value = job_results.get(field)
                        if failed_value is not None:
                            failed_metrics = int(failed_value) if isinstance(failed_value, (int, float)) else None
                            break
        
        except (AttributeError, TypeError, ValueError) as e:
            self.logger.warning(f"Error extracting metrics for job {job_id}: {e}")
        
        return processed_metrics, failed_metrics
    
    def _write_metrics_to_vm(
        self,
        job_config: Dict[str, Any],
        job_id: str,
        start_time_ms: int,
        end_time_ms: int,
        run_time_ms: int,
        processed_metrics: Optional[int],
        failed_metrics: Optional[int]
    ):
        """Write job execution metrics to Victoria Metrics.
        
        Args:
            job_config: Job configuration dictionary
            job_id: Job identifier (used as vmj_job label)
            start_time_ms: Job start timestamp in milliseconds
            end_time_ms: Job end timestamp in milliseconds
            run_time_ms: Job execution duration in milliseconds
            processed_metrics: Number of processed metrics (optional)
            failed_metrics: Number of failed metrics (optional)
        """
        try:
            # Get Victoria Metrics configuration from job config or environment config
            vm_config = job_config.get('victoria_metrics', {})
            
            # If not in job config, try to get from environment config
            if not vm_config or not vm_config.get('gateway_url'):
                # Try to load environment config
                try:
                    from .config import ConfigLoader
                    config_loader = ConfigLoader()
                    env_config = config_loader.load(self.config_path)
                    vm_config = env_config.get('victoria_metrics', {})
                except Exception as e:
                    self.logger.debug(f"Could not load environment config for VM: {e}")
            
            gateway_url = vm_config.get('gateway_url', '')
            if not gateway_url:
                self.logger.debug(f"Victoria Metrics gateway URL not configured for job {job_id}, skipping metric write")
                return
            
            # Build Prometheus text format metrics
            metrics_lines = []
            
            # vmj_run_time
            metrics_lines.append(f'vmj_run_time{{job="vmj",vmj_job="{job_id}"}} {run_time_ms}')
            
            # vmj_start_time
            metrics_lines.append(f'vmj_start_time{{job="vmj",vmj_job="{job_id}"}} {start_time_ms}')
            
            # vmj_end_time
            metrics_lines.append(f'vmj_end_time{{job="vmj",vmj_job="{job_id}"}} {end_time_ms}')
            
            # vmj_number_of_processed_metrics (if available)
            if processed_metrics is not None:
                metrics_lines.append(f'vmj_number_of_processed_metrics{{job="vmj",vmj_job="{job_id}"}} {processed_metrics}')
            
            # vmj_number_of_failed_metrics (if available)
            if failed_metrics is not None:
                metrics_lines.append(f'vmj_number_of_failed_metrics{{job="vmj",vmj_job="{job_id}"}} {failed_metrics}')
            
            if not metrics_lines:
                return
            
            # Join metrics with newlines
            metrics_payload = '\n'.join(metrics_lines) + '\n'
            
            # Prepare HTTP request
            import requests
            
            # Ensure gateway_url doesn't have /api/v1/import/prometheus if it's already in the URL
            if gateway_url.endswith('/api/v1/import/prometheus'):
                import_url = gateway_url
            elif gateway_url.endswith('/api/v1/write'):
                # Replace /write with /import/prometheus
                import_url = gateway_url.replace('/api/v1/write', '/api/v1/import/prometheus')
            else:
                # Append the endpoint
                import_url = f"{gateway_url.rstrip('/')}/api/v1/import/prometheus"
            
            headers = {'Content-Type': 'text/plain'}
            vm_token = vm_config.get('token', '')
            if vm_token:
                headers['Authorization'] = f'Bearer {vm_token}'
            
            # Write metrics to Victoria Metrics
            timeout = vm_config.get('timeout', 30)
            response = requests.post(
                import_url,
                data=metrics_payload,
                headers=headers,
                timeout=timeout
            )
            response.raise_for_status()
            
            self.logger.debug(f"Successfully wrote metrics to Victoria Metrics for job {job_id}")
            
        except ImportError:
            self.logger.warning(f"requests library not available, cannot write metrics to Victoria Metrics for job {job_id}")
        except Exception as e:
            self.logger.warning(f"Failed to write metrics to Victoria Metrics for job {job_id}: {e}")