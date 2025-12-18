#!/usr/bin/env python3
"""
Metrics Cleanup Job - Removes old .prom files that weren't scraped.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Any, List, Optional

from ..common import BaseJob, BaseJobState, Err, Ok, Result
from ...scheduler.metrics_file_manager import ScrapeOnceMetricsManager


@dataclass
class MetricsCleanupState(BaseJobState):
    """State object for the metrics cleanup job."""
    
    metrics_dir: str = ""
    archive_dir: Optional[str] = None
    enable_archive: bool = True
    max_age_days: int = 14  # Default: 2 weeks (files can be scraped multiple times safely)
    files_removed: int = 0
    notebooks_output_dir: Optional[str] = None
    notebooks_retention_days: int = 14  # Retention days for notebooks (from referenced job)
    notebooks_archive_dir: Optional[str] = None
    notebook_dirs_removed: int = 0


class MetricsCleanupJob(BaseJob):
    """Job that cleans up old metrics .prom files."""
    
    def __init__(self, config_path: str = None, verbose: bool = False):
        """Initialize the metrics cleanup job.
        
        Args:
            config_path: Path to configuration file
            verbose: Whether to enable verbose logging
        """
        super().__init__('metrics_cleanup', config_path, verbose)
    
    def create_initial_state(self, job_id: str) -> Result[MetricsCleanupState, Exception]:
        """Create initial state for metrics cleanup job.
        
        Args:
            job_id: Job ID to use for configuration selection
            
        Returns:
            Result containing the initial state or an error
        """
        try:
            job_config = self.get_job_config(job_id)
            
            # Get metrics configuration from environment metrics config (added by JobConfigManager)
            metrics_config = job_config.get('metrics', {})
            
            # Get metrics directory and retention settings
            metrics_dir = metrics_config.get('directory', '/var/lib/scheduler/metrics')
            archive_dir = metrics_config.get('archive_directory')
            enable_archive = metrics_config.get('enable_archive', True)
            retention_days = metrics_config.get('retention_days', 14)  # Default: 2 weeks
            
            # Get notebooks cleanup settings from referenced job (optional)
            notebooks_output_dir = None
            notebooks_retention_days = retention_days
            notebooks_job_id = job_config.get('notebooks_job_id')
            
            if notebooks_job_id:
                try:
                    # Get notebooks configuration from the referenced job
                    notebooks_job_config = self.get_job_config(notebooks_job_id)
                    notebooks_output_dir = notebooks_job_config.get('notebooks_output_directory')
                    notebooks_retention_days = notebooks_job_config.get('notebooks_retention_days', retention_days)
                except Exception as e:
                    self.logger.warning(f"Could not load notebooks job config for '{notebooks_job_id}': {e}")
            
            notebooks_archive_dir = None  # Use same archive logic as metrics if enabled
            
            # Job config can override (for backward compatibility)
            if 'metrics_directory' in job_config:
                metrics_dir = job_config['metrics_directory']
            if 'archive_directory' in job_config:
                archive_dir = job_config['archive_directory']
            if 'enable_archive' in job_config:
                enable_archive = job_config['enable_archive']
            if 'max_age_days' in job_config:
                retention_days = job_config['max_age_days']
            
            state = MetricsCleanupState(
                job_id=job_id,
                job_config=job_config,
                started_at=datetime.now(),
                metrics_dir=metrics_dir,
                archive_dir=archive_dir,
                enable_archive=enable_archive,
                max_age_days=retention_days,
                files_removed=0,
                notebooks_output_dir=notebooks_output_dir,
                notebooks_retention_days=notebooks_retention_days,
                notebooks_archive_dir=notebooks_archive_dir,
                notebook_dirs_removed=0
            )
            
            return Ok(state)
            
        except Exception as e:
            return Err(e)
    
    def get_workflow_steps(self) -> List:
        """Get the list of workflow steps for the cleanup job.
        
        Returns:
            List of step functions
        """
        return [
            self._cleanup_metrics_files,
            self._cleanup_notebooks_files,
        ]
    
    def finalize_state(self, state: MetricsCleanupState) -> MetricsCleanupState:
        """Finalize the cleanup state before converting to results.
        
        Args:
            state: The final state after all steps
            
        Returns:
            Finalized state ready for conversion to results
        """
        state.completed_at = datetime.now()
        
        messages = []
        if state.files_removed > 0:
            messages.append(f"Cleaned up {state.files_removed} old metrics directories")
        if state.notebook_dirs_removed > 0:
            messages.append(f"Cleaned up {state.notebook_dirs_removed} old notebook directories")
        
        if messages:
            state.status = 'success'
            state.message = "; ".join(messages)
        else:
            state.status = 'success'
            state.message = f"No old files to clean up (checked files older than {state.max_age_days} days)"
        
        return state
    
    def _cleanup_metrics_files(self, state: MetricsCleanupState) -> Result[MetricsCleanupState, Exception]:
        """Cleanup old metrics files.
        
        Args:
            state: Current job state
            
        Returns:
            Result containing updated state or an error
        """
        try:
            metrics_manager = ScrapeOnceMetricsManager(
                metrics_dir=state.metrics_dir,
                archive_dir=state.archive_dir,
                enable_archive=state.enable_archive
            )
            
            files_removed = metrics_manager.cleanup_very_old_files(state.max_age_days)
            state.files_removed = files_removed
            
            self.logger.info(f"Metrics cleanup completed: {files_removed} directories removed")
            return Ok(state)
            
        except Exception as e:
            self.logger.error(f"Metrics cleanup failed: {e}")
            return Err(e)
    
    def _cleanup_notebooks_files(self, state: MetricsCleanupState) -> Result[MetricsCleanupState, Exception]:
        """Cleanup old notebooks output files.
        
        Args:
            state: Current job state
            
        Returns:
            Result containing updated state or an error
        """
        # Skip if notebooks output directory is not configured
        if not state.notebooks_output_dir:
            self.logger.info("Notebooks cleanup skipped: no notebooks_output_directory configured")
            return Ok(state)
        
        try:
            # Use notebooks retention days from state (already loaded from referenced job)
            notebooks_retention = state.notebooks_retention_days
            
            notebooks_manager = ScrapeOnceMetricsManager(
                metrics_dir=state.notebooks_output_dir,
                archive_dir=state.notebooks_archive_dir,
                enable_archive=state.enable_archive
            )
            
            notebook_dirs_removed = notebooks_manager.cleanup_very_old_files(notebooks_retention)
            state.notebook_dirs_removed = notebook_dirs_removed
            
            self.logger.info(f"Notebooks cleanup completed: {notebook_dirs_removed} directories removed (retention: {notebooks_retention} days)")
            return Ok(state)
            
        except Exception as e:
            self.logger.warning(f"Notebooks cleanup failed (continuing): {e}")
            # Don't fail the entire job if notebooks cleanup fails
            return Ok(state)
    
    @classmethod
    def main(cls, description: str = "Metrics Cleanup Job", epilog: str = None):
        """Main entry point for running the metrics cleanup job.
        
        Args:
            description: Description for argument parser
            epilog: Optional epilog text with examples
        """
        parser = cls.create_argument_parser(description, epilog)
        args = parser.parse_args()
        
        if args.list_jobs:
            job = cls(args.config, args.verbose)
            jobs = job.list_jobs()
            print(f"Available job IDs: {', '.join(jobs)}")
            return
        
        if not args.job_id:
            parser.error("--job-id is required when not using --list-jobs")
        
        job = cls(args.config, args.verbose)
        results = job.run_job(args.job_id)
        
        # Output results as JSON
        import json
        print(json.dumps(results, indent=2))
    

