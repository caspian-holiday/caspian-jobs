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
                files_removed=0
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
            self._cleanup_old_files
        ]
    
    def finalize_state(self, state: MetricsCleanupState) -> MetricsCleanupState:
        """Finalize the cleanup state before converting to results.
        
        Args:
            state: The final state after all steps
            
        Returns:
            Finalized state ready for conversion to results
        """
        state.completed_at = datetime.now()
        
        if state.files_removed > 0:
            state.status = 'success'
            state.message = f"Cleaned up {state.files_removed} old metrics files (older than {state.max_age_days} days)"
        else:
            state.status = 'success'
            state.message = f"No old files to clean up (checked files older than {state.max_age_days} days)"
        
        return state
    
    def _cleanup_old_files(self, state: MetricsCleanupState) -> Result[MetricsCleanupState, Exception]:
        """Cleanup old metrics files.
        
        Args:
            state: Current job state
            
        Returns:
            Result containing updated state or an error
        """
        try:
            # Create metrics manager instance
            metrics_manager = ScrapeOnceMetricsManager(
                metrics_dir=state.metrics_dir,
                archive_dir=state.archive_dir,
                enable_archive=state.enable_archive
            )
            
            # Perform cleanup
            files_removed = metrics_manager.cleanup_very_old_files(state.max_age_days)
            state.files_removed = files_removed
            
            self.logger.info(f"Cleanup completed: {files_removed} files removed")
            
            return Ok(state)
            
        except Exception as e:
            self.logger.error(f"Cleanup failed: {e}")
            return Err(e)
    
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
    

