#!/usr/bin/env python3
"""
Main Victoria Metrics Jobs service implementation using APScheduler.
"""

import logging
import os
import signal
import sys
import time
from typing import Dict, Any, Optional

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
from apscheduler.schedulers.base import BaseScheduler

from .config import ConfigLoader
from .jobs import JobExecutor
from .database import DatabaseManager


class SchedulerService:
    """Main Victoria Metrics Jobs service that manages job execution."""
    
    def __init__(self, config_path: str):
        """Initialize the Victoria Metrics Jobs service.
        
        Args:
            config_path: Path to the YAML configuration file (required)
        """
        self.config_path = config_path
        self.scheduler: Optional[BaseScheduler] = None
        self.config_loader = ConfigLoader()
        self.database_manager: Optional[DatabaseManager] = None
        self.job_executor: Optional[JobExecutor] = None
        self.logger = logging.getLogger(__name__)
        self.running = False
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        self.logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.stop()
    
    def start(self):
        """Start the scheduler service."""
        try:
            # Validate config file exists
            if not os.path.exists(self.config_path):
                raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
            
            # Load configuration
            config = self.config_loader.load(self.config_path)
            self.logger.info(f"Loaded configuration from {self.config_path}")
            
            # Initialize database manager if database config is present
            if 'database' in config:
                try:
                    self.database_manager = DatabaseManager(config['database'])
                    self.database_manager.connect()
                    
                    # Test database connection and advisory locks
                    if self.database_manager.test_connection():
                        self.logger.info("Database connection and advisory locks verified")
                    else:
                        self.logger.error("Database connection test failed")
                        raise RuntimeError("Database connection test failed")
                        
                except Exception as e:
                    self.logger.error(f"Failed to initialize database manager: {e}")
                    raise
            else:
                self.logger.warning("No database configuration found - jobs will run without advisory locking")
            
            # Initialize job executor with database manager and config path
            self.job_executor = JobExecutor(self.database_manager, self.config_path)
            
            # Configure scheduler
            jobstores = {
                'default': MemoryJobStore()
            }
            executors = {
                'default': ThreadPoolExecutor(max_workers=config.get('max_workers', 10))
            }
            job_defaults = {
                'coalesce': True,
                'max_instances': 3
            }
            
            self.scheduler = BlockingScheduler(
                jobstores=jobstores,
                executors=executors,
                job_defaults=job_defaults
            )
            
            # Add event listeners
            self.scheduler.add_listener(self._job_executed, EVENT_JOB_EXECUTED)
            self.scheduler.add_listener(self._job_error, EVENT_JOB_ERROR)
            
            # Add jobs from configuration
            self._add_jobs_from_config(config)
            
            self.running = True
            self.logger.info("Scheduler service started successfully")
            
            # Start the scheduler (this will block)
            self.scheduler.start()
            
        except Exception as e:
            self.logger.error(f"Failed to start scheduler service: {e}")
            raise
    
    def stop(self):
        """Stop the scheduler service."""
        if self.scheduler and self.running:
            self.logger.info("Stopping scheduler service...")
            self.scheduler.shutdown(wait=True)
            self.running = False
            self.logger.info("Scheduler service stopped")
        
        # Disconnect from database
        if self.database_manager:
            self.database_manager.disconnect()
    
    def _add_jobs_from_config(self, config: Dict[str, Any]):
        """Add jobs to the scheduler based on configuration."""
        jobs = config.get('jobs', {})
        
        if not jobs:
            self.logger.info("No jobs configured - scheduler will run with no scheduled jobs")
            return
        
        jobs_added = 0
        
        # Handle both dict and list formats for jobs
        if isinstance(jobs, dict):
            # Convert dict format to list for processing
            jobs_list = []
            for job_id, job_config in jobs.items():
                # Ensure job_id is in the config
                if 'id' not in job_config:
                    job_config['id'] = job_id
                jobs_list.append(job_config)
            jobs = jobs_list
        
        for job_config in jobs:
            try:
                job_id = job_config.get('id')
                script = job_config.get('script')
                schedule = job_config.get('schedule')
                enabled = job_config.get('enabled', True)
                
                if not enabled:
                    self.logger.info(f"Skipping disabled job: {job_id}")
                    continue
                
                if not all([job_id, script, schedule]):
                    self.logger.warning(f"Invalid job configuration: {job_config}")
                    continue
                
                # Add job to scheduler
                self.scheduler.add_job(
                    func=self.job_executor.execute_job,
                    trigger=schedule.get('type', 'cron'),
                    args=[job_config],
                    id=job_id,
                    name=job_config.get('name', job_id),
                    **schedule.get('args', {})
                )
                
                self.logger.info(f"Added job: {job_id} (script: {script})")
                jobs_added += 1
                
            except Exception as e:
                self.logger.error(f"Failed to add job {job_config.get('id', 'unknown')}: {e}")
        
        if jobs_added == 0:
            self.logger.info("No enabled jobs found - scheduler will run with no scheduled jobs")
    
    def _job_executed(self, event):
        """Handle successful job execution."""
        self.logger.info(f"Job {event.job_id} executed successfully")
    
    def _job_error(self, event):
        """Handle job execution errors."""
        self.logger.error(f"Job {event.job_id} failed: {event.exception}")
    
    def reload_config(self):
        """Reload configuration and restart scheduler."""
        self.logger.info("Reloading configuration...")
        self.stop()
        time.sleep(1)  # Brief pause before restart
        self.start()
