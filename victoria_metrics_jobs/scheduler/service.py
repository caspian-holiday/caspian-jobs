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
from threading import Thread

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
from apscheduler.schedulers.base import BaseScheduler
from flask import Flask

from .config import ConfigLoader
from .jobs import JobExecutor
from .database import DatabaseManager
from .notebooks_file_manager import NotebooksFileManager


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
        self.notebooks_manager: Optional[NotebooksFileManager] = None
        self.http_app: Optional[Flask] = None
        self.http_thread: Optional[Thread] = None
        self.notebooks_config: Optional[Dict[str, Any]] = None
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
            
            # Initialize notebooks manager and HTTP server if notebooks config is present
            if 'metrics' in config:
                metrics_config = config['metrics']
                notebooks_output_dir = metrics_config.get('notebooks_output_directory')
                
                if notebooks_output_dir:
                    try:
                        # Initialize notebooks manager for cleanup and serving
                        self.notebooks_manager = NotebooksFileManager(
                            notebooks_dir=notebooks_output_dir,
                            archive_dir=metrics_config.get('notebooks_archive_directory'),
                            enable_archive=metrics_config.get('enable_archive', True)
                        )
                        self.logger.info(f"Initialized notebooks manager with directory: {notebooks_output_dir}")
                        
                        # Start HTTP server for notebooks and health endpoints
                        http_port = metrics_config.get('port', 8000)
                        http_host = metrics_config.get('host', '0.0.0.0')
                        self._start_http_server(http_host, http_port, metrics_config)
                        
                        # Store notebooks config
                        self.notebooks_config = metrics_config
                        
                    except Exception as e:
                        self.logger.warning(f"Failed to initialize notebooks manager: {e}. Notebooks serving will not be available.")
                        self.notebooks_manager = None
                else:
                    # Start HTTP server for health endpoint only
                    http_port = metrics_config.get('port', 8000)
                    http_host = metrics_config.get('host', '0.0.0.0')
                    self._start_http_server(http_host, http_port, metrics_config)
            
            # Initialize job executor with database manager and config path
            self.job_executor = JobExecutor(
                self.database_manager,
                self.config_path
            )
            
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
        
        # Stop HTTP server
        if self.http_app:
            # Flask doesn't have a clean shutdown, but we can mark it
            self.logger.info("Stopping HTTP server...")
            # The thread will exit when scheduler stops
        
        # Disconnect from database
        if self.database_manager:
            self.database_manager.disconnect()
    
    def _start_http_server(self, host: str, port: int, config: Dict[str, Any]):
        """Start HTTP server for health and notebooks endpoints in background thread.
        
        Args:
            host: Host to bind the server
            port: Port to bind the server
            config: Configuration dictionary (for notebooks directory)
        """
        from pathlib import Path
        
        self.http_app = Flask(__name__)
        
        @self.http_app.route('/health')
        def health():
            """Health check endpoint."""
            return {'status': 'ok'}, 200
        
        @self.http_app.route('/notebooks')
        def notebooks_listing():
            """List available notebooks organized by date."""
            if not self.notebooks_manager:
                return {'error': 'Notebooks manager not initialized'}, 503
            notebooks_dir = config.get('notebooks_output_directory')
            if not notebooks_dir:
                return {'error': 'Notebooks output directory not configured'}, 404
            return self.notebooks_manager.serve_notebook_directory_listing(Path(notebooks_dir))
        
        @self.http_app.route('/notebooks/<year>/<month>/<day>/<filename>')
        def notebooks_file(year, month, day, filename):
            """Serve a notebook file (.ipynb or .html)."""
            if not self.notebooks_manager:
                return {'error': 'Notebooks manager not initialized'}, 503
            notebooks_dir = config.get('notebooks_output_directory')
            if not notebooks_dir:
                return {'error': 'Notebooks output directory not configured'}, 404
            return self.notebooks_manager.serve_notebook_file(Path(notebooks_dir), year, month, day, filename)
        
        def run_server():
            """Run Flask server (blocks)."""
            try:
                self.http_app.run(
                    host=host,
                    port=port,
                    debug=False,
                    use_reloader=False,
                    threaded=True
                )
            except Exception as e:
                self.logger.error(f"HTTP server error: {e}")
        
        # Start in background thread
        self.http_thread = Thread(target=run_server, daemon=True)
        self.http_thread.start()
        self.logger.info(f"HTTP server started on {host}:{port} (health, notebooks)")
    
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
                
                # Format schedule in human-readable form
                schedule_str = self._format_schedule(schedule)
                self.logger.info(f"Added job: {job_id} (script: {script}, schedule: {schedule_str})")
                jobs_added += 1
                
            except Exception as e:
                self.logger.error(f"Failed to add job {job_config.get('id', 'unknown')}: {e}")
        
        if jobs_added == 0:
            self.logger.info("No enabled jobs found - scheduler will run with no scheduled jobs")
    
    def _format_schedule(self, schedule: Dict[str, Any]) -> str:
        """Format schedule configuration in human-readable form.
        
        Args:
            schedule: Schedule configuration dictionary
            
        Returns:
            Human-readable schedule description
        """
        schedule_type = schedule.get('type', 'cron')
        args = schedule.get('args', {})
        
        if schedule_type == 'cron':
            try:
                from cron_descriptor import get_description, CasingTypeEnum
                
                # Convert APScheduler keyword args to cron expression: minute hour day month day_of_week
                minute = str(args.get('minute', '*'))
                hour = str(args.get('hour', '*'))
                day = str(args.get('day', '*'))
                month = str(args.get('month', '*'))
                day_of_week = args.get('day_of_week', '*')
                
                # Handle lists/ranges for day_of_week
                if isinstance(day_of_week, (list, tuple)):
                    day_of_week = ','.join(str(d) for d in day_of_week)
                else:
                    day_of_week = str(day_of_week)
                
                cron_expr = f"{minute} {hour} {day} {month} {day_of_week}"
                return get_description(cron_expr, casing_type=CasingTypeEnum.Sentence)
            except ImportError:
                # Fallback if cron-descriptor not available
                hour = args.get('hour', '*')
                minute = args.get('minute', '*')
                return f"daily at {int(hour):02d}:{int(minute):02d}" if hour != '*' and minute != '*' else "daily"
        
        elif schedule_type == 'interval':
            # Simple interval formatting
            parts = []
            for unit in ['weeks', 'days', 'hours', 'minutes', 'seconds']:
                if unit in args:
                    val = args[unit]
                    unit_name = unit.rstrip('s') if val == 1 else unit
                    parts.append(f"{val} {unit_name}")
            return f"every {', '.join(parts)}" if parts else "interval"
        
        elif schedule_type == 'date':
            run_date = args.get('run_date', 'unspecified')
            return f"on {run_date}"
        
        else:
            return f"{schedule_type}"
    
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
