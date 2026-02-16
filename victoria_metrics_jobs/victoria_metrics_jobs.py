#!/usr/bin/env python3
"""
Main entry point for the Victoria Metrics Jobs service.
"""

import argparse
import os
import sys
from pathlib import Path

from .scheduler.service import SchedulerService
from .scheduler.logging_config import setup_logging


def main():
    """Main function that starts the Victoria Metrics Jobs service."""
    parser = argparse.ArgumentParser(
        description="Victoria Metrics Jobs Service",
        epilog="""
Environment Configuration:
  The service requires an environment to be specified via the VM_JOBS_ENVIRONMENT environment variable.
  Valid environments: local, dev, stg, prod
  
  Example:
    VM_JOBS_ENVIRONMENT=local python -m victoria_metrics_jobs.victoria_metrics_jobs --config config.yml
    VM_JOBS_ENVIRONMENT=dev python -m victoria_metrics_jobs.victoria_metrics_jobs --config config.yml
    VM_JOBS_ENVIRONMENT=stg python -m victoria_metrics_jobs.victoria_metrics_jobs --config config.yml
    VM_JOBS_ENVIRONMENT=prod python -m victoria_metrics_jobs.victoria_metrics_jobs --config config.yml
        """
    )
    parser.add_argument(
        "--config",
        required=True,
        help="Path to configuration file (required)"
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Logging level (default: INFO)"
    )
    parser.add_argument(
        "--log-file",
        required=True,
        help="Path to log file (required)"
    )
    
    args = parser.parse_args()
    
    # Check if environment is specified
    environment = os.getenv('VM_JOBS_ENVIRONMENT')
    if not environment:
        print("Error: VM_JOBS_ENVIRONMENT environment variable is required.")
        print("Set VM_JOBS_ENVIRONMENT to 'local', 'dev', 'stg', or 'prod' before running the service.")
        print("Example: VM_JOBS_ENVIRONMENT=local python -m victoria_metrics_jobs.victoria_metrics_jobs --config config.yml")
        return 1
    
    # Validate environment
    valid_environments = ['local', 'dev', 'stg', 'prod']
    if environment not in valid_environments:
        print(f"Error: Invalid environment '{environment}'.")
        print(f"Valid environments: {', '.join(valid_environments)}")
        return 1
    
    # Set up logging
    setup_logging(
        log_level=args.log_level,
        log_file=args.log_file
    )
    
    # Create and start the scheduler service
    try:
        service = SchedulerService(config_path=args.config)
        service.start()
    except KeyboardInterrupt:
        print("\nReceived interrupt signal, shutting down...")
        return 0
    except Exception as e:
        print(f"Failed to start scheduler service: {e}")
        return 1
    
    return 0




if __name__ == "__main__":
    sys.exit(main())
