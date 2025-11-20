#!/usr/bin/env python3
"""
Entry point for running scheduler service as a module.
Usage: python -m scheduler.service
"""

if __name__ == "__main__":
    from .service import SchedulerService
    import argparse
    import sys
    from .logging_config import setup_logging
    
    parser = argparse.ArgumentParser(description="Victoria Metrics Jobs Service")
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
        help="Path to log file (default: /var/log/scheduler/scheduler.log)"
    )
    
    args = parser.parse_args()
    
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
        sys.exit(0)
    except Exception as e:
        print(f"Failed to start scheduler service: {e}")
        sys.exit(1)
