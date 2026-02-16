#!/usr/bin/env python3
"""
Logging configuration for the scheduler service.
"""

import logging
import logging.handlers
import os
import sys
from typing import Optional


def setup_logging(
    log_level: str = "INFO",
    log_file: Optional[str] = None,
    log_dir: str = "/var/log/scheduler",
    backup_count: int = 5
) -> None:
    """Set up logging configuration for the scheduler service.
    
    Logs are rotated weekly at midnight on Monday. Backup files are retained
    for the number of weeks specified by backup_count.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Path to log file (if None, logs to stdout)
        log_dir: Directory for log files (used if log_file is None)
        backup_count: Number of weekly backup log files to keep
    """
    # Convert string level to logging constant
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    
    # Create formatter
    formatter = logging.Formatter(
        fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Get root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)
    
    # Clear existing handlers
    root_logger.handlers.clear()
    
    # Determine log destination
    if log_file:
        # Use specified log file
        log_path = log_file
    else:
        # Use default log directory
        os.makedirs(log_dir, exist_ok=True)
        log_path = os.path.join(log_dir, "scheduler.log")
    
    # Create file handler with weekly rotation
    try:
        file_handler = logging.handlers.TimedRotatingFileHandler(
            log_path,
            when='W0',
            interval=1,
            backupCount=backup_count,
            encoding='utf-8'
        )
        file_handler.setLevel(numeric_level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
        
        print(f"Logging to file: {log_path}")
        
    except PermissionError:
        # Fall back to stdout if we can't write to the log file
        print(f"Warning: Cannot write to log file {log_path}, falling back to stdout")
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(numeric_level)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)
    
    # Also add console handler for development/debugging
    if os.getenv('SCHEDULER_DEBUG', '').lower() in ('true', '1', 'yes'):
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.DEBUG)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)
    
    # Set specific logger levels
    logging.getLogger('apscheduler').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance.
    
    Args:
        name: Logger name (usually __name__)
        
    Returns:
        Logger instance
    """
    return logging.getLogger(name)
