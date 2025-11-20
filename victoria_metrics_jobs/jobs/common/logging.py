"""
Common logging utilities for scheduler jobs.
"""

import logging
import sys
from typing import Optional


def setup_job_logging(job_name: str, verbose: bool = False) -> logging.Logger:
    """Set up logging for a job.
    
    Args:
        job_name: Name of the job (used as logger name)
        verbose: Whether to enable verbose (DEBUG) logging
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(job_name)
    
    # Only add handlers if none exist (avoid duplicate handlers)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    # Set log level
    if verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    
    return logger


def set_verbose_logging(job_name: str, verbose: bool = True):
    """Set verbose logging for a job logger.
    
    Args:
        job_name: Name of the job logger
        verbose: Whether to enable verbose (DEBUG) logging
    """
    logger = logging.getLogger(job_name)
    if verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
