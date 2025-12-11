#!/usr/bin/env python3
"""
Entry point for running the metrics_extract job as a module.
Usage: python -m victoria_metrics_jobs.jobs.metrics_extract
"""

from .metrics_extract import main


if __name__ == "__main__":
    main()

