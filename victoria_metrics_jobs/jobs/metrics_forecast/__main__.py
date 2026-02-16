#!/usr/bin/env python3
"""
Entry point for running the metrics_forecast job as a module.
Usage: python -m victoria_metrics_jobs.jobs.metrics_forecast
"""

from .metrics_forecast import main


if __name__ == "__main__":
    main()

