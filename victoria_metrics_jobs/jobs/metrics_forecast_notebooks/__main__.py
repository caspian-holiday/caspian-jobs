#!/usr/bin/env python3
"""
Entry point for running the metrics_forecast_notebooks job as a module.
Usage: python -m victoria_metrics_jobs.jobs.metrics_forecast_notebooks
"""

from .metrics_forecast_notebooks import main


if __name__ == "__main__":
    main()

