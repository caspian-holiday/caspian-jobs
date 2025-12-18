#!/usr/bin/env python3
"""
Entry point for metrics cleanup job.
Usage: python -m victoria_metrics_jobs.jobs.metrics_cleanup
"""

if __name__ == "__main__":
    from .metrics_cleanup import MetricsCleanupJob
    MetricsCleanupJob.main(
        description="Metrics Cleanup Job - Removes old .prom files",
        epilog="""
Examples:
  # List available cleanup job configurations
  VM_JOBS_ENVIRONMENT=local poetry run python -m victoria_metrics_jobs.jobs.metrics_cleanup \\
    --config victoria_metrics_jobs/victoria_metrics_jobs.yml --list-jobs

  # Run cleanup job
  VM_JOBS_ENVIRONMENT=local poetry run python -m victoria_metrics_jobs.jobs.metrics_cleanup \\
    --config victoria_metrics_jobs/victoria_metrics_jobs.yml --job-id metrics_cleanup
        """
    )

