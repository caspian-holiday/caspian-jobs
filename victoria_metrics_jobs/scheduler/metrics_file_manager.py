#!/usr/bin/env python3
"""
Metrics file manager with explicit timestamps for VictoriaMetrics deduplication.
Manages .prom files that can be scraped multiple times safely.
Files are kept for a configurable period (default: 14 days) and cleaned up by scheduled job.
"""

import os
import logging
import shutil
from pathlib import Path
from typing import Optional
from datetime import date, timedelta
from flask import Response

logger = logging.getLogger(__name__)


class ScrapeOnceMetricsManager:
    """Manages .prom files with explicit timestamps for VictoriaMetrics deduplication.
    
    Serves all files except today's to avoid read/write conflicts.
    With explicit timestamps, VictoriaMetrics deduplicates automatically,
    so files can be scraped multiple times safely.
    Cleanup job removes files older than configured max_age_days (default: 14 days).
    """
    
    def __init__(
        self,
        metrics_dir: str,
        archive_dir: Optional[str] = None,
        enable_archive: bool = True
    ):
        """Initialize the metrics file manager.
        
        Args:
            metrics_dir: Directory for .prom files
            archive_dir: Optional directory for archived files
            enable_archive: Whether to archive files instead of deleting
        """
        self.metrics_dir = Path(metrics_dir)
        self.metrics_dir.mkdir(parents=True, exist_ok=True)
        
        self.archive_dir = Path(archive_dir) if archive_dir and enable_archive else None
        if self.archive_dir:
            self.archive_dir.mkdir(parents=True, exist_ok=True)
        
        self.enable_archive = enable_archive
    
    def get_current_business_date(self) -> str:
        """Get current business date (today, or last weekday if weekend).
        
        Returns:
            ISO format date string (YYYY-MM-DD)
        """
        today = date.today()
        # If weekend, use last Friday
        while today.weekday() >= 5:  # Saturday=5, Sunday=6
            today -= timedelta(days=1)
        return today.isoformat()
    
    def _get_partition_path(self, business_date: str) -> Path:
        """Get partition directory path for a given business date.
        
        Creates year/month/day partition structure: YYYY/MM/DD/
        
        Args:
            business_date: Business date in ISO format (YYYY-MM-DD)
            
        Returns:
            Path to the partition directory
        """
        # Parse date to extract year, month, and day
        date_obj = date.fromisoformat(business_date)
        year = str(date_obj.year)
        month = f"{date_obj.month:02d}"  # Zero-padded month
        day = f"{date_obj.day:02d}"  # Zero-padded day
        
        # Create partition directory: metrics_dir/YYYY/MM/DD/
        partition_dir = self.metrics_dir / year / month / day
        partition_dir.mkdir(parents=True, exist_ok=True)
        
        return partition_dir
    
    def append_job_metrics(
        self,
        job_id: str,
        business_date: str,
        start_timestamp: float,
        end_timestamp: float,
        runtime_seconds: float,
        status: str,
        processed_entries: Optional[int] = None
    ):
        """Append metrics to daily .prom file with explicit timestamps.
        
        Files are stored in year/month/day partitions: YYYY/MM/DD/job_metrics_{job_id}_{business_date}.prom
        Multiple runs per day append to the same file.
        Database advisory lock ensures no concurrent writes to same job_id.
        Explicit timestamps allow VictoriaMetrics to deduplicate if scraped multiple times.
        
        Args:
            job_id: Job identifier
            business_date: Business date in ISO format (YYYY-MM-DD)
            start_timestamp: Unix timestamp of job start (seconds)
            end_timestamp: Unix timestamp of job end (seconds)
            runtime_seconds: Job execution duration in seconds
            status: Job status (success/failure/partial_success)
            processed_entries: Optional number of processed entries (job-specific)
        """
        # Get partition directory (YYYY/MM/DD/)
        partition_dir = self._get_partition_path(business_date)
        metrics_file = partition_dir / f"job_metrics_{job_id}_{business_date}.prom"
        
        # Convert to milliseconds for Prometheus format (explicit timestamps)
        start_ts_ms = int(start_timestamp * 1000)
        end_ts_ms = int(end_timestamp * 1000)
        
        # Append mode - multiple runs append to same file
        with open(metrics_file, 'a') as f:
            run_id = f"{int(start_timestamp)}_{job_id}"
            
            # Write metrics in Prometheus text format with explicit timestamps
            f.write(f"# Run {run_id} at {int(start_timestamp)}\n")
            
            # Job start timestamp (with explicit timestamp in milliseconds)
            f.write(f"# TYPE job_start_timestamp gauge\n")
            f.write(
                f"job_start_timestamp{{job_id='{job_id}',business_date='{business_date}'}} "
                f"{int(start_timestamp)} {start_ts_ms}\n"
            )
            
            # Job end timestamp (with explicit timestamp in milliseconds)
            f.write(f"# TYPE job_end_timestamp gauge\n")
            f.write(
                f"job_end_timestamp{{job_id='{job_id}',business_date='{business_date}'}} "
                f"{int(end_timestamp)} {end_ts_ms}\n"
            )
            
            # Job runtime (use start timestamp as metric timestamp)
            f.write(f"# TYPE job_runtime_seconds gauge\n")
            f.write(
                f"job_runtime_seconds{{job_id='{job_id}',business_date='{business_date}',status='{status}'}} "
                f"{runtime_seconds} {start_ts_ms}\n"
            )
            
            # Processed entries (use start timestamp as metric timestamp)
            if processed_entries is not None:
                f.write(f"# TYPE job_processed_entries counter\n")
                f.write(
                    f"job_processed_entries{{job_id='{job_id}',business_date='{business_date}'}} "
                    f"{processed_entries} {start_ts_ms}\n"
                )
            
            f.write("\n")  # Blank line separator
        
        # Set permissions
        os.chmod(metrics_file, 0o644)
        logger.debug(f"Appended metrics to {metrics_file.name}")
    
    def serve_available_metrics(self) -> Response:
        """Serve all metrics files except today's.
        
        With explicit timestamps in metrics, VictoriaMetrics deduplicates automatically.
        Files can be scraped multiple times safely without creating duplicate data.
        Only current day files are excluded to avoid read/write conflicts.
        Iterates through year/month/day partition directories.
        
        Returns:
            Flask Response with Prometheus text format metrics
        """
        current_date = self.get_current_business_date()
        
        logger.info(f"Serving available metrics (excluding current date: {current_date})")
        
        # Get all day directories except current day
        day_dirs_to_serve = []
        for year_dir in sorted(self.metrics_dir.iterdir()):
            if not year_dir.is_dir() or not year_dir.name.isdigit():
                continue
            for month_dir in sorted(year_dir.iterdir()):
                if not month_dir.is_dir() or not month_dir.name.isdigit():
                    continue
                for day_dir in sorted(month_dir.iterdir()):
                    if not day_dir.is_dir() or not day_dir.name.isdigit():
                        continue
                    # Extract date from directory path (YYYY/MM/DD)
                    day_date = f"{year_dir.name}-{month_dir.name}-{day_dir.name}"
                    if day_date != current_date:
                        day_dirs_to_serve.append(day_dir)
        
        if not day_dirs_to_serve:
            return Response(
                f"# No metrics available (current date: {current_date})\n",
                mimetype='text/plain; version=0.0.4'
            )
        
        def generate():
            """Stream files from day directories without deleting (cleanup job handles deletion)."""
            for day_dir in sorted(day_dirs_to_serve):
                for file_path in sorted(day_dir.glob("job_metrics_*.prom")):
                    logger.debug(f"Serving {file_path.name}")
                    try:
                        with open(file_path, 'r') as f:
                            yield from f
                            yield "\n"
                    except Exception as e:
                        logger.error(f"Error reading {file_path.name}: {e}")
        
        return Response(
            generate(),
            mimetype='text/plain; version=0.0.4',
            headers={'Content-Type': 'text/plain; version=0.0.4'}
        )
    
    def list_current_day_files(self) -> list:
        """List current day files (for debugging, not served).
        
        Returns:
            List of Path objects for current day files
        """
        current_date = self.get_current_business_date()
        # Get current day's partition directory
        partition_dir = self._get_partition_path(current_date)
        return sorted(partition_dir.glob("job_metrics_*.prom"))
    
    def cleanup_very_old_files(self, max_age_days: int = 14):
        """Cleanup day directories older than max_age_days.
        
        Since files can be scraped multiple times safely (explicit timestamps enable deduplication),
        we can keep them longer as a safety buffer. Default is 2 weeks.
        Deletes entire day directories (YYYY/MM/DD/) at once for efficiency.
        Also cleans up empty month and year directories after day directories are removed.
        
        Args:
            max_age_days: Maximum age in days before cleanup (default: 14)
        """
        cutoff_date = date.today() - timedelta(days=max_age_days)
        removed_dirs = 0
        empty_dirs_removed = 0
        
        # Iterate through day directories and delete entire directories
        day_dirs_to_remove = []
        for year_dir in sorted(self.metrics_dir.iterdir()):
            if not year_dir.is_dir() or not year_dir.name.isdigit():
                continue
            for month_dir in sorted(year_dir.iterdir()):
                if not month_dir.is_dir() or not month_dir.name.isdigit():
                    continue
                for day_dir in sorted(month_dir.iterdir()):
                    if not day_dir.is_dir() or not day_dir.name.isdigit():
                        continue
                    # Extract date from directory path (YYYY/MM/DD)
                    try:
                        day_date = date.fromisoformat(f"{year_dir.name}-{month_dir.name}-{day_dir.name}")
                        if day_date < cutoff_date:
                            day_dirs_to_remove.append((day_dir, year_dir, month_dir))
                    except ValueError:
                        logger.warning(f"Invalid date in directory name: {day_dir}")
                        continue
        
        # Remove day directories
        for day_dir, year_dir, month_dir in day_dirs_to_remove:
            try:
                if self.archive_dir:
                    # Archive entire day directory, preserving structure
                    relative_path = day_dir.relative_to(self.metrics_dir)
                    archive_path = self.archive_dir / relative_path
                    archive_path.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(day_dir), str(archive_path))
                    logger.info(f"Archived day directory: {day_dir.name} (older than {max_age_days} days)")
                else:
                    shutil.rmtree(day_dir)
                    logger.info(f"Deleted day directory: {day_dir.name} (older than {max_age_days} days)")
                removed_dirs += 1
            except Exception as e:
                logger.error(f"Failed to cleanup day directory {day_dir}: {e}")
        
        # Clean up empty month and year directories
        # Start from deepest level (month) and work up
        for month_dir in sorted(self.metrics_dir.rglob("*"), reverse=True):
            if month_dir.is_dir() and not any(month_dir.iterdir()):
                try:
                    month_dir.rmdir()
                    empty_dirs_removed += 1
                    logger.debug(f"Removed empty directory: {month_dir}")
                except Exception as e:
                    logger.debug(f"Could not remove directory {month_dir}: {e}")
        
        if empty_dirs_removed > 0:
            logger.info(f"Removed {empty_dirs_removed} empty partition directories")
        
        return removed_dirs
    
    def serve_notebook_directory_listing(self, notebooks_dir: Path) -> Response:
        """Serve directory listing of available notebooks organized by date.
        
        Args:
            notebooks_dir: Directory containing notebook outputs (with YYYY/MM/DD structure)
            
        Returns:
            Flask Response with HTML directory listing
        """
        if not notebooks_dir.exists():
            return Response(
                "<html><body><h1>Notebooks Directory Not Found</h1></body></html>",
                mimetype='text/html',
                status=404
            )
        
        html_parts = ["<html><head><title>Notebooks</title></head><body>"]
        html_parts.append("<h1>Executed Notebooks</h1>")
        html_parts.append("<ul>")
        
        # Walk through date partitions
        for year_dir in sorted(notebooks_dir.iterdir()):
            if not year_dir.is_dir() or not year_dir.name.isdigit():
                continue
            for month_dir in sorted(year_dir.iterdir()):
                if not month_dir.is_dir() or not month_dir.name.isdigit():
                    continue
                for day_dir in sorted(month_dir.iterdir()):
                    if not day_dir.is_dir() or not day_dir.name.isdigit():
                        continue
                    
                    date_str = f"{year_dir.name}/{month_dir.name}/{day_dir.name}"
                    html_parts.append(f'<li><strong>{date_str}</strong><ul>')
                    
                    # List notebooks for this day
                    for notebook_file in sorted(day_dir.glob("*.ipynb")):
                        filename = notebook_file.name
                        html_filename = notebook_file.stem + ".html"
                        html_path = day_dir / html_filename
                        
                        html_parts.append(
                            f'<li>'
                            f'<a href="/notebooks/{year_dir.name}/{month_dir.name}/{day_dir.name}/{filename}">{filename}</a> '
                            f'(<a href="/notebooks/{year_dir.name}/{month_dir.name}/{day_dir.name}/{html_filename}">HTML</a>)'
                            f'</li>'
                        )
                    
                    html_parts.append("</ul></li>")
        
        html_parts.append("</ul></body></html>")
        return Response("".join(html_parts), mimetype='text/html')
    
    def serve_notebook_file(self, notebooks_dir: Path, year: str, month: str, day: str, filename: str) -> Response:
        """Serve a notebook file (.ipynb or .html).
        
        Args:
            notebooks_dir: Directory containing notebook outputs
            year: Year (YYYY)
            month: Month (MM)
            day: Day (DD)
            filename: Filename (with extension)
            
        Returns:
            Flask Response with file content
        """
        file_path = notebooks_dir / year / month / day / filename
        
        if not file_path.exists():
            return Response(
                f"File not found: {filename}",
                mimetype='text/plain',
                status=404
            )
        
        try:
            if filename.endswith('.html'):
                mimetype = 'text/html'
            elif filename.endswith('.ipynb'):
                mimetype = 'application/json'
            else:
                mimetype = 'application/octet-stream'
            
            with open(file_path, 'rb') as f:
                content = f.read()
            
            return Response(content, mimetype=mimetype)
        except Exception as e:
            logger.error(f"Error serving notebook file {file_path}: {e}")
            return Response(
                f"Error reading file: {str(e)}",
                mimetype='text/plain',
                status=500
            )

