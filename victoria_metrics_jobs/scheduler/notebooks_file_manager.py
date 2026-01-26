#!/usr/bin/env python3
"""
Notebooks file manager for managing notebook output files.
Handles cleanup of old notebooks and serving notebook files via HTTP.
Files are organized in YYYY/MM/DD partition structure and cleaned up by scheduled job.
"""

import os
import logging
import shutil
from pathlib import Path
from typing import Optional
from datetime import date, timedelta
from flask import Response

logger = logging.getLogger(__name__)


class NotebooksFileManager:
    """Manages notebook output files with date-based partitioning.
    
    Handles cleanup of old notebooks and serving notebook files.
    Cleanup job removes files older than configured max_age_days (default: 14 days).
    """
    
    def __init__(
        self,
        notebooks_dir: str,
        archive_dir: Optional[str] = None,
        enable_archive: bool = True
    ):
        """Initialize the notebooks file manager.
        
        Args:
            notebooks_dir: Directory for notebook output files
            archive_dir: Optional directory for archived files
            enable_archive: Whether to archive files instead of deleting
        """
        self.notebooks_dir = Path(notebooks_dir)
        self.notebooks_dir.mkdir(parents=True, exist_ok=True)
        
        self.archive_dir = Path(archive_dir) if archive_dir and enable_archive else None
        if self.archive_dir:
            self.archive_dir.mkdir(parents=True, exist_ok=True)
        
        self.enable_archive = enable_archive
    
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
        
        # Create partition directory: notebooks_dir/YYYY/MM/DD/
        partition_dir = self.notebooks_dir / year / month / day
        partition_dir.mkdir(parents=True, exist_ok=True)
        
        return partition_dir
    
    def cleanup_very_old_files(self, max_age_days: int = 14):
        """Cleanup day directories older than max_age_days.
        
        Deletes entire day directories (YYYY/MM/DD/) at once for efficiency.
        Also cleans up empty month and year directories after day directories are removed.
        
        Args:
            max_age_days: Maximum age in days before cleanup (default: 14)
            
        Returns:
            Number of directories removed
        """
        cutoff_date = date.today() - timedelta(days=max_age_days)
        removed_dirs = 0
        empty_dirs_removed = 0
        
        # Iterate through day directories and delete entire directories
        day_dirs_to_remove = []
        for year_dir in sorted(self.notebooks_dir.iterdir()):
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
                    relative_path = day_dir.relative_to(self.notebooks_dir)
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
        for month_dir in sorted(self.notebooks_dir.rglob("*"), reverse=True):
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
