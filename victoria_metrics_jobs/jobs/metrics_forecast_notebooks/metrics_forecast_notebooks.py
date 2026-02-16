#!/usr/bin/env python3
"""
Metrics Forecast Notebooks Job - Executes Jupyter notebooks for forecasting.

The job:
1. Scans notebooks/ folder for .ipynb files
2. Executes each notebook using papermill
3. Converts executed notebooks to HTML using nbconvert
4. Stores outputs in date-partitioned structure (YYYY/MM/DD/)
5. Notebooks handle their own forecasting logic using darts wrappers
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

# Add the scheduler module to the path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from victoria_metrics_jobs.jobs.common import BaseJob, BaseJobState, Err, Ok, Result
from victoria_metrics_jobs.scheduler.notebooks_file_manager import NotebooksFileManager


@dataclass
class MetricsForecastNotebooksState(BaseJobState):
    """State object for the metrics_forecast_notebooks job."""

    notebooks_dir: Path = Path("notebooks")
    notebooks_output_dir: Path = Path("/var/lib/scheduler/notebooks_output")
    notebooks_found: List[str] = field(default_factory=list)
    notebooks_executed: int = 0
    notebooks_succeeded: int = 0
    notebooks_failed: int = 0
    timeseries_processed: int = 0  # Total timeseries with successful forecast (across all notebooks)
    timeseries_failed: int = 0  # Total timeseries skipped/failed (across all notebooks)
    execution_results: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    current_business_date: Optional[date] = None
    vm_query_url: str = ""
    vm_token: str = ""
    db_connection_string: str = ""
    papermill_start_timeout: int = 300  # Timeout for kernel startup (seconds); nbclient Integer trait expects int
    papermill_execution_timeout: Optional[int] = None  # Timeout for cell execution (None = no timeout); nbclient Integer trait expects int
    config_path: str = ""  # Path to YAML config file; passed to notebook so it can call create_database_connection(..., config_path=...)

    def to_results(self) -> Dict[str, Any]:
        """Extend base results with notebook execution metadata."""
        results = super().to_results()
        results.update(
            {
                "notebooks_found": len(self.notebooks_found),
                "notebooks_executed": self.notebooks_executed,
                "notebooks_succeeded": self.notebooks_succeeded,
                "notebooks_failed": self.notebooks_failed,
                "timeseries_processed": self.timeseries_processed,
                "timeseries_failed": self.timeseries_failed,
                "current_business_date": self.current_business_date.isoformat()
                if self.current_business_date
                else None,
            }
        )
        return results


class MetricsForecastNotebooksJob(BaseJob):
    """Metrics Forecast Notebooks job that executes Jupyter notebooks."""

    def __init__(self, config_path: str = None, verbose: bool = False):
        super().__init__("metrics_forecast_notebooks", config_path, verbose)

    def create_initial_state(
        self, job_id: str
    ) -> Result[MetricsForecastNotebooksState, Exception]:
        try:
            job_config = self.get_job_config(job_id)

            # Get notebooks directory (relative to job directory or absolute)
            notebooks_dir = job_config.get("notebooks_directory", "notebooks")
            notebooks_dir_path = Path(notebooks_dir)
            if not notebooks_dir_path.is_absolute():
                # If relative, assume relative to this job's directory
                job_dir = Path(__file__).parent
                notebooks_dir_path = job_dir / notebooks_dir_path

            # Get notebooks output directory
            metrics_config = job_config.get("metrics", {})
            notebooks_output_dir = metrics_config.get(
                "notebooks_output_directory",
                job_config.get("notebooks_output_directory", "/var/lib/scheduler/notebooks_output"),
            )
            notebooks_output_dir_path = Path(notebooks_output_dir)
            
            # Get timeout settings for papermill execution (nbclient Integer trait expects int, not float)
            # start_timeout: time to wait for kernel to start (default: 60s, increase for slow systems)
            # execution_timeout: time to wait for each cell execution (default: None = no timeout)
            _start = job_config.get("papermill_start_timeout", 300)
            _exec = job_config.get("papermill_execution_timeout", None)
            papermill_start_timeout = int(_start) if _start is not None else 300
            papermill_execution_timeout = int(_exec) if _exec is not None else None

            # Get Victoria Metrics and database config for notebook parameters
            victoria_metrics_cfg = job_config.get("victoria_metrics", {})
            database_cfg = job_config.get("database") or job_config.get("forecast_database", {})
            
            # Config path used to load this job (scheduler passes --config); notebook uses it for create_database_connection
            config_path = getattr(self, "config_path", None) or ""

            state = MetricsForecastNotebooksState(
                job_id=job_id,
                job_config=job_config,
                started_at=datetime.now(),
                notebooks_dir=notebooks_dir_path,
                notebooks_output_dir=notebooks_output_dir_path,
                vm_query_url=victoria_metrics_cfg.get("query_url", ""),
                vm_token=victoria_metrics_cfg.get("token", ""),
                db_connection_string=self._build_database_connection_string(database_cfg) if database_cfg else "",
                papermill_start_timeout=papermill_start_timeout,
                papermill_execution_timeout=papermill_execution_timeout,
                config_path=config_path or "",
            )

            self.logger.info(
                "Initialized metrics_forecast_notebooks job. "
                f"Notebooks directory: {notebooks_dir_path}, "
                f"Output directory: {notebooks_output_dir_path}"
            )

            return Ok(state)
        except Exception as exc:
            return Err(exc)

    def get_workflow_steps(self) -> List[Callable]:
        return [
            self._derive_current_business_date,
            self._discover_notebooks,
            self._execute_notebooks,
        ]

    def finalize_state(
        self, state: MetricsForecastNotebooksState
    ) -> MetricsForecastNotebooksState:
        state.completed_at = datetime.now()

        if state.notebooks_failed > 0 and state.notebooks_succeeded == 0:
            state.status = "error"
            state.message = "All notebook executions failed"
        elif state.notebooks_failed > 0:
            state.status = "partial_success"
            state.message = (
                f"Notebooks executed with warnings: "
                f"{state.notebooks_succeeded} succeeded, {state.notebooks_failed} failed"
            )
        elif state.notebooks_executed == 0:
            state.status = "success"
            state.message = "No notebooks found to execute"
        else:
            state.status = "success"
            state.message = (
                f"Successfully executed {state.notebooks_succeeded} notebook(s)"
            )

        return state

    def _derive_current_business_date(
        self, state: MetricsForecastNotebooksState
    ) -> Result[MetricsForecastNotebooksState, Exception]:
        """Determine the current business date for partitioning outputs."""
        try:
            cutoff_hour = int(state.job_config.get("cutoff_hour", 6))
            now = datetime.utcnow()

            if now.weekday() >= 5 or now.hour < cutoff_hour:
                # roll back to previous business day
                days_back = 1
                if now.weekday() == 5:  # Saturday -> Friday
                    days_back = 1
                elif now.weekday() == 6:  # Sunday -> Friday
                    days_back = 2
                elif now.hour < cutoff_hour and now.weekday() == 0:
                    days_back = 3  # Monday before cutoff -> Friday
                state.current_business_date = (now - timedelta(days=days_back)).date()
            else:
                state.current_business_date = now.date()

            self.logger.info("Current business date: %s", state.current_business_date)
            return Ok(state)
        except Exception as exc:
            self.logger.error("Failed to derive business date: %s", exc)
            return Err(exc)

    def _discover_notebooks(
        self, state: MetricsForecastNotebooksState
    ) -> Result[MetricsForecastNotebooksState, Exception]:
        """Scan notebooks directory for .ipynb files."""
        try:
            if not state.notebooks_dir.exists():
                self.logger.warning(
                    "Notebooks directory does not exist: %s", state.notebooks_dir
                )
                return Ok(state)

            # Find all .ipynb files (but exclude files starting with . or _)
            notebooks = [
                str(nb.relative_to(state.notebooks_dir))
                for nb in state.notebooks_dir.glob("*.ipynb")
                if not nb.name.startswith(".") and not nb.name.startswith("_")
            ]

            state.notebooks_found = sorted(notebooks)
            self.logger.info(
                "Discovered %s notebook(s): %s",
                len(state.notebooks_found),
                ", ".join(state.notebooks_found) if state.notebooks_found else "none",
            )

            return Ok(state)
        except Exception as exc:
            self.logger.error("Failed to discover notebooks: %s", exc)
            return Err(exc)

    def _execute_notebooks(
        self, state: MetricsForecastNotebooksState
    ) -> Result[MetricsForecastNotebooksState, Exception]:
        """Execute each discovered notebook using papermill."""
        try:
            if not state.notebooks_found:
                self.logger.info("No notebooks to execute")
                return Ok(state)

            # Get partition path for output (using same structure as notebooks)
            business_date_str = state.current_business_date.isoformat()
            notebooks_manager = NotebooksFileManager(
                notebooks_dir=str(state.notebooks_output_dir),
                archive_dir=None,
                enable_archive=False,
            )
            partition_dir = notebooks_manager._get_partition_path(business_date_str)

            # Execute each notebook
            for notebook_rel_path in state.notebooks_found:
                notebook_path = state.notebooks_dir / notebook_rel_path
                notebook_name = notebook_path.stem

                # Generate output filename with timestamp
                timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                output_filename = f"{notebook_name}_{timestamp}.ipynb"
                output_path = partition_dir / output_filename

                try:
                    self.logger.info(
                        "Executing notebook: %s -> %s", notebook_path, output_path
                    )

                    # Path for notebook to write timeseries_processed / timeseries_failed
                    output_results_path = partition_dir / f"{output_path.stem}_results.json"

                    # Execute notebook using papermill
                    result = self._execute_with_papermill(
                        notebook_path, output_path, state, output_results_path
                    )

                    if result["success"]:
                        # Generate HTML version
                        html_output_path = partition_dir / f"{notebook_name}_{timestamp}.html"
                        self._convert_to_html(output_path, html_output_path)

                        state.notebooks_succeeded += 1
                        exec_result = {
                            "status": "success",
                            "output_path": str(output_path),
                            "html_path": str(html_output_path),
                            "execution_time": result.get("execution_time", 0),
                        }
                        # Add timeseries counts if notebook wrote results
                        if result.get("timeseries_processed") is not None:
                            exec_result["timeseries_processed"] = result["timeseries_processed"]
                            state.timeseries_processed += result["timeseries_processed"]
                        if result.get("timeseries_failed") is not None:
                            exec_result["timeseries_failed"] = result["timeseries_failed"]
                            state.timeseries_failed += result["timeseries_failed"]
                        state.execution_results[notebook_rel_path] = exec_result
                        self.logger.info(
                            "Successfully executed notebook: %s", notebook_rel_path
                        )
                    else:
                        state.notebooks_failed += 1
                        state.execution_results[notebook_rel_path] = {
                            "status": "failed",
                            "error": result.get("error", "Unknown error"),
                        }
                        self.logger.error(
                            "Failed to execute notebook %s: %s",
                            notebook_rel_path,
                            result.get("error", "Unknown error"),
                        )

                    state.notebooks_executed += 1

                except Exception as exc:
                    state.notebooks_failed += 1
                    state.execution_results[notebook_rel_path] = {
                        "status": "failed",
                        "error": str(exc),
                    }
                    self.logger.error(
                        "Exception executing notebook %s: %s",
                        notebook_rel_path,
                        exc,
                        exc_info=True,
                    )

            return Ok(state)

        except Exception as exc:
            self.logger.error("Failed to execute notebooks: %s", exc)
            return Err(exc)

    def _build_database_connection_string(self, db_config: Dict[str, Any]) -> str:
        """Build PostgreSQL connection string from config.
        
        Args:
            db_config: Database configuration dictionary
            
        Returns:
            PostgreSQL connection string
        """
        from urllib.parse import quote_plus
        
        host = db_config.get("host", "localhost")
        port = db_config.get("port", 5432)
        dbname = db_config.get("name", "forecasts_db")
        user = db_config.get("user", "forecast_user")
        password = db_config.get("password", "")
        sslmode = db_config.get("ssl_mode", "prefer")
        connect_timeout = db_config.get("connection_timeout", 10)
        
        # URL-encode password to handle special characters
        if password:
            password = quote_plus(password)
        
        connection_string = (
            f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
            f"?sslmode={sslmode}&connect_timeout={connect_timeout}"
        )
        
        return connection_string

    def _execute_with_papermill(
        self,
        input_path: Path,
        output_path: Path,
        state: MetricsForecastNotebooksState,
        output_results_path: Optional[Path] = None,
    ) -> Dict[str, Any]:
        """Execute notebook using papermill with injected parameters.
        
        If output_results_path is set, the notebook may write a JSON file with
        timeseries_processed and timeseries_failed; those are read and returned.
        """
        try:
            import papermill as pm
            import time
            import json

            start_time = time.time()

            # Get environment from job config or environment variable
            environment = os.getenv('VM_JOBS_ENVIRONMENT', '')
            
            # Prepare parameters to inject into notebook
            # Note: All parameters defined in the notebook's parameters cell must be passed
            # to avoid papermill warnings about unknown parameters
            # vm_jobs_config_path: passed so notebook can call create_database_connection(..., config_path=...)
            notebook_parameters = {
                "vm_query_url": state.vm_query_url,
                "vm_token": state.vm_token,
                "vm_jobs_environment": environment,
                "vm_jobs_config_path": state.config_path or "",
                "dry_run": False,  # Default to False for production runs
                "output_results_path": str(output_results_path) if output_results_path else "",
            }

            # Execute notebook with parameters
            # Set working directory to the notebook's directory so imports work correctly
            notebooks_dir = input_path.parent
            original_cwd = os.getcwd()
            try:
                os.chdir(str(notebooks_dir))
                # Execute notebook with parameters
                # Note: Papermill 2.6.0+ may show warnings about "unknown parameters" even when
                # parameters are correctly defined. These are typically warnings, not errors.
                # The parameters are still injected correctly into the notebook.
                import warnings
                with warnings.catch_warnings():
                    # Suppress papermill parameter warnings if they occur
                    warnings.filterwarnings('ignore', message='.*unknown.*parameter.*', category=UserWarning)
                    
                    # Prepare execute_notebook arguments (nbclient Integer trait expects int, not float)
                    execute_kwargs = {
                        'input_path': str(input_path),
                        'output_path': str(output_path),
                        'parameters': notebook_parameters,
                        'kernel_name': 'python3',  # Explicitly specify kernel to avoid "no kernel name" error
                        'log_output': True,
                        'stdout_file': None,  # Don't capture stdout
                        'stderr_file': None,  # Don't capture stderr
                        'start_timeout': int(state.papermill_start_timeout),  # Timeout for kernel startup (seconds)
                    }
                    
                    # Add execution_timeout only if specified (None means no timeout)
                    if state.papermill_execution_timeout is not None:
                        execute_kwargs['execution_timeout'] = int(state.papermill_execution_timeout)
                    
                    self.logger.info(
                        f"Executing notebook with start_timeout={state.papermill_start_timeout}s, "
                        f"execution_timeout={'unlimited' if state.papermill_execution_timeout is None else f'{state.papermill_execution_timeout}s'}"
                    )
                    
                    pm.execute_notebook(**execute_kwargs)
            finally:
                # Restore original working directory
                os.chdir(original_cwd)

            execution_time = time.time() - start_time

            out = {
                "success": True,
                "execution_time": execution_time,
            }
            # Read notebook results file if present (timeseries_processed, timeseries_failed)
            if output_results_path and output_results_path.exists():
                try:
                    with open(output_results_path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    out["timeseries_processed"] = data.get("timeseries_processed")
                    out["timeseries_failed"] = data.get("timeseries_failed")
                except Exception as read_err:
                    self.logger.debug(
                        "Could not read notebook results file %s: %s",
                        output_results_path,
                        read_err,
                    )
            return out

        except ImportError:
            self.logger.error("papermill not installed")
            return {
                "success": False,
                "error": "papermill not installed",
            }
        except Exception as exc:
            self.logger.error("Papermill execution failed: %s", exc)
            return {
                "success": False,
                "error": str(exc),
            }

    def _convert_to_html(self, notebook_path: Path, html_path: Path) -> None:
        """Convert executed notebook to HTML using nbconvert."""
        try:
            from nbconvert import HTMLExporter
            import nbformat

            # Read the executed notebook
            with open(notebook_path, "r", encoding="utf-8") as f:
                notebook_content = nbformat.read(f, as_version=4)

            # Export to HTML using classic template
            html_exporter = HTMLExporter(template_name="classic")
            (body, resources) = html_exporter.from_notebook_node(notebook_content)

            # Write HTML file
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(body)

            self.logger.debug("Generated HTML version: %s", html_path)

        except ImportError as exc:
            self.logger.warning(
                "nbconvert not available, skipping HTML generation: %s", exc
            )
        except Exception as exc:
            self.logger.warning(
                "Failed to convert notebook to HTML: %s", exc, exc_info=True
            )


def main():
    """CLI entry point."""
    epilog = """
Examples:
  # List available job configurations
  python -m victoria_metrics_jobs.jobs.metrics_forecast_notebooks --config victoria_metrics_jobs/victoria_metrics_jobs.yml --list-jobs

  # Run metrics forecast notebooks job
  python -m victoria_metrics_jobs.jobs.metrics_forecast_notebooks --config victoria_metrics_jobs/victoria_metrics_jobs.yml --job-id metrics_forecast_notebooks
    """

    return MetricsForecastNotebooksJob.main(
        description="Metrics Forecast Notebooks Job - Executes Jupyter notebooks for forecasting",
        epilog=epilog,
    )


if __name__ == "__main__":
    sys.exit(main())

