#!/usr/bin/env python3
"""
Metrics Forecast Job - trains Prophet models per metric series and stores
business-day forecasts in a PostgreSQL database.

The job:
1. Derives the current business date
2. Collects historical metrics from VictoriaMetrics
3. Trains Prophet models and generates forecasts
4. Writes forecasts to the database with upsert logic
5. Publishes job status metric to VictoriaMetrics

Database Storage:
- Forecasts are stored in a PostgreSQL 'vm_forecasted_metric' table
- Primary key: (job, biz_date, auid, metric_name, forecast_type)
- Re-running forecasts overwrites existing values (idempotent)
- Table is partitioned by job for better performance
"""

from __future__ import annotations

import gc
import json
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
from prophet import Prophet
from prometheus_api_client import PrometheusConnect
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from urllib.parse import quote_plus

# Add the scheduler module to the path for imports shared with other jobs
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from victoria_metrics_jobs.jobs.common import BaseJob, BaseJobState, Err, Ok, Result


@dataclass
class SeriesHistory:
    """Container for a single metric series history."""

    metric_name: str
    labels: Dict[str, str]
    samples: List[Tuple[datetime, float]]
    selection_value: Optional[str] = None  # PromQL selector string


@dataclass
class MetricsForecastState(BaseJobState):
    """State object for the metrics_forecast job (DB-driven mode)."""

    current_business_date: Optional[date] = None
    history_days: int = 365
    history_offset_days: int = 0
    history_step_hours: int = 24
    forecast_horizon_days: int = 20
    forecast_types: List[Dict[str, str]] = field(default_factory=list)
    min_history_points: int = 30
    vm_query_url: str = ""
    vm_gateway_url: str = ""
    vm_token: str = ""
    series_processed: int = 0
    forecasts_written: int = 0
    failed_series: int = 0
    prom_client: Optional[PrometheusConnect] = None
    forecast_db_config: Dict[str, Any] = field(default_factory=dict)
    db_engine: Optional[Engine] = None
    db_connection: Optional[Any] = None
    # DB-driven fields (no longer use YAML for Prophet config or selections)
    source_job_names: List[str] = field(default_factory=list)  # Legacy compatibility
    metric_selectors: List[str] = field(default_factory=list)  # Legacy compatibility
    prophet_config: Dict[str, Any] = field(default_factory=dict)  # Legacy compatibility
    prophet_fit_kwargs: Dict[str, Any] = field(default_factory=dict)  # Legacy compatibility

    def to_results(self) -> Dict[str, Any]:
        """Extend base results with forecasting metadata (DB-driven mode)."""
        results = super().to_results()
        results.update(
            {
                "series_processed": self.series_processed,
                "forecasts_written": self.forecasts_written,
                "failed_series": self.failed_series,
                "current_business_date": self.current_business_date.isoformat()
                if self.current_business_date
                else None,
                "mode": "database_driven",
            }
        )
        return results


class MetricsForecastJob(BaseJob):
    """Metrics Forecast job orchestrating Prophet training and publishing."""

    def __init__(self, config_path: str = None, verbose: bool = False):
        super().__init__("metrics_forecast", config_path, verbose)

    def create_initial_state(self, job_id: str) -> Result[MetricsForecastState, Exception]:
        try:
            job_config = self.get_job_config(job_id)

            # Forecast types configuration
            forecast_types = job_config.get("forecast_types") or [
                {"name": "trend", "field": "yhat"},
                {"name": "lower", "field": "yhat_lower"},
                {"name": "upper", "field": "yhat_upper"},
            ]

            victoria_metrics_cfg = job_config.get("victoria_metrics", {})

            # Get database configuration - check for forecast_database first, fallback to common database
            forecast_db_config = job_config.get("forecast_database")
            if not forecast_db_config:
                # Try to get common database config
                forecast_db_config = job_config.get("database")
            
            if not forecast_db_config:
                raise ValueError(
                    "Database configuration required for forecast storage. "
                    "Specify 'forecast_database' in job config or 'database' in common config."
                )

            state = MetricsForecastState(
                job_id=job_id,
                job_config=job_config,
                started_at=datetime.now(),
                source_job_names=[],  # No longer used - DB-driven
                metric_selectors=[],  # No longer used - DB-driven
                history_days=int(job_config.get("history_days", 365)),
                history_offset_days=int(job_config.get("history_offset_days", 0)),
                history_step_hours=max(1, int(job_config.get("history_step_hours", 24))),
                forecast_horizon_days=int(job_config.get("forecast_horizon_days", 20)),
                forecast_types=forecast_types,
                min_history_points=int(job_config.get("min_history_points", 30)),
                prophet_config={},  # No longer used - DB-driven
                prophet_fit_kwargs={},  # No longer used - DB-driven
                vm_query_url=victoria_metrics_cfg.get("query_url", ""),
                vm_gateway_url=victoria_metrics_cfg.get("gateway_url", ""),
                vm_token=victoria_metrics_cfg.get("token", ""),
                forecast_db_config=forecast_db_config,
            )

            self.logger.info(
                "Initialized metrics_forecast job in DB-driven mode. "
                "All Prophet configs and selections will be loaded from vm_forecast_config table."
            )

            return Ok(state)
        except Exception as exc:
            return Err(exc)

    def get_workflow_steps(self) -> List[Callable]:
        return [
            self._derive_current_business_date,
            self._process_forecast_configs,
            self._publish_job_status_metric,
        ]

    def finalize_state(self, state: MetricsForecastState) -> MetricsForecastState:
        # Close database connection before finalizing
        self._close_database_connection(state)
        
        state.completed_at = datetime.now()
        if state.failed_series > 0 and state.forecasts_written == 0:
            state.status = "error"
            state.message = "Forecasting failed for all series"
        elif state.failed_series > 0:
            state.status = "partial_success"
            state.message = (
                f"Forecasts published with warnings: "
                f"{state.series_processed} processed, {state.failed_series} failed"
            )
        elif state.series_processed == 0:
            state.status = "success"
            state.message = "No matching series to forecast"
        else:
            state.status = "success"
            state.message = (
                f"Forecasts published for {state.series_processed} series "
                f"({state.forecasts_written} samples)"
            )
        return state

    # Step 1: Determine the business date anchor for the run
    def _derive_current_business_date(
        self, state: MetricsForecastState
    ) -> Result[MetricsForecastState, Exception]:
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

    # Step 2: Process all forecast configurations from database (DB-driven workflow)
    def _process_forecast_configs(
        self, state: MetricsForecastState
    ) -> Result[MetricsForecastState, Exception]:
        """Load forecast configs from DB and process each one end-to-end.
        
        This is the main DB-driven workflow that:
        1. Loads all enabled configs from vm_forecast_config table
        2. For each config:
           - Creates a forecast run record
           - Queries matching metric series
           - Forecasts each series with the config's Prophet parameters
           - Saves forecasts to database
        
        No YAML configuration needed - everything driven by database.
        """
        try:
            # Get database connection
            conn = self._get_database_connection(state)
            if not conn:
                raise ValueError("Database connection required for DB-driven forecasting")
            
            # Get Prometheus client for querying metrics
            prom = self._get_prometheus_client(state)
            if prom is None:
                raise ValueError("Prometheus client could not be initialized")
            
            # Load all enabled forecast configurations from database for this job
            self.logger.info(
                "Loading forecast configurations from database for job_id='%s'...",
                state.job_id
            )
            query = text("""
                SELECT 
                    config_id,
                    selection_value,
                    prophet_params,
                    prophet_fit_params,
                    history_days,
                    history_offset_days,
                    history_step_hours,
                    forecast_horizon_days,
                    min_history_points,
                    cutoff_hour,
                    notes
                FROM vm_forecast_config
                WHERE job_id = :job_id
                  AND enabled = true
                ORDER BY config_id
            """)
            
            result = conn.execute(query, {"job_id": state.job_id})
            config_rows = result.fetchall()
            
            if not config_rows:
                self.logger.warning(
                    "No enabled forecast configurations found in vm_forecast_config table "
                    "for job_id='%s'. Add configurations with matching job_id to start forecasting.",
                    state.job_id
                )
                return Ok(state)
            
            self.logger.info(
                "Found %s enabled forecast configuration(s) to process",
                len(config_rows)
            )
            
            # Process each configuration
            for config_row in config_rows:
                config_id = config_row[0]
                selection_value = config_row[1]
                prophet_params = dict(config_row[2]) if config_row[2] else {}
                prophet_fit_params = dict(config_row[3]) if config_row[3] else {}
                history_days = config_row[4] if config_row[4] is not None else state.history_days
                history_offset_days = config_row[5] if config_row[5] is not None else state.history_offset_days
                history_step_hours = config_row[6] if config_row[6] is not None else state.history_step_hours
                forecast_horizon_days = config_row[7] if config_row[7] is not None else state.forecast_horizon_days
                min_history_points = config_row[8] if config_row[8] is not None else state.min_history_points
                cutoff_hour = config_row[9] if config_row[9] is not None else state.job_config.get("cutoff_hour", 6)
                notes = config_row[10]
                
                self.logger.info(
                    "Processing config %s: selector='%s' (notes: %s)",
                    config_id,
                    selection_value,
                    notes or "none"
                )
                
                try:
                    # Create forecast run record for this configuration
                    run_id = self._create_forecast_run_record(
                        state,
                        selection_value,
                        prophet_params,
                        prophet_fit_params,
                        selection_value,
                        history_days,
                        history_offset_days,
                        history_step_hours,
                        forecast_horizon_days,
                        min_history_points,
                    )
                    
                    if not run_id:
                        self.logger.warning(
                            "Failed to create run record for config %s, skipping",
                            config_id
                        )
                        continue
                    
                    # Query metric series for this selector
                    series_list = self._query_series_for_selection(
                        state,
                        prom,
                        selection_value,
                        history_days,
                        history_offset_days,
                        history_step_hours,
                        cutoff_hour
                    )
                    
                    if not series_list:
                        self.logger.info(
                            "No series found for selector='%s'",
                            selection_value
                        )
                        continue
                    
                    self.logger.info(
                        "Found %s series for selector='%s', starting forecasts...",
                        len(series_list),
                        selection_value
                    )
                    
                    # Forecast each series with this configuration
                    for series_idx, series in enumerate(series_list):
                        try:
                            # Small delay between series to avoid resource contention
                            if series_idx > 0 and series_idx % 10 == 0:
                                time.sleep(0.5)
                            
                            rows_written = self._forecast_single_series(
                                state,
                                series,
                                prophet_params,
                                prophet_fit_params,
                                run_id,
                                forecast_horizon_days,
                                min_history_points
                            )
                            
                            if rows_written > 0:
                                state.forecasts_written += rows_written
                                state.series_processed += 1
                            
                        except Exception as series_exc:
                            state.failed_series += 1
                            self.logger.error(
                                "Failed to forecast series %s: %s",
                                series.metric_name,
                                series_exc
                            )
                    
                    self.logger.info(
                        "Completed config %s: processed %s series",
                        config_id,
                        len(series_list)
                    )
                    
                except Exception as config_exc:
                    self.logger.error(
                        "Failed to process config %s (selector='%s'): %s",
                        config_id,
                        selection_value,
                        config_exc
                    )
                    continue
            
            self.logger.info(
                "Forecast processing complete: %s series processed, %s forecasts written, %s failed",
                state.series_processed,
                state.forecasts_written,
                state.failed_series
            )
            
            return Ok(state)
            
        except Exception as exc:
            self.logger.error("Failed to process forecast configurations: %s", exc)
            return Err(exc)

    # OLD Step 2: Load per-selection Prophet configurations from database
    def _load_selection_configs(
        self, state: MetricsForecastState
    ) -> Result[MetricsForecastState, Exception]:
        """Load per-selection Prophet configuration from database.
        
        Loads configuration overrides for specific jobs or selectors from the
        vm_forecast_config table. These configs override default YAML settings
        for specific metric selections.
        
        Format of selection_configs:
            {('job', 'extractor'): {'prophet_params': {...}, 'prophet_fit_params': {...}}}
        """
        try:
            # Try to get database connection - fail gracefully if not available
            conn = self._get_database_connection(state)
            if not conn:
                self.logger.warning(
                    "Database connection not available - using default Prophet config for all selections"
                )
                return Ok(state)
            
            # Query for enabled configurations
            query = text("""
                SELECT selection_type, selection_value, prophet_params, prophet_fit_params
                FROM vm_forecast_config
                WHERE enabled = true
            """)
            
            try:
                result = conn.execute(query)
                rows = result.fetchall()
                
                # Parse and store configurations
                for row in rows:
                    selection_type = row[0]
                    selection_value = row[1]
                    prophet_params = row[2]  # Already parsed as dict by JSONB
                    prophet_fit_params = row[3]  # Already parsed as dict by JSONB
                    
                    config_key = (selection_type, selection_value)
                    config_value = {}
                    
                    if prophet_params:
                        config_value['prophet_params'] = dict(prophet_params)
                    if prophet_fit_params:
                        config_value['prophet_fit_params'] = dict(prophet_fit_params)
                    
                    state.selection_configs[config_key] = config_value
                    
                    self.logger.info(
                        "Loaded custom config for %s='%s': %s",
                        selection_type,
                        selection_value,
                        config_value,
                    )
                
                if state.selection_configs:
                    self.logger.info(
                        "Loaded %s custom Prophet configurations from database",
                        len(state.selection_configs),
                    )
                else:
                    self.logger.info(
                        "No custom Prophet configurations found - using defaults for all selections"
                    )
                
            except Exception as query_exc:
                # If table doesn't exist or query fails, log warning and continue with defaults
                self.logger.warning(
                    "Failed to load selection configs from database (table may not exist): %s. "
                    "Using default config for all selections.",
                    query_exc,
                )
            
            return Ok(state)
            
        except Exception as exc:
            # Non-critical error - log and continue with default configs
            self.logger.warning(
                "Error loading selection configs: %s. Using default config for all selections.",
                exc,
            )
            return Ok(state)

    # Step 3: Create forecast run records upfront based on configuration
    def _create_forecast_runs(
        self, state: MetricsForecastState
    ) -> Result[MetricsForecastState, Exception]:
        """Create forecast run records for each selection before collecting series.
        
        This creates run records upfront based on what we're configured to query,
        rather than discovering unique selections after series collection.
        
        Benefits:
        - No need to group/deduplicate series later
        - Run records exist even if query returns no series
        - Simpler, more predictable code
        """
        try:
            # Determine selections based on configuration
            if state.source_job_names:
                # Create run record for each configured job
                for job_name in state.source_job_names:
                    config_key = ("job", job_name)
                    selection_config = state.selection_configs.get(config_key, {})
                    
                    # Merge prophet config (defaults + per-selection)
                    prophet_config = dict(state.prophet_config)
                    if 'prophet_params' in selection_config:
                        prophet_config.update(selection_config['prophet_params'])
                    
                    # Merge prophet fit config
                    prophet_fit_config = dict(state.prophet_fit_kwargs)
                    if 'prophet_fit_params' in selection_config:
                        prophet_fit_config.update(selection_config['prophet_fit_params'])
                    
                    # Determine config source
                    config_source = f"job={job_name}" if selection_config else "default"
                    
                    # Create run record
                    run_id = self._create_forecast_run_record(
                        state,
                        "job",
                        job_name,
                        prophet_config,
                        prophet_fit_config,
                        config_source,
                    )
                    
                    if run_id:
                        state.forecast_run_ids[config_key] = run_id
                        self.logger.info(
                            "Created forecast run %s for job='%s' with config_source='%s'",
                            run_id,
                            job_name,
                            config_source,
                        )
                
                self.logger.info(
                    "Created %s forecast run record(s) for source jobs",
                    len(state.forecast_run_ids),
                )
            
            else:
                # Create run record for each configured selector
                for selector in state.metric_selectors:
                    config_key = ("selector", selector)
                    selection_config = state.selection_configs.get(config_key, {})
                    
                    # Merge prophet config
                    prophet_config = dict(state.prophet_config)
                    if 'prophet_params' in selection_config:
                        prophet_config.update(selection_config['prophet_params'])
                    
                    # Merge prophet fit config
                    prophet_fit_config = dict(state.prophet_fit_kwargs)
                    if 'prophet_fit_params' in selection_config:
                        prophet_fit_config.update(selection_config['prophet_fit_params'])
                    
                    # Determine config source
                    config_source = f"selector={selector}" if selection_config else "default"
                    
                    # Create run record
                    run_id = self._create_forecast_run_record(
                        state,
                        "selector",
                        selector,
                        prophet_config,
                        prophet_fit_config,
                        config_source,
                    )
                    
                    if run_id:
                        state.forecast_run_ids[config_key] = run_id
                        self.logger.info(
                            "Created forecast run %s for selector='%s' with config_source='%s'",
                            run_id,
                            selector,
                            config_source,
                        )
                
                self.logger.info(
                    "Created %s forecast run record(s) for selectors",
                    len(state.forecast_run_ids),
                )
            
            return Ok(state)
            
        except Exception as exc:
            self.logger.error("Failed to create forecast runs: %s", exc)
            return Err(exc)

    # Step 4: Collect historical samples per series
    def _collect_metric_histories(
        self, state: MetricsForecastState
    ) -> Result[MetricsForecastState, Exception]:
        try:
            if not state.current_business_date:
                raise ValueError("current_business_date must be computed first")

            prom = self._get_prometheus_client(state)
            if prom is None:
                raise ValueError("Prometheus client could not be initialized")

            history_end = state.current_business_date - timedelta(days=state.history_offset_days)
            history_start = history_end - timedelta(days=state.history_days)
            state.history_start_date = history_start
            state.history_end_date = history_end

            start_dt = datetime.combine(history_start, datetime.min.time()).replace(tzinfo=timezone.utc)
            end_dt = datetime.combine(history_end, datetime.max.time()).replace(tzinfo=timezone.utc)
            step_str = f"{state.history_step_hours}h"

            collected: List[SeriesHistory] = []

            # Selection logic: use EITHER source_job_names OR metric_selectors (not both)
            # Priority: source_job_names first, then metric_selectors
            if state.source_job_names:
                # Query by job names only (using wildcard selector with job label filter)
                for job_name in state.source_job_names:
                    query = self._build_metric_selector(
                        "{__name__!=\"\"}", "job", job_name
                    )
                    self.logger.debug("Executing query for job=%s: %s", job_name, query)
                    query_result = prom.custom_query_range(
                        query=query,
                        start_time=start_dt,
                        end_time=end_dt,
                        step=step_str,
                    )
                    # Track selection source for Prophet config lookup
                    series = self._parse_range_query(
                        query_result, 
                        selection_type="job", 
                        selection_value=job_name
                    )
                    collected.extend(series)
                
                self.logger.info(
                    "Collected %s metric series from %s job(s)",
                    len(collected),
                    len(state.source_job_names),
                )
            else:
                # Query by metric selectors only (use selectors as-is, no modification)
                for selector in state.metric_selectors:
                    # Normalize selector: PromQL requires double quotes for label values
                    # Replace single quotes with double quotes for label matchers
                    query = selector.strip().replace("'", '"')
                    self.logger.debug("Executing selector query: %s", query)
                    query_result = prom.custom_query_range(
                        query=query,
                        start_time=start_dt,
                        end_time=end_dt,
                        step=step_str,
                    )
                    # Track selection source for Prophet config lookup
                    series = self._parse_range_query(
                        query_result,
                        selection_type="selector",
                        selection_value=selector
                    )
                    collected.extend(series)
                
                self.logger.info(
                    "Collected %s metric series from %s selector(s)",
                    len(collected),
                    len(state.metric_selectors),
                )

            state.series_histories = collected

            return Ok(state)
        except Exception as exc:
            self.logger.error("Failed to collect metric histories: %s", exc)
            return Err(exc)

    # Step 5: Train Prophet models and write forecasts to database
    def _forecast_and_publish(
        self, state: MetricsForecastState
    ) -> Result[MetricsForecastState, Exception]:
        try:
            if not state.series_histories:
                self.logger.warning("No metric series discovered for forecasting")
                return Ok(state)

            # Initialize database connection
            conn = self._get_database_connection(state)
            if conn is None:
                raise ValueError("Database connection could not be established")
            
            # Still need Prometheus client for history collection (already done in step 2)
            prom = self._get_prometheus_client(state)
            if prom is None:
                raise ValueError("Prometheus client could not be initialized")

            # Note: Forecast run records are already created in _create_forecast_runs step
            # Each series should already have a run_id available in state.forecast_run_ids

            for series_idx, series in enumerate(state.series_histories):
                try:
                    # Add small delay between series to avoid resource contention
                    if series_idx > 0:
                        time.sleep(0.5)  # 500ms delay between series
                    
                    training_df = self._prepare_training_frame(series.samples)
                    if len(training_df) < state.min_history_points:
                        self.logger.info(
                            "Skipping %s due to insufficient history (%s < %s)",
                            series.metric_name,
                            len(training_df),
                            state.min_history_points,
                        )
                        continue
                    
                    # Validate training data before fitting
                    if training_df.empty or training_df["y"].isna().all():
                        self.logger.warning(
                            "Skipping %s: training data is empty or all NaN",
                            series.metric_name,
                        )
                        continue
                    
                    # Check for infinite or extremely large values that could crash Stan
                    if np.isinf(training_df["y"]).any():
                        self.logger.warning(
                            "Skipping %s: training data contains infinite values",
                            series.metric_name,
                        )
                        continue
                    
                    # Prophet can handle NaNs by dropping those rows, but we need enough valid points
                    # Count non-NaN values to ensure we have sufficient data after Prophet drops NaNs
                    valid_points = training_df["y"].notna().sum()
                    if valid_points < state.min_history_points:
                        self.logger.warning(
                            "Skipping %s: insufficient valid data points (%s < %s) after accounting for NaNs",
                            series.metric_name,
                            valid_points,
                            state.min_history_points,
                        )
                        continue
                    
                    # Warn about very large datasets that might cause memory issues
                    if len(training_df) > 10000:
                        self.logger.warning(
                            "Large dataset for %s (%s points) may cause memory issues with cmdstanpy. "
                            "Consider reducing history_days if crashes occur.",
                            series.metric_name,
                            len(training_df),
                        )
                    
                    # Log if there are NaNs (Prophet will drop them automatically)
                    nan_count = training_df["y"].isna().sum()
                    if nan_count > 0:
                        self.logger.debug(
                            "Series %s has %s NaN values (out of %s total); Prophet will drop these rows",
                            series.metric_name,
                            nan_count,
                            len(training_df),
                        )
                    
                    # Additional data validation
                    # Check for constant values which can cause Stan compilation issues
                    valid_y = training_df["y"].dropna()
                    if len(valid_y) > 0:
                        value_std = valid_y.std()
                        if value_std == 0 or np.isnan(value_std):
                            self.logger.warning(
                                "Skipping %s: constant or invalid values (std=%s) may cause Stan issues",
                                series.metric_name,
                                value_std,
                            )
                            continue
                        
                        # Check for extreme value ranges that might cause numerical issues
                        value_range = valid_y.max() - valid_y.min()
                        if value_range > 1e10:
                            self.logger.warning(
                                "Skipping %s: extremely large value range (%s) may cause numerical instability",
                                series.metric_name,
                                value_range,
                            )
                            continue

                    # Get merged Prophet fit kwargs (defaults + per-selection overrides)
                    prophet_fit_kwargs = dict(state.prophet_fit_kwargs)
                    if series.selection_type and series.selection_value:
                        config_key = (series.selection_type, series.selection_value)
                        selection_config = state.selection_configs.get(config_key)
                        if selection_config and 'prophet_fit_params' in selection_config:
                            prophet_fit_kwargs.update(selection_config['prophet_fit_params'])

                    model = self._create_prophet_model(state, series)
                    
                    # Fit Prophet model with retry logic for cmdstanpy stability
                    max_retries = 3
                    fit_success = False
                    last_error = None
                    
                    for attempt in range(max_retries):
                        try:
                            model.fit(training_df, **prophet_fit_kwargs)
                            fit_success = True
                            break
                        except Exception as fit_error:
                            last_error = fit_error
                            error_msg = str(fit_error)
                            
                            if attempt < max_retries - 1:
                                wait_seconds = (2 ** attempt) + 1  # Exponential backoff, min 2s
                                self.logger.warning(
                                    "Prophet fit failed (attempt %s/%s) for %s: %s. "
                                    "Retrying in %s seconds...",
                                    attempt + 1,
                                    max_retries,
                                    series.metric_name,
                                    error_msg[:200],
                                    wait_seconds,
                                )
                                time.sleep(wait_seconds)
                                
                                # Cleanup before retry
                                del model
                                gc.collect()
                                time.sleep(0.5)  # Additional small delay for cleanup
                                
                                # Recreate model for retry to clear any corrupted state
                                model = self._create_prophet_model(state, series)
                            else:
                                self.logger.error(
                                    "Prophet fit failed after %s attempts for %s: %s",
                                    max_retries,
                                    series.metric_name,
                                    error_msg[:200],
                                )
                                raise
                    
                    if not fit_success:
                        raise Exception(f"Prophet fit failed after {max_retries} attempts: {last_error}")

                    last_history_date = training_df["ds"].max().date()
                    future_dates = self._future_business_dates(
                        last_history_date, state.forecast_horizon_days
                    )
                    if not future_dates:
                        continue

                    future_df = pd.DataFrame({"ds": future_dates})
                    forecast_df = model.predict(future_df)

                    # Get forecast run_id for this series
                    forecast_run_id = None
                    if series.selection_type and series.selection_value:
                        config_key = (series.selection_type, series.selection_value)
                        forecast_run_id = state.forecast_run_ids.get(config_key)

                    # Write forecasts to database with run reference
                    rows_written = self._write_forecasts_to_database(
                        state, series, forecast_df, forecast_run_id
                    )
                    
                    if rows_written > 0:
                        state.forecasts_written += rows_written

                    state.series_processed += 1
                    
                    # Clean up model to free memory
                    del model
                    gc.collect()

                except Exception as series_exc:
                    state.failed_series += 1
                    self.logger.error(
                        "Failed to forecast series %s labels=%s: %s",
                        series.metric_name,
                        series.labels,
                        series_exc,
                    )
                    # Clean up memory on failure
                    gc.collect()

            return Ok(state)
        except Exception as exc:
            self.logger.error("Failed to generate forecasts: %s", exc)
            return Err(exc)

    # Step 6: Publish status metric for observability
    def _publish_job_status_metric(
        self, state: MetricsForecastState
    ) -> Result[MetricsForecastState, Exception]:
        try:
            if not state.vm_gateway_url:
                return Ok(state)

            status_value = 1 if state.status == "success" else 0
            timestamp = int(datetime.utcnow().timestamp())

            env = state.job_config.get("env", "default")
            labels_cfg = state.job_config.get("labels", {})
            label_pairs = [
                f'job_id="{state.job_id}"',
                f'status="{state.status}"',
                f'env="{env}"',
            ]
            for key, value in labels_cfg.items():
                label_pairs.append(f'{key}="{value}"')

            metric_line = (
                f'metrics_forecast_job_status{{{",".join(label_pairs)}}} {status_value} {timestamp}'
            )

            self._write_metric_to_vm(state, metric_line, timeout=30)
            return Ok(state)
        except Exception as exc:
            self.logger.warning("Failed to publish job status metric: %s", exc)
            return Ok(state)

    # Helpers -----------------------------------------------------------------
    def _normalize_list(self, value: Any) -> List[str]:
        if value is None:
            return []
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        return []

    def _parse_range_query(
        self, 
        query_result: Any,
        selection_value: Optional[str] = None
    ) -> List[SeriesHistory]:
        """Parse Prometheus range query response into SeriesHistory objects.
        
        Args:
            query_result: Prometheus query result
            selection_value: PromQL selector string
            
        Returns:
            List of SeriesHistory objects with selection tracking
        """
        if not query_result:
            return []

        if isinstance(query_result, dict):
            if query_result.get("status") != "success":
                self.logger.warning("Prometheus query unsuccessful")
                return []
            data = query_result.get("data", {})
            raw_series = data.get("result", [])
        else:
            raw_series = query_result

        histories: List[SeriesHistory] = []
        for item in raw_series:
            metric = item.get("metric", {})
            metric_name = metric.get("__name__")
            if not metric_name:
                continue

            labels = {k: v for k, v in metric.items() if k != "__name__"}
            values = item.get("values", []) or []
            samples: List[Tuple[datetime, float]] = []

            for value_pair in values:
                if not isinstance(value_pair, (list, tuple)) or len(value_pair) < 2:
                    continue
                ts_raw, value_raw = value_pair[:2]
                try:
                    ts = datetime.fromtimestamp(float(ts_raw), tz=timezone.utc)
                    samples.append((ts, float(value_raw)))
                except Exception:
                    continue

            if samples:
                histories.append(
                    SeriesHistory(
                        metric_name=metric_name,
                        labels=labels,
                        samples=samples,
                        selection_value=selection_value,
                    )
                )

        return histories

    def _query_series_for_selection(
        self,
        state: MetricsForecastState,
        prom: PrometheusConnect,
        selection_value: str,
        history_days: int,
        history_offset_days: int,
        history_step_hours: int,
        cutoff_hour: int
    ) -> List[SeriesHistory]:
        """Query metric series for a specific PromQL selector.
        
        Args:
            state: Job state
            prom: Prometheus client
            selection_value: PromQL selector string
            history_days: Days of history to fetch
            history_offset_days: Days to skip at end of history window
            history_step_hours: Sampling interval in hours
            cutoff_hour: Hour (UTC) for business date cutoff
            
        Returns:
            List of SeriesHistory objects
        """
        try:
            history_end = state.current_business_date - timedelta(days=history_offset_days)
            history_start = history_end - timedelta(days=history_days)
            
            start_dt = datetime.combine(history_start, datetime.min.time()).replace(tzinfo=timezone.utc)
            end_dt = datetime.combine(history_end, datetime.max.time()).replace(tzinfo=timezone.utc)
            step_str = f"{history_step_hours}h"
            
            # Use selector as-is (complete PromQL query)
            query = selection_value.strip().replace("'", '"')
            
            self.logger.debug("Executing query: %s", query)
            
            query_result = prom.custom_query_range(
                query=query,
                start_time=start_dt,
                end_time=end_dt,
                step=step_str,
            )
            
            series_list = self._parse_range_query(
                query_result,
                selection_value=selection_value
            )
            
            return series_list
            
        except Exception as exc:
            self.logger.error(
                "Failed to query series for selector '%s': %s",
                selection_value,
                exc
            )
            return []

    def _forecast_single_series(
        self,
        state: MetricsForecastState,
        series: SeriesHistory,
        prophet_params: Dict[str, Any],
        prophet_fit_params: Dict[str, Any],
        run_id: int,
        forecast_horizon_days: int,
        min_history_points: int
    ) -> int:
        """Forecast a single series with given Prophet parameters.
        
        Args:
            state: Job state
            series: Series to forecast
            prophet_params: Prophet model parameters
            prophet_fit_params: Prophet fit parameters
            run_id: Forecast run ID to reference
            forecast_horizon_days: Number of business days to forecast ahead
            min_history_points: Minimum data points required
            
        Returns:
            Number of forecast rows written
        """
        try:
            # Prepare training data
            training_df = self._prepare_training_frame(series.samples)
            if len(training_df) < min_history_points:
                self.logger.debug(
                    "Skipping %s: insufficient history (%s < %s)",
                    series.metric_name,
                    len(training_df),
                    min_history_points,
                )
                return 0
            
            # Validate training data
            if training_df.empty or training_df["y"].isna().all():
                self.logger.debug("Skipping %s: empty or all NaN", series.metric_name)
                return 0
            
            if np.isinf(training_df["y"]).any():
                self.logger.debug("Skipping %s: contains infinite values", series.metric_name)
                return 0
            
            valid_points = training_df["y"].notna().sum()
            if valid_points < min_history_points:
                self.logger.debug(
                    "Skipping %s: insufficient valid points (%s < %s)",
                    series.metric_name,
                    valid_points,
                    min_history_points,
                )
                return 0
            
            # Create and fit Prophet model
            model = Prophet(**prophet_params)
            if not hasattr(model, "stan_backend"):
                model.stan_backend = None
            
            model.fit(training_df, **prophet_fit_params)
            
            # Generate forecast
            last_history_date = training_df["ds"].max().date()
            future_dates = self._future_business_dates(
                last_history_date,
                forecast_horizon_days
            )
            
            if not future_dates:
                return 0
            
            future_df = pd.DataFrame({"ds": future_dates})
            forecast_df = model.predict(future_df)
            
            # Write forecasts to database
            rows_written = self._write_forecasts_to_database(
                state,
                series,
                forecast_df,
                run_id
            )
            
            # Clean up
            del model
            gc.collect()
            
            return rows_written
            
        except Exception as exc:
            self.logger.error(
                "Failed to forecast %s: %s",
                series.metric_name,
                exc
            )
            return 0

    def _prepare_training_frame(
        self, samples: Sequence[Tuple[datetime, float]]
    ) -> pd.DataFrame:
        """Convert raw samples into a business-day indexed DataFrame."""
        if not samples:
            return pd.DataFrame(columns=["ds", "y"])

        df = pd.DataFrame(samples, columns=["ds", "y"])
        df["ds"] = pd.to_datetime(df["ds"], utc=True).dt.tz_localize(None)
        df["date"] = df["ds"].dt.date
        # Keep the latest value per business day
        daily = (
            df.groupby("date")
            .agg({"ds": "max", "y": "last"})
            .reset_index(drop=True)
        )
        if daily.empty:
            return pd.DataFrame(columns=["ds", "y"])

        daily["ds"] = pd.to_datetime(daily["ds"])
        start = daily["ds"].min()
        end = daily["ds"].max()
        all_business_days = pd.bdate_range(start=start, end=end)

        daily = (
            daily.set_index("ds")
            .reindex(all_business_days)
            .rename_axis("ds")
            .reset_index()
        )
        daily["y"] = daily["y"].interpolate(method="linear").ffill().bfill()
        return daily[["ds", "y"]]

    def _future_business_dates(self, last_history_date: date, periods: int) -> List[pd.Timestamp]:
        """Produce the next N business-day timestamps after last_history_date."""
        future_dates: List[pd.Timestamp] = []
        candidate = last_history_date
        while len(future_dates) < periods:
            candidate += timedelta(days=1)
            if candidate.weekday() >= 5:  # skip weekends
                continue
            future_dates.append(pd.Timestamp(candidate))
        return future_dates

    def _create_prophet_model(
        self, 
        state: MetricsForecastState, 
        series: Optional[SeriesHistory] = None
    ) -> Prophet:
        """Create Prophet model with config precedence: defaults → YAML → per-selection DB config.
        
        Args:
            state: Job state with default Prophet config and selection configs from DB
            series: Optional series to lookup per-selection config
            
        Returns:
            Prophet model instance with merged configuration
        """
        # Start with default config from YAML
        prophet_config = dict(state.prophet_config)
        config_source = "default"
        
        # Check for per-selection override from database
        if series and series.selection_type and series.selection_value:
            config_key = (series.selection_type, series.selection_value)
            selection_config = state.selection_configs.get(config_key)
            
            if selection_config:
                # Merge per-selection prophet_params over defaults
                if 'prophet_params' in selection_config:
                    prophet_config.update(selection_config['prophet_params'])
                    config_source = f"{series.selection_type}={series.selection_value}"
                    self.logger.debug(
                        "Using custom Prophet config for %s='%s': %s",
                        series.selection_type,
                        series.selection_value,
                        selection_config['prophet_params'],
                    )
        
        # Create model with merged config
        model = Prophet(**prophet_config)
        
        # Some downstream environments ship Prophet builds that forget to set this attribute.
        if not hasattr(model, "stan_backend"):
            model.stan_backend = None  # Prophet.fit() will populate a backend when None
            self.logger.debug("Prophet instance missing stan_backend attribute; initialized to None")
        
        # Log which config is being used
        if series:
            self.logger.info(
                "Created Prophet model for %s (config_source=%s)",
                series.metric_name,
                config_source,
            )
        
        return model

    def _create_forecast_run_record(
        self,
        state: MetricsForecastState,
        selection_value: str,
        prophet_config: Dict[str, Any],
        prophet_fit_config: Dict[str, Any],
        config_source: str,
        history_days: int,
        history_offset_days: int,
        history_step_hours: int,
        forecast_horizon_days: int,
        min_history_points: int,
    ) -> Optional[int]:
        """Create a forecast job run record in the database.
        
        Args:
            state: Job state
            selection_value: PromQL selector string
            prophet_config: Prophet model parameters used
            prophet_fit_config: Prophet fit parameters used
            config_source: Where config came from
            history_days: Days of history used
            history_offset_days: Days offset used
            history_step_hours: Step hours used
            forecast_horizon_days: Forecast horizon used
            min_history_points: Minimum history points required
            
        Returns:
            run_id if successful, None otherwise
        """
        try:
            conn = self._get_database_connection(state)
            if not conn:
                self.logger.warning("Cannot create forecast run record - no database connection")
                return None
            
            insert_sql = text("""
                INSERT INTO vm_forecast_job (
                    job_id,
                    selection_value,
                    prophet_config,
                    prophet_fit_config,
                    config_source,
                    history_days,
                    forecast_horizon_days,
                    min_history_points,
                    business_date,
                    started_at,
                    status
                )
                VALUES (
                    :job_id,
                    :selection_value,
                    CAST(:prophet_config AS jsonb),
                    CAST(:prophet_fit_config AS jsonb),
                    :config_source,
                    :history_days,
                    :forecast_horizon_days,
                    :min_history_points,
                    :business_date,
                    :started_at,
                    :status
                )
                RETURNING run_id
            """)
            
            result = conn.execute(insert_sql, {
                "job_id": state.job_id,
                "selection_value": selection_value,
                "prophet_config": json.dumps(prophet_config),
                "prophet_fit_config": json.dumps(prophet_fit_config) if prophet_fit_config else None,
                "config_source": config_source,
                "history_days": history_days,
                "forecast_horizon_days": forecast_horizon_days,
                "min_history_points": min_history_points,
                "business_date": state.current_business_date,
                "started_at": datetime.utcnow(),
                "status": "running",
            })
            
            conn.commit()
            run_id = result.fetchone()[0]
            
            self.logger.info(
                "Created forecast run record: run_id=%s for selector='%s'",
                run_id,
                selection_value,
            )
            
            return run_id
            
        except Exception as exc:
            self.logger.warning(
                "Failed to create forecast run record for selector='%s': %s",
                selection_value,
                exc,
            )
            return None

    def _build_database_connection_string(self, db_config: Dict[str, Any]) -> str:
        """Build PostgreSQL connection string from config.
        
        Args:
            db_config: Database configuration dictionary
            
        Returns:
            PostgreSQL connection string
        """
        host = db_config.get("host", "localhost")
        port = db_config.get("port", 5432)
        dbname = db_config.get("name", "forecasts")
        user = db_config.get("user", "forecast_user")
        password = db_config.get("password", "")
        sslmode = db_config.get("ssl_mode", "prefer")
        connect_timeout = db_config.get("connection_timeout", 10)
        
        # URL-encode password to handle special characters
        if password:
            password = quote_plus(password)
        
        # Build connection string
        connection_string = (
            f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
            f"?sslmode={sslmode}&connect_timeout={connect_timeout}"
        )
        
        return connection_string

    def _get_database_engine(self, state: MetricsForecastState) -> Optional[Engine]:
        """Get or create SQLAlchemy engine for forecast database.
        
        Args:
            state: Job state with database configuration
            
        Returns:
            SQLAlchemy engine or None if creation fails
        """
        if state.db_engine:
            return state.db_engine
        
        try:
            if not state.forecast_db_config:
                self.logger.error("No database configuration available")
                return None
            
            connection_string = self._build_database_connection_string(state.forecast_db_config)
            
            # Create engine with connection pooling
            state.db_engine = create_engine(
                connection_string,
                pool_size=5,
                max_overflow=10,
                pool_pre_ping=True,
                pool_recycle=3600,
                echo=False,
                future=True
            )
            
            self.logger.info("Database engine created successfully")
            return state.db_engine
            
        except Exception as exc:
            self.logger.error("Failed to create database engine: %s", exc)
            return None

    def _get_database_connection(self, state: MetricsForecastState) -> Optional[Any]:
        """Get or create database connection.
        
        Args:
            state: Job state with database engine
            
        Returns:
            Database connection or None if creation fails
        """
        if state.db_connection:
            return state.db_connection
        
        try:
            engine = self._get_database_engine(state)
            if not engine:
                return None
            
            state.db_connection = engine.connect()
            self.logger.info("Database connection established")
            return state.db_connection
            
        except Exception as exc:
            self.logger.error("Failed to create database connection: %s", exc)
            return None

    def _close_database_connection(self, state: MetricsForecastState) -> None:
        """Close database connection and dispose of engine.
        
        Args:
            state: Job state with database connection and engine
        """
        try:
            if state.db_connection:
                state.db_connection.close()
                state.db_connection = None
                self.logger.info("Database connection closed")
            
            if state.db_engine:
                state.db_engine.dispose()
                state.db_engine = None
                self.logger.info("Database engine disposed")
                
        except Exception as exc:
            self.logger.warning("Error closing database connection: %s", exc)

    def _write_forecasts_to_database(
        self,
        state: MetricsForecastState,
        series: SeriesHistory,
        forecast_df: pd.DataFrame,
        forecast_run_id: Optional[int] = None,
    ) -> int:
        """Write forecast data to database using upsert logic.
        
        Args:
            state: Job state with database connection
            series: Series history containing metric metadata
            forecast_df: Prophet forecast DataFrame with predictions
            forecast_run_id: Optional reference to vm_forecast_job run record
            
        Returns:
            Number of forecast rows written
        """
        try:
            conn = self._get_database_connection(state)
            if not conn:
                raise ValueError("Database connection not available")
            
            # Extract job label (required for database partitioning)
            job = series.labels.get("job")
            if not job:
                self.logger.error(
                    "Missing 'job' label in series %s",
                    series.metric_name,
                )
                return 0
            
            # Extract auid label (optional - use empty string if missing)
            auid = series.labels.get("auid", "")
            
            # Build remaining labels JSON (exclude job, auid, biz_date, forecast)
            excluded_labels = {"job", "auid", "biz_date", "forecast"}
            remaining_labels = {
                k: v for k, v in series.labels.items() 
                if k not in excluded_labels
            }
            metric_labels_json = json.dumps(remaining_labels, sort_keys=True)
            
            # Get current timestamp for all rows
            current_timestamp = datetime.utcnow()
            
            # Prepare rows for batch insert
            rows_to_insert = []
            
            for future_row in forecast_df.itertuples():
                # Use forecast date as-is (biz_date from Prophet forecast)
                forecast_date = future_row.ds.date()
                
                for forecast_type in state.forecast_types:
                    name = forecast_type.get("name")
                    field = forecast_type.get("field")
                    if not name or not field or not hasattr(future_row, field):
                        continue
                    
                    value = getattr(future_row, field)
                    if value is None or np.isnan(value):
                        continue
                    
                    rows_to_insert.append({
                        "job": job,
                        "biz_date": forecast_date,
                        "auid": auid,
                        "metric_name": series.metric_name,
                        "value": float(value),
                        "metric_labels": metric_labels_json,
                        "forecast_type": name,
                        "forecast_run_id": forecast_run_id,
                        "created_at": current_timestamp,
                        "updated_at": current_timestamp,
                    })
            
            if not rows_to_insert:
                self.logger.debug("No forecast rows to write for %s", series.metric_name)
                return 0
            
            # Build PostgreSQL upsert statement
            # ON CONFLICT ... DO UPDATE for idempotent writes
            
            upsert_sql = text("""
                INSERT INTO vm_forecasted_metric (
                    job, biz_date, auid, metric_name, 
                    value, metric_labels, forecast_type,
                    forecast_run_id,
                    created_at, updated_at
                )
                VALUES (
                    :job, :biz_date, :auid, :metric_name,
                    :value, CAST(:metric_labels AS jsonb), :forecast_type,
                    :forecast_run_id,
                    :created_at, :updated_at
                )
                ON CONFLICT (job, biz_date, auid, metric_name, forecast_type)
                DO UPDATE SET
                    value = EXCLUDED.value,
                    metric_labels = EXCLUDED.metric_labels,
                    forecast_run_id = EXCLUDED.forecast_run_id,
                    updated_at = EXCLUDED.updated_at
            """)
            
            # Execute batch insert
            for row in rows_to_insert:
                conn.execute(upsert_sql, row)
            
            conn.commit()
            
            self.logger.info(
                "Wrote %s forecast rows for %s (job=%s, auid=%s)",
                len(rows_to_insert),
                series.metric_name,
                job,
                auid,
            )
            
            return len(rows_to_insert)
            
        except SQLAlchemyError as exc:
            self.logger.error(
                "Database error writing forecasts for %s: %s",
                series.metric_name,
                exc,
            )
            # Rollback on error
            if conn:
                conn.rollback()
            return 0
        except Exception as exc:
            self.logger.error(
                "Failed to write forecasts to database for %s: %s",
                series.metric_name,
                exc,
            )
            return 0

    def _get_prometheus_client(self, state: MetricsForecastState) -> Optional[PrometheusConnect]:
        if state.prom_client:
            return state.prom_client
        headers = {}
        if state.vm_token:
            headers["Authorization"] = f"Bearer {state.vm_token}"
        url = state.vm_query_url or state.vm_gateway_url
        if not url:
            return None
        state.prom_client = PrometheusConnect(url=url, headers=headers, disable_ssl=True)
        return state.prom_client

    def _write_metric_to_vm(
        self, state: MetricsForecastState, metric_line: str, timeout: int = 60
    ) -> bool:
        """Write a single metric line to VictoriaMetrics (used for job status metric)."""
        try:
            if not metric_line:
                return False
            if not state.vm_gateway_url:
                self.logger.error("VM gateway URL not configured")
                return False
            prom = self._get_prometheus_client(state)
            if prom is None:
                return False

            session = prom._session

            headers = {"Content-Type": "text/plain"}
            if state.vm_token:
                headers["Authorization"] = f"Bearer {state.vm_token}"
            response = session.post(
                f"{state.vm_gateway_url}/api/v1/import/prometheus",
                data=metric_line,
                headers=headers,
                timeout=timeout,
            )
            response.raise_for_status()
            return True
        except Exception as exc:
            self.logger.error("Failed to write metric to VM: %s", exc)
            return False


def main():
    """CLI entry point."""
    epilog = """
Examples:
  # List available job configurations
  python -m victoria_metrics_jobs.jobs.metrics_forecast --config victoria_metrics_jobs/victoria_metrics_jobs.yml --list-jobs

  # Run metrics forecast job
  python -m victoria_metrics_jobs.jobs.metrics_forecast --config victoria_metrics_jobs/victoria_metrics_jobs.yml --job-id metrics_forecast
    """

    return MetricsForecastJob.main(
        description="Metrics Forecast Job - Prophet-based forecasts of business-day metrics",
        epilog=epilog,
    )


if __name__ == "__main__":
    sys.exit(main())

