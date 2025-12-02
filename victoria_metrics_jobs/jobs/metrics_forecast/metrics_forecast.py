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
- Forecasts are stored in a PostgreSQL 'vm_forecast' table
- Primary key: (source, biz_date, metric_auid, metric_name, forecast_type)
- Re-running forecasts overwrites existing values (idempotent)
- Table is partitioned by source for better performance
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
    source_value: str
    samples: List[Tuple[datetime, float]]


@dataclass
class MetricsForecastState(BaseJobState):
    """State object for the metrics_forecast job."""

    current_business_date: Optional[date] = None
    history_start_date: Optional[date] = None
    history_end_date: Optional[date] = None
    source_job_names: List[str] = field(default_factory=list)
    source_label: str = "source"
    metric_selectors: List[str] = field(default_factory=list)
    history_days: int = 365
    history_offset_days: int = 0
    history_step_hours: int = 24
    forecast_horizon_days: int = 20
    forecast_types: List[Dict[str, str]] = field(default_factory=list)
    min_history_points: int = 30
    prophet_config: Dict[str, Any] = field(default_factory=dict)
    prophet_fit_kwargs: Dict[str, Any] = field(default_factory=dict)
    vm_query_url: str = ""
    vm_gateway_url: str = ""
    vm_token: str = ""
    series_histories: List[SeriesHistory] = field(default_factory=list)
    series_processed: int = 0
    forecasts_written: int = 0
    failed_series: int = 0
    prom_client: Optional[PrometheusConnect] = None
    forecast_db_config: Dict[str, Any] = field(default_factory=dict)
    db_engine: Optional[Engine] = None
    db_connection: Optional[Any] = None

    def to_results(self) -> Dict[str, Any]:
        """Extend base results with forecasting metadata."""
        results = super().to_results()
        results.update(
            {
                "series_processed": self.series_processed,
                "forecasts_written": self.forecasts_written,
                "failed_series": self.failed_series,
                "current_business_date": self.current_business_date.isoformat()
                if self.current_business_date
                else None,
                "history_window": {
                    "start": self.history_start_date.isoformat()
                    if self.history_start_date
                    else None,
                    "end": self.history_end_date.isoformat()
                    if self.history_end_date
                    else None,
                },
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

            source_job_names = self._normalize_list(job_config.get("source_job_names"))
            if not source_job_names:
                raise ValueError("source_job_names must be configured")

            metric_selectors = self._normalize_list(job_config.get("metric_selectors"))
            if not metric_selectors:
                # Default to wildcard selector; job label restriction will keep it scoped
                metric_selectors = ["{__name__!=\"\"}"]

            forecast_types = job_config.get("forecast_types") or [
                {"name": "trend", "field": "yhat"},
                {"name": "lower", "field": "yhat_lower"},
                {"name": "upper", "field": "yhat_upper"},
            ]

            default_prophet_config = {
                "weekly_seasonality": False,
                "daily_seasonality": False,
                "yearly_seasonality": True,
                "seasonality_mode": "additive",
            }
            prophet_config_input = dict(job_config.get("prophet", {}) or {})
            prophet_fit_kwargs = dict(job_config.get("prophet_fit", {}) or {})

            # Allow legacy configs to place fit-only args inside prophet block
            if "algorithm" in prophet_config_input and "algorithm" not in prophet_fit_kwargs:
                prophet_fit_kwargs["algorithm"] = prophet_config_input.pop("algorithm")
            if "iterations" in prophet_config_input and "iterations" not in prophet_fit_kwargs:
                prophet_fit_kwargs["iterations"] = prophet_config_input.pop("iterations")

            prophet_config = {**default_prophet_config, **prophet_config_input}

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
                source_job_names=source_job_names,
                source_label=job_config.get("source_label", "source"),
                metric_selectors=metric_selectors,
                history_days=int(job_config.get("history_days", 365)),
                history_offset_days=int(job_config.get("history_offset_days", 0)),
                history_step_hours=max(1, int(job_config.get("history_step_hours", 24))),
                forecast_horizon_days=int(job_config.get("forecast_horizon_days", 20)),
                forecast_types=forecast_types,
                min_history_points=int(job_config.get("min_history_points", 30)),
                prophet_config=prophet_config,
                prophet_fit_kwargs=prophet_fit_kwargs,
                vm_query_url=victoria_metrics_cfg.get("query_url", ""),
                vm_gateway_url=victoria_metrics_cfg.get("gateway_url", ""),
                vm_token=victoria_metrics_cfg.get("token", ""),
                forecast_db_config=forecast_db_config,
            )

            return Ok(state)
        except Exception as exc:
            return Err(exc)

    def get_workflow_steps(self) -> List[Callable]:
        return [
            self._derive_current_business_date,
            self._collect_metric_histories,
            self._forecast_and_publish,
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

    # Step 2: Collect historical samples per series
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

            for source_name in state.source_job_names:
                for selector in state.metric_selectors:
                    query = self._build_metric_selector(
                        selector, state.source_label, source_name
                    )
                    query_result = prom.custom_query_range(
                        query=query,
                        start_time=start_dt,
                        end_time=end_dt,
                        step=step_str,
                    )
                    series = self._parse_range_query(
                        query_result, state.source_label, source_name
                    )
                    collected.extend(series)

            state.series_histories = collected
            self.logger.info(
                "Collected %s metric series across %s %s labels",
                len(collected),
                len(state.source_job_names),
                state.source_label,
            )

            return Ok(state)
        except Exception as exc:
            self.logger.error("Failed to collect metric histories: %s", exc)
            return Err(exc)

    # Step 3: Train Prophet models and write forecasts to database
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

                    model = self._create_prophet_model(state)
                    
                    # Fit Prophet model with retry logic for cmdstanpy stability
                    max_retries = 3
                    fit_success = False
                    last_error = None
                    
                    for attempt in range(max_retries):
                        try:
                            model.fit(training_df, **state.prophet_fit_kwargs)
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
                                model = self._create_prophet_model(state)
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

                    # Write forecasts to database (replaces VictoriaMetrics write)
                    rows_written = self._write_forecasts_to_database(
                        state, series, forecast_df
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

    # Step 4: Publish status metric for observability
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

    def _build_metric_selector(
        self, selector: str, label_key: str, label_value: str
    ) -> str:
        """Ensure the desired label is present in the selector (supports $JOB/$SOURCE)."""
        selector = selector.strip()

        for placeholder in ("$JOB", "$SOURCE"):
            if placeholder in selector:
                selector = selector.replace(placeholder, label_value)

        label_pattern = rf'{label_key}\s*=\s*"[^"]*"'

        def ensure_label(label_body: str) -> str:
            if re.search(label_pattern, label_body):
                label_body = re.sub(label_pattern, f'{label_key}="{label_value}"', label_body)
            else:
                label_body = label_body.strip()
                if label_body:
                    label_body = f'{label_body},{label_key}="{label_value}"'
                else:
                    label_body = f'{label_key}="{label_value}"'
            
            # Exclude metrics with forecast label using PromQL regex negative match
            # forecast!~".+" means "forecast does not match any non-empty string"
            # This effectively matches only metrics where forecast label doesn't exist
            if 'forecast!~' not in label_body:
                label_body = f'{label_body},forecast!~".+"'
            
            return label_body

        if "{" in selector and selector.endswith("}"):
            metric_name, label_body = selector.split("{", 1)
            label_body = ensure_label(label_body.rstrip("}"))
            return f"{metric_name}{{{label_body}}}"

        if selector.startswith("{") and selector.endswith("}"):
            label_body = ensure_label(selector.strip("{}"))
            return f"{{{label_body}}}"

        return f'{selector}{{{label_key}="{label_value}"}}'

    def _parse_range_query(
        self, query_result: Any, label_key: str, label_value: str
    ) -> List[SeriesHistory]:
        """Parse Prometheus range query response into SeriesHistory objects."""
        if not query_result:
            return []

        if isinstance(query_result, dict):
            if query_result.get("status") != "success":
                self.logger.warning(
                    "Prometheus query unsuccessful for %s=%s",
                    label_key,
                    label_value,
                )
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
            
            # Skip metrics that have a forecast label (these are generated forecasts)
            # We exclude any metric with forecast label, regardless of its value
            if "forecast" in labels:
                continue
            
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
                        source_value=label_value,
                        samples=samples,
                    )
                )

        return histories

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

    def _create_prophet_model(self, state: MetricsForecastState) -> Prophet:
        model = Prophet(**state.prophet_config)
        # Some downstream environments ship Prophet builds that forget to set this attribute.
        if not hasattr(model, "stan_backend"):
            model.stan_backend = None  # Prophet.fit() will populate a backend when None
            self.logger.debug("Prophet instance missing stan_backend attribute; initialized to None")
        
        return model

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
    ) -> int:
        """Write forecast data to database using upsert logic.
        
        Args:
            state: Job state with database connection
            series: Series history containing metric metadata
            forecast_df: Prophet forecast DataFrame with predictions
            
        Returns:
            Number of forecast rows written
        """
        try:
            conn = self._get_database_connection(state)
            if not conn:
                raise ValueError("Database connection not available")
            
            # Extract required labels
            source = series.labels.get(state.source_label)
            if not source:
                self.logger.error(
                    "Missing '%s' label in series %s",
                    state.source_label,
                    series.metric_name,
                )
                return 0
            
            auid = series.labels.get("auid")
            if not auid:
                self.logger.error(
                    "Missing 'auid' label in series %s",
                    series.metric_name,
                )
                return 0
            
            # Build remaining labels JSON (exclude source, auid, biz_date, forecast)
            excluded_labels = {state.source_label, "auid", "biz_date", "forecast"}
            remaining_labels = {
                k: v for k, v in series.labels.items() 
                if k not in excluded_labels
            }
            metric_labels_json = json.dumps(remaining_labels, sort_keys=True)
            
            # Prepare rows for batch insert
            rows_to_insert = []
            
            for future_row in forecast_df.itertuples():
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
                        "source": source,
                        "biz_date": forecast_date,
                        "metric_auid": auid,
                        "metric_name": series.metric_name,
                        "metric_value": float(value),
                        "metric_labels": metric_labels_json,
                        "forecast_type": name,
                        "created_at": current_timestamp,
                        "updated_at": current_timestamp,
                    })
            
            if not rows_to_insert:
                self.logger.debug("No forecast rows to write for %s", series.metric_name)
                return 0
            
            # Build PostgreSQL upsert statement
            # ON CONFLICT ... DO UPDATE for idempotent writes
            # Client supplies both created_at and updated_at timestamps
            current_timestamp = datetime.utcnow()
            
            upsert_sql = text("""
                INSERT INTO vm_forecast (
                    source, biz_date, metric_auid, metric_name, 
                    metric_value, metric_labels, forecast_type,
                    created_at, updated_at
                )
                VALUES (
                    :source, :biz_date, :metric_auid, :metric_name,
                    :metric_value, CAST(:metric_labels AS jsonb), :forecast_type,
                    :created_at, :updated_at
                )
                ON CONFLICT (source, biz_date, metric_auid, metric_name, forecast_type)
                DO UPDATE SET
                    metric_value = EXCLUDED.metric_value,
                    metric_labels = EXCLUDED.metric_labels,
                    updated_at = EXCLUDED.updated_at
            """)
            
            # Execute batch insert
            for row in rows_to_insert:
                conn.execute(upsert_sql, row)
            
            conn.commit()
            
            self.logger.info(
                "Wrote %s forecast rows for %s (auid=%s)",
                len(rows_to_insert),
                series.metric_name,
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

