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
- Forecasts are stored in PostgreSQL 'vm_metric_data' and 'vm_metric_metadata' tables
- Metadata table (vm_metric_metadata): stores job_idx, metric_id, job_id, metric_name, metric_labels
- Data table (vm_metric_data): stores job_idx, metric_id, metric_timestamp, metric_value
- Primary key: (job_idx, metric_id, metric_timestamp)
- Re-running forecasts overwrites existing values (idempotent)
- Job labels are transformed by adding "_forecast" suffix
"""

from __future__ import annotations

import gc
import json
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
                FROM public.vm_forecast_config
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
                INSERT INTO public.vm_forecast_job (
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

    def _normalize_metric_labels_for_comparison(self, labels: Dict[str, str]) -> str:
        """Normalize metric labels for consistent comparison.
        
        Sorts labels by key and converts to JSON string to ensure consistent
        matching when searching for existing metric_id entries.
        
        Args:
            labels: Dictionary of metric labels
            
        Returns:
            JSON string with sorted keys
        """
        # Sort by key to ensure consistent ordering
        sorted_labels = dict(sorted(labels.items()))
        return json.dumps(sorted_labels, sort_keys=True)

    def _find_or_get_job_idx(
        self, 
        conn: Any, 
        job_id: str
    ) -> Optional[int]:
        """Find existing job_idx for a given job_id.
        
        Returns None if no job_idx exists - the first metric entry will create it automatically.
        
        Args:
            conn: Database connection
            job_id: Job ID to look up
            
        Returns:
            job_idx value if found, None if not found (will be created with first metric)
        """
        try:
            # Try to find existing job_idx for this job_id
            query = text("""
                SELECT DISTINCT job_idx
                FROM public.vm_metric_metadata
                WHERE job_id = :job_id
                LIMIT 1
            """)
            
            result = conn.execute(query, {"job_id": job_id})
            row = result.fetchone()
            
            if row:
                job_idx = row[0]
                self.logger.debug(
                    "Found existing job_idx=%s for job_id='%s'",
                    job_idx,
                    job_id
                )
                return job_idx
            
            # No existing job_idx found - return None
            # The first metric entry will auto-generate the job_idx via BIGSERIAL
            self.logger.debug(
                "No existing job_idx for job_id='%s' - will be created with first metric entry",
                job_id
            )
            return None
            
        except SQLAlchemyError as exc:
            self.logger.error(
                "Database error finding job_idx for job_id='%s': %s",
                job_id,
                exc
            )
            return None
        except Exception as exc:
            self.logger.error(
                "Failed to find job_idx for job_id='%s': %s",
                job_id,
                exc
            )
            return None

    def _find_or_get_metric_id(
        self,
        conn: Any,
        job_idx: Optional[int],
        job_id: str,
        metric_name: str,
        metric_labels: Dict[str, str]
    ) -> Tuple[Optional[int], Optional[int]]:
        """Find existing metric_id or create new one in vm_metric_metadata.
        
        Searches for existing metric_id by matching job_idx, job_id, metric_name,
        and normalized metric_labels. If not found, inserts a new row and returns
        the new metric_id. If job_idx is None, the first insert will auto-generate it.
        
        Args:
            conn: Database connection
            job_idx: Job index value, or None if this is the first metric for this job_id
            job_id: Job ID string
            metric_name: Metric name
            metric_labels: Dictionary of metric labels (will be normalized)
            
        Returns:
            Tuple of (job_idx, metric_id) - both will be set after first insert if job_idx was None
        """
        try:
            # Normalize labels for comparison
            normalized_labels_json = self._normalize_metric_labels_for_comparison(metric_labels)
            
            # If job_idx is provided, try to find existing metric_id
            if job_idx is not None:
                query = text("""
                    SELECT metric_id
                    FROM public.vm_metric_metadata
                    WHERE job_idx = :job_idx
                      AND job_id = :job_id
                      AND metric_name = :metric_name
                      AND metric_labels = CAST(:normalized_labels_json AS jsonb)
                    LIMIT 1
                """)
                
                result = conn.execute(query, {
                    "job_idx": job_idx,
                    "job_id": job_id,
                    "metric_name": metric_name,
                    "normalized_labels_json": normalized_labels_json
                })
                row = result.fetchone()
                
                if row:
                    metric_id = row[0]
                    self.logger.info(
                        "Found existing metric_id=%s for job_id='%s', metric_name='%s', labels=%s",
                        metric_id,
                        job_id,
                        metric_name,
                        normalized_labels_json[:100] if len(normalized_labels_json) > 100 else normalized_labels_json
                    )
                    return (job_idx, metric_id)
                
                # Not found - need to create new entry with existing job_idx
                max_query = text("""
                    SELECT COALESCE(MAX(metric_id), 0)
                    FROM public.vm_metric_metadata
                    WHERE job_idx = :job_idx
                """)
                
                max_result = conn.execute(max_query, {"job_idx": job_idx})
                max_row = max_result.fetchone()
                new_metric_id = (max_row[0] if max_row else 0) + 1
                
                # Insert new metadata entry
                insert_query = text("""
                    INSERT INTO public.vm_metric_metadata (
                        job_idx, metric_id, job_id, metric_name, metric_labels
                    )
                    VALUES (
                        :job_idx, :metric_id, :job_id, :metric_name, CAST(:metric_labels AS jsonb)
                    )
                    RETURNING metric_id
                """)
                
                insert_result = conn.execute(insert_query, {
                    "job_idx": job_idx,
                    "metric_id": new_metric_id,
                    "job_id": job_id,
                    "metric_name": metric_name,
                    "metric_labels": normalized_labels_json
                })
                
                conn.commit()
                new_metric_id = insert_result.fetchone()[0]
                
                self.logger.info(
                    "Created new metric_id=%s for job_id='%s', metric_name='%s'",
                    new_metric_id,
                    job_id,
                    metric_name
                )
                
                return (job_idx, new_metric_id)
            else:
                # No job_idx exists - this is the first metric for this job_id
                # Insert will auto-generate job_idx via BIGSERIAL
                # Use metric_id = 1 for the first metric
                insert_query = text("""
                    INSERT INTO public.vm_metric_metadata (
                        job_idx, metric_id, job_id, metric_name, metric_labels
                    )
                    VALUES (
                        DEFAULT, 1, :job_id, :metric_name, CAST(:metric_labels AS jsonb)
                    )
                    RETURNING job_idx, metric_id
                """)
                
                insert_result = conn.execute(insert_query, {
                    "job_id": job_id,
                    "metric_name": metric_name,
                    "metric_labels": normalized_labels_json
                })
                
                conn.commit()
                row = insert_result.fetchone()
                new_job_idx = row[0]
                new_metric_id = row[1]
                
                self.logger.info(
                    "Created new job_idx=%s and metric_id=%s for job_id='%s', metric_name='%s'",
                    new_job_idx,
                    new_metric_id,
                    job_id,
                    metric_name
                )
                
                return (new_job_idx, new_metric_id)
            
        except SQLAlchemyError as exc:
            self.logger.error(
                "Database error finding/creating metric_id for job_id='%s', metric_name='%s': %s",
                job_id,
                metric_name,
                exc
            )
            if conn:
                conn.rollback()
            return (None, None)
        except Exception as exc:
            self.logger.error(
                "Failed to find/create metric_id for job_id='%s', metric_name='%s': %s",
                job_id,
                metric_name,
                exc
            )
            return (None, None)

    def _write_forecasts_to_database(
        self,
        state: MetricsForecastState,
        series: SeriesHistory,
        forecast_df: pd.DataFrame,
        run_id: Optional[int] = None,
    ) -> int:
        """Write forecast data to new schema (vm_metric_data and vm_metric_metadata).
        
        This method:
        1. Extracts job label and adds "_forecast" suffix
        2. Removes job label from metric_labels
        3. Adds forecast_type to metric_labels
        4. Finds or creates job_idx and metric_id in vm_metric_metadata
        5. Inserts forecast values into vm_metric_data
        
        Args:
            state: Job state with database connection
            series: Series history containing metric metadata
            forecast_df: Prophet forecast DataFrame with predictions
            run_id: Optional reference to vm_forecast_job run record (stored in vm_metric_data)
            
        Returns:
            Number of forecast rows written
        """
        try:
            conn = self._get_database_connection(state)
            if not conn:
                raise ValueError("Database connection not available")
            
            # Extract job label (required for transformation)
            input_job = series.labels.get("job")
            if not input_job:
                self.logger.error(
                    "Missing 'job' label in series %s",
                    series.metric_name,
                )
                return 0
            
            # Transform job_id: add "_forecast" suffix
            forecast_job_id = f"{input_job}_forecast"
            
            # Prepare base metric_labels: remove job label and exclude system labels
            excluded_labels = {"job", "auid", "biz_date", "forecast"}
            base_metric_labels = {
                k: v for k, v in series.labels.items() 
                if k not in excluded_labels
            }
            
            # Find existing job_idx for the forecast_job_id (may be None if first metric)
            job_idx = self._find_or_get_job_idx(conn, forecast_job_id)
            
            # Each forecast_type becomes its own timeseries with its own metric_id
            # STEP 1: Look up or create ALL metadata entries FIRST (before inserting any data)
            # This ensures the FK constraint is satisfied
            forecast_type_metric_ids = {}
            
            for forecast_type in state.forecast_types:
                name = forecast_type.get("name")
                if not name:
                    continue
                
                # Add forecast_type to metric_labels - this makes each forecast_type a separate timeseries
                metric_labels_with_type = dict(base_metric_labels)
                metric_labels_with_type["forecast_type"] = name
                
                # Find or get metric_id for this forecast_type timeseries
                # This will CREATE the metadata entry if it doesn't exist
                # If job_idx is None, this will auto-generate it with the first metric
                job_idx, metric_id = self._find_or_get_metric_id(
                    conn,
                    job_idx,
                    forecast_job_id,
                    series.metric_name,
                    metric_labels_with_type
                )
                
                if job_idx is None or metric_id is None:
                    self.logger.warning(
                        "Failed to get job_idx/metric_id for %s (forecast_type=%s), skipping this forecast type",
                        series.metric_name,
                        name
                    )
                    continue
                
                forecast_type_metric_ids[name] = metric_id
            
            if not forecast_type_metric_ids:
                self.logger.warning(
                    "No valid metric_ids found for any forecast_type for %s, skipping",
                    series.metric_name
                )
                return 0
            
            # STEP 2: Ensure all metadata entries are committed before inserting data
            # (The _find_or_get_metric_id method commits after creation, but we ensure it's done here)
            conn.commit()
            
            # STEP 3: Now we can safely insert data (FK constraint will be satisfied)
            
            # Now prepare rows for batch insert using the pre-looked-up metric_ids
            rows_to_insert = []
            
            for future_row in forecast_df.itertuples():
                # Use forecast date as-is (biz_date from Prophet forecast)
                forecast_date = future_row.ds.date()
                forecast_timestamp = datetime.combine(
                    forecast_date, 
                    datetime.min.time()
                ).replace(tzinfo=timezone.utc)
                
                for forecast_type in state.forecast_types:
                    name = forecast_type.get("name")
                    field = forecast_type.get("field")
                    if not name or not field or not hasattr(future_row, field):
                        continue
                    
                    # Skip if we didn't get a metric_id for this forecast_type
                    if name not in forecast_type_metric_ids:
                        continue
                    
                    value = getattr(future_row, field)
                    if value is None or np.isnan(value):
                        continue
                    
                    # Use the pre-looked-up metric_id for this forecast_type
                    metric_id = forecast_type_metric_ids[name]
                    
                    rows_to_insert.append({
                        "job_idx": job_idx,
                        "metric_id": metric_id,
                        "metric_timestamp": forecast_timestamp,
                        "metric_value": float(value),
                        "run_id": run_id,  # Store run_id to track which forecast run generated this data
                    })
            
            if not rows_to_insert:
                self.logger.debug("No forecast rows to write for %s", series.metric_name)
                return 0
            
            # Build PostgreSQL upsert statement for vm_metric_data
            # ON CONFLICT ... DO UPDATE for idempotent writes
            upsert_sql = text("""
                INSERT INTO public.vm_metric_data (
                    job_idx, metric_id, metric_timestamp, metric_value, run_id
                )
                VALUES (
                    :job_idx, :metric_id, :metric_timestamp, :metric_value, :run_id
                )
                ON CONFLICT (job_idx, metric_id, metric_timestamp)
                DO UPDATE SET
                    metric_value = EXCLUDED.metric_value,
                    run_id = EXCLUDED.run_id
            """)
            
            # Execute batch insert
            for row in rows_to_insert:
                conn.execute(upsert_sql, row)
            
            conn.commit()
            
            self.logger.info(
                "Wrote %s forecast rows for %s (job_idx=%s, forecast_job_id='%s')",
                len(rows_to_insert),
                series.metric_name,
                job_idx,
                forecast_job_id,
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

