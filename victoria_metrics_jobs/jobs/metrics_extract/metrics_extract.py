#!/usr/bin/env python3
"""
Metrics Extract Job - extracts time-series metrics from VictoriaMetrics and stores
them in PostgreSQL database.

The job:
1. Derives the current business date (optional, for cutoff logic)
2. Processes each configured metric selector
3. Queries VictoriaMetrics for metrics since last extracted timestamp
4. Writes metrics to the database with upsert logic
5. Publishes job status metric to VictoriaMetrics

Database Storage:
- Metrics are stored in PostgreSQL 'vm_metric_data' and 'vm_metric_metadata' tables
- Metadata table (vm_metric_metadata): stores job_idx, metric_id, job_id, metric_name, metric_labels
- Data table (vm_metric_data): stores job_idx, metric_id, metric_timestamp, metric_value, run_id
- Primary key: (job_idx, metric_id, metric_timestamp)
- Re-running extractions overwrites existing values (idempotent)
- Job labels are kept as-is (no transformation)
- Tracks latest timestamp per selector in vm_metric_extract_job table
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

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
class MetricsExtractState(BaseJobState):
    """State object for the metrics_extract job."""

    current_business_date: Optional[date] = None
    metric_selectors: List[str] = field(default_factory=list)
    initial_lookback_days: int = 30
    vm_query_url: str = ""
    vm_gateway_url: str = ""
    vm_token: str = ""
    series_processed: int = 0
    metrics_saved_count: int = 0
    failed_series: int = 0
    prom_client: Optional[PrometheusConnect] = None
    extract_db_config: Dict[str, Any] = field(default_factory=dict)
    db_engine: Optional[Engine] = None
    db_connection: Optional[Any] = None

    def to_results(self) -> Dict[str, Any]:
        """Extend base results with extraction metadata."""
        results = super().to_results()
        results.update(
            {
                "series_processed": self.series_processed,
                "metrics_saved_count": self.metrics_saved_count,
                "failed_series": self.failed_series,
                "current_business_date": self.current_business_date.isoformat()
                if self.current_business_date
                else None,
            }
        )
        return results


class MetricsExtractJob(BaseJob):
    """Metrics Extract job for extracting time-series metrics from VictoriaMetrics."""

    def __init__(self, config_path: str = None, verbose: bool = False):
        super().__init__("metrics_extract", config_path, verbose)

    def create_initial_state(self, job_id: str) -> Result[MetricsExtractState, Exception]:
        try:
            job_config = self.get_job_config(job_id)

            # Get metric selectors from config
            metric_selectors = job_config.get("metric_selectors", [])
            if not metric_selectors:
                raise ValueError(
                    "metric_selectors configuration required. "
                    "Specify a list of PromQL selectors in job config."
                )

            victoria_metrics_cfg = job_config.get("victoria_metrics", {})

            # Get database configuration - check for extract_database first, fallback to common database
            extract_db_config = job_config.get("extract_database")
            if not extract_db_config:
                # Try to get common database config
                extract_db_config = job_config.get("database")

            if not extract_db_config:
                raise ValueError(
                    "Database configuration required for metric storage. "
                    "Specify 'extract_database' in job config or 'database' in common config."
                )

            state = MetricsExtractState(
                job_id=job_id,
                job_config=job_config,
                started_at=datetime.now(),
                metric_selectors=metric_selectors,
                initial_lookback_days=int(job_config.get("initial_lookback_days", 30)),
                vm_query_url=victoria_metrics_cfg.get("query_url", ""),
                vm_gateway_url=victoria_metrics_cfg.get("gateway_url", ""),
                vm_token=victoria_metrics_cfg.get("token", ""),
                extract_db_config=extract_db_config,
            )

            self.logger.info(
                "Initialized metrics_extract job with %s selector(s)",
                len(metric_selectors),
            )

            return Ok(state)
        except Exception as exc:
            return Err(exc)

    def get_workflow_steps(self) -> List[callable]:
        return [
            self._derive_current_business_date,
            self._process_extract_selectors,
            self._publish_job_status_metric,
        ]

    def finalize_state(self, state: MetricsExtractState) -> MetricsExtractState:
        # Close database connection before finalizing
        self._close_database_connection(state)

        state.completed_at = datetime.now()
        if state.failed_series > 0 and state.metrics_saved_count == 0:
            state.status = "error"
            state.message = "Extraction failed for all series"
        elif state.failed_series > 0:
            state.status = "partial_success"
            state.message = (
                f"Extraction completed with warnings: "
                f"{state.series_processed} processed, {state.failed_series} failed, "
                f"{state.metrics_saved_count} metrics saved"
            )
        elif state.series_processed == 0:
            state.status = "success"
            state.message = "No matching series to extract"
        else:
            state.status = "success"
            state.message = (
                f"Extraction completed: {state.series_processed} series processed, "
                f"{state.metrics_saved_count} metrics saved"
            )
        return state

    # Step 1: Determine the business date anchor for the run (optional)
    def _derive_current_business_date(
        self, state: MetricsExtractState
    ) -> Result[MetricsExtractState, Exception]:
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

    # Step 2: Process all extract selectors
    def _process_extract_selectors(
        self, state: MetricsExtractState
    ) -> Result[MetricsExtractState, Exception]:
        """Process all metric selectors from config.
        
        For each selector:
        1. Create a run record in metric_extract_job
        2. Query last extracted timestamp for this selector
        3. Query VM for metrics since last timestamp
        4. Save metrics to database
        5. Update last_timestamp in run record
        """
        try:
            # Get database connection
            conn = self._get_database_connection(state)
            if not conn:
                raise ValueError("Database connection required for extraction")

            # Get Prometheus client for querying metrics
            prom = self._get_prometheus_client(state)
            if prom is None:
                raise ValueError("Prometheus client could not be initialized")

            self.logger.info(
                "Processing %s metric selector(s) for job_id='%s'...",
                len(state.metric_selectors),
                state.job_id,
            )

            # Process each selector
            for selector in state.metric_selectors:
                try:
                    self.logger.info("Processing selector: %s", selector)

                    # Create extract run record for this selector
                    run_id = self._create_extract_run_record(state, selector)
                    if not run_id:
                        self.logger.warning(
                            "Failed to create run record for selector '%s', skipping",
                            selector,
                        )
                        continue

                    # Get last extracted timestamp for this selector
                    last_timestamp = self._get_last_timestamp(state, selector)
                    if last_timestamp:
                        self.logger.info(
                            "Last extracted timestamp for '%s': %s",
                            selector,
                            last_timestamp,
                        )
                    else:
                        self.logger.info(
                            "No previous extraction found for '%s', using initial lookback of %s days",
                            selector,
                            state.initial_lookback_days,
                        )

                    # Calculate time range for extraction
                    end_time = datetime.utcnow().replace(tzinfo=timezone.utc)
                    if last_timestamp:
                        start_time = last_timestamp
                    else:
                        # Use initial lookback if no previous extraction
                        start_time = end_time - timedelta(days=state.initial_lookback_days)

                    self.logger.info(
                        "Extracting metrics for '%s' from %s to %s",
                        selector,
                        start_time,
                        end_time,
                    )

                    # Query metric series for this selector
                    series_list = self._query_series_for_selection(
                        state, prom, selector, start_time, end_time
                    )

                    if not series_list:
                        self.logger.info("No series found for selector='%s'", selector)
                        # Update run record with no data
                        self._update_extract_run_record(
                            state, run_id, 0, 0, end_time, "completed"
                        )
                        continue

                    self.logger.info(
                        "Found %s series for selector='%s', starting extraction...",
                        len(series_list),
                        selector,
                    )

                    # Extract each series
                    max_timestamp = None
                    series_count = 0
                    metrics_count = 0

                    for series in series_list:
                        try:
                            rows_written, series_max_ts = self._save_series_to_database(
                                state, series, run_id
                            )

                            if rows_written > 0:
                                metrics_count += rows_written
                                series_count += 1
                                state.series_processed += 1
                                state.metrics_saved_count += rows_written

                                # Track max timestamp across all series
                                if series_max_ts and (
                                    max_timestamp is None or series_max_ts > max_timestamp
                                ):
                                    max_timestamp = series_max_ts

                        except Exception as series_exc:
                            state.failed_series += 1
                            self.logger.error(
                                "Failed to extract series %s: %s",
                                series.metric_name,
                                series_exc,
                            )

                    # Update run record with results
                    final_timestamp = max_timestamp if max_timestamp else end_time
                    status = "completed" if series_count > 0 else "completed"
                    self._update_extract_run_record(
                        state, run_id, series_count, metrics_count, final_timestamp, status
                    )

                    self.logger.info(
                        "Completed selector '%s': %s series, %s metrics saved",
                        selector,
                        series_count,
                        metrics_count,
                    )

                except Exception as selector_exc:
                    self.logger.error(
                        "Failed to process selector '%s': %s", selector, selector_exc
                    )
                    continue

            self.logger.info(
                "Extraction processing complete: %s series processed, %s metrics saved, %s failed",
                state.series_processed,
                state.metrics_saved_count,
                state.failed_series,
            )

            return Ok(state)

        except Exception as exc:
            self.logger.error("Failed to process extract selectors: %s", exc)
            return Err(exc)

    # Step 3: Publish status metric for observability
    def _publish_job_status_metric(
        self, state: MetricsExtractState
    ) -> Result[MetricsExtractState, Exception]:
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
                f'metrics_extract_job_status{{{",".join(label_pairs)}}} {status_value} {timestamp}'
            )

            self._write_metric_to_vm(state, metric_line, timeout=30)
            return Ok(state)
        except Exception as exc:
            self.logger.warning("Failed to publish job status metric: %s", exc)
            return Ok(state)

    # Helper methods
    def _parse_range_query(
        self, query_result: Any, selection_value: Optional[str] = None
    ) -> List[SeriesHistory]:
        """Parse Prometheus range query response into SeriesHistory objects."""
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
        state: MetricsExtractState,
        prom: PrometheusConnect,
        selection_value: str,
        start_time: datetime,
        end_time: datetime,
    ) -> List[SeriesHistory]:
        """Query metric series for a specific PromQL selector."""
        try:
            # Use selector as-is (complete PromQL query)
            query = selection_value.strip().replace("'", '"')

            self.logger.debug("Executing query: %s", query)

            # Calculate step size (use 1 hour steps for extraction)
            step_str = "1h"

            # Use custom_query_range for PromQL queries
            query_result = prom.custom_query_range(
                query=query,
                start_time=start_time,
                end_time=end_time,
                step=step_str,
            )

            # Parse the result into SeriesHistory objects
            series_list = self._parse_range_query(query_result, selection_value)

            return series_list

        except Exception as exc:
            self.logger.error(
                "Failed to query series for selector '%s': %s", selection_value, exc
            )
            return []

    def _create_extract_run_record(
        self, state: MetricsExtractState, selection_value: str
    ) -> Optional[int]:
        """Create an extract job run record in the database."""
        try:
            conn = self._get_database_connection(state)
            if not conn:
                self.logger.warning(
                    "Cannot create extract run record - no database connection"
                )
                return None

            insert_sql = text("""
                INSERT INTO public.vm_metric_extract_job (
                    job_id,
                    selection_value,
                    started_at,
                    status
                )
                VALUES (
                    :job_id,
                    :selection_value,
                    :started_at,
                    :status
                )
                RETURNING run_id
            """)

            result = conn.execute(
                insert_sql,
                {
                    "job_id": state.job_id,
                    "selection_value": selection_value,
                    "started_at": datetime.utcnow(),
                    "status": "running",
                },
            )

            conn.commit()
            run_id = result.fetchone()[0]

            self.logger.info(
                "Created extract run record: run_id=%s for selector='%s'",
                run_id,
                selection_value,
            )

            return run_id

        except Exception as exc:
            self.logger.warning(
                "Failed to create extract run record for selector='%s': %s",
                selection_value,
                exc,
            )
            return None

    def _update_extract_run_record(
        self,
        state: MetricsExtractState,
        run_id: int,
        series_count: int,
        metrics_saved_count: int,
        last_timestamp: datetime,
        status: str,
    ) -> None:
        """Update extract run record with completion information."""
        try:
            conn = self._get_database_connection(state)
            if not conn:
                return

            update_sql = text("""
                UPDATE public.vm_metric_extract_job
                SET completed_at = :completed_at,
                    duration_seconds = EXTRACT(EPOCH FROM (:completed_at - started_at)),
                    series_count = :series_count,
                    metrics_saved_count = :metrics_saved_count,
                    last_timestamp = :last_timestamp,
                    status = :status
                WHERE run_id = :run_id
            """)

            completed_at = datetime.utcnow()
            conn.execute(
                update_sql,
                {
                    "run_id": run_id,
                    "completed_at": completed_at,
                    "series_count": series_count,
                    "metrics_saved_count": metrics_saved_count,
                    "last_timestamp": last_timestamp,
                    "status": status,
                },
            )

            conn.commit()

        except Exception as exc:
            self.logger.warning("Failed to update extract run record: %s", exc)

    def _get_last_timestamp(
        self, state: MetricsExtractState, selection_value: str
    ) -> Optional[datetime]:
        """Query last extracted timestamp for a selector."""
        try:
            conn = self._get_database_connection(state)
            if not conn:
                return None

            query = text("""
                SELECT last_timestamp
                FROM public.vm_metric_extract_job
                WHERE selection_value = :selection_value
                  AND last_timestamp IS NOT NULL
                ORDER BY run_id DESC
                LIMIT 1
            """)

            result = conn.execute(query, {"selection_value": selection_value})
            row = result.fetchone()

            if row and row[0]:
                return row[0]

            return None

        except Exception as exc:
            self.logger.warning(
                "Failed to query last timestamp for selector '%s': %s",
                selection_value,
                exc,
            )
            return None

    def _build_database_connection_string(self, db_config: Dict[str, Any]) -> str:
        """Build PostgreSQL connection string from config."""
        host = db_config.get("host", "localhost")
        port = db_config.get("port", 5432)
        dbname = db_config.get("name", "metrics")
        user = db_config.get("user", "metrics_user")
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

    def _get_database_engine(self, state: MetricsExtractState) -> Optional[Engine]:
        """Get or create SQLAlchemy engine for extract database."""
        if state.db_engine:
            return state.db_engine

        try:
            if not state.extract_db_config:
                self.logger.error("No database configuration available")
                return None

            connection_string = self._build_database_connection_string(
                state.extract_db_config
            )

            # Create engine with connection pooling
            state.db_engine = create_engine(
                connection_string,
                pool_size=5,
                max_overflow=10,
                pool_pre_ping=True,
                pool_recycle=3600,
                echo=False,
                future=True,
            )

            self.logger.info("Database engine created successfully")
            return state.db_engine

        except Exception as exc:
            self.logger.error("Failed to create database engine: %s", exc)
            return None

    def _get_database_connection(self, state: MetricsExtractState) -> Optional[Any]:
        """Get or create database connection."""
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

    def _close_database_connection(self, state: MetricsExtractState) -> None:
        """Close database connection and dispose of engine."""
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
        """Normalize metric labels for consistent comparison."""
        # Sort by key to ensure consistent ordering
        sorted_labels = dict(sorted(labels.items()))
        return json.dumps(sorted_labels, sort_keys=True)

    def _find_or_get_job_idx(self, conn: Any, job_id: str) -> Optional[int]:
        """Find existing job_idx for a given job_id.
        
        Returns None if no job_idx exists - the first metric entry will create it automatically.
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
                    "Found existing job_idx=%s for job_id='%s'", job_idx, job_id
                )
                return job_idx

            # No existing job_idx found - return None
            # The first metric entry will auto-generate the job_idx via BIGSERIAL
            self.logger.debug(
                "No existing job_idx for job_id='%s' - will be created with first metric entry",
                job_id,
            )
            return None

        except SQLAlchemyError as exc:
            self.logger.error(
                "Database error finding job_idx for job_id='%s': %s",
                job_id,
                exc,
            )
            return None
        except Exception as exc:
            self.logger.error(
                "Failed to find job_idx for job_id='%s': %s", job_id, exc
            )
            return None

    def _find_or_get_metric_id(
        self,
        conn: Any,
        job_idx: Optional[int],
        job_id: str,
        metric_name: str,
        metric_labels: Dict[str, str],
    ) -> Tuple[Optional[int], Optional[int]]:
        """Find existing metric_id or create new one in vm_metric_metadata.
        
        Args:
            conn: Database connection
            job_idx: Existing job_idx, or None if this is the first metric for this job_id
            job_id: Job ID string
            metric_name: Metric name
            metric_labels: Dictionary of metric labels (will be normalized)
            
        Returns:
            Tuple of (job_idx, metric_id) - both will be set after first insert if job_idx was None
        """
        try:
            # Normalize labels for comparison
            normalized_labels_json = self._normalize_metric_labels_for_comparison(
                metric_labels
            )

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

                result = conn.execute(
                    query,
                    {
                        "job_idx": job_idx,
                        "job_id": job_id,
                        "metric_name": metric_name,
                        "normalized_labels_json": normalized_labels_json,
                    },
                )
                row = result.fetchone()

                if row:
                    metric_id = row[0]
                    self.logger.debug(
                        "Found existing metric_id=%s for job_id='%s', metric_name='%s'",
                        metric_id,
                        job_id,
                        metric_name,
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

                insert_result = conn.execute(
                    insert_query,
                    {
                        "job_idx": job_idx,
                        "metric_id": new_metric_id,
                        "job_id": job_id,
                        "metric_name": metric_name,
                        "metric_labels": normalized_labels_json,
                    },
                )

                conn.commit()
                new_metric_id = insert_result.fetchone()[0]

                self.logger.info(
                    "Created new metric_id=%s for job_id='%s', metric_name='%s'",
                    new_metric_id,
                    job_id,
                    metric_name,
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

                insert_result = conn.execute(
                    insert_query,
                    {
                        "job_id": job_id,
                        "metric_name": metric_name,
                        "metric_labels": normalized_labels_json,
                    },
                )

                conn.commit()
                row = insert_result.fetchone()
                new_job_idx = row[0]
                new_metric_id = row[1]

                self.logger.info(
                    "Created new job_idx=%s and metric_id=%s for job_id='%s', metric_name='%s'",
                    new_job_idx,
                    new_metric_id,
                    job_id,
                    metric_name,
                )

                return (new_job_idx, new_metric_id)

        except SQLAlchemyError as exc:
            self.logger.error(
                "Database error finding/creating metric_id for job_id='%s', metric_name='%s': %s",
                job_id,
                metric_name,
                exc,
            )
            if conn:
                conn.rollback()
            return (None, None)
        except Exception as exc:
            self.logger.error(
                "Failed to find/create metric_id for job_id='%s', metric_name='%s': %s",
                job_id,
                metric_name,
                exc,
            )
            return (None, None)

    def _save_series_to_database(
        self,
        state: MetricsExtractState,
        series: SeriesHistory,
        run_id: int,
    ) -> Tuple[int, Optional[datetime]]:
        """Save a single series to database.
        
        Returns:
            Tuple of (rows_written, max_timestamp)
        """
        try:
            conn = self._get_database_connection(state)
            if not conn:
                raise ValueError("Database connection not available")

            # Extract job label from metric labels (keep as-is, no transformation)
            job_id = series.labels.get("job")
            if not job_id:
                # Fallback to extractor job's job_id if no job label
                job_id = state.job_id
                self.logger.debug(
                    "No 'job' label in series %s, using extractor job_id '%s'",
                    series.metric_name,
                    job_id,
                )

            # Prepare metric_labels: exclude system labels that shouldn't be stored
            excluded_labels = {"job"}  # job is stored separately as job_id
            metric_labels = {
                k: v for k, v in series.labels.items() if k not in excluded_labels
            }

            # Find existing job_idx for the job_id (may be None if first metric)
            job_idx = self._find_or_get_job_idx(conn, job_id)

            # Find or get metric_id for this series
            # This will create job_idx automatically if it doesn't exist
            job_idx, metric_id = self._find_or_get_metric_id(
                conn, job_idx, job_id, series.metric_name, metric_labels
            )

            if job_idx is None or metric_id is None:
                self.logger.warning(
                    "Failed to get job_idx/metric_id for %s, skipping", series.metric_name
                )
                return (0, None)

            # Ensure metadata is committed before inserting data
            conn.commit()

            # Prepare rows for batch insert
            rows_to_insert = []
            max_timestamp = None

            for sample_ts, sample_value in series.samples:
                rows_to_insert.append(
                    {
                        "job_idx": job_idx,
                        "metric_id": metric_id,
                        "metric_timestamp": sample_ts,
                        "metric_value": float(sample_value),
                        "run_id": run_id,
                    }
                )

                # Track max timestamp
                if max_timestamp is None or sample_ts > max_timestamp:
                    max_timestamp = sample_ts

            if not rows_to_insert:
                self.logger.debug("No samples to write for %s", series.metric_name)
                return (0, None)

            # Build PostgreSQL upsert statement for vm_metric_data
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
                "Wrote %s metric rows for %s (job_idx=%s, job_id='%s', metric_id=%s)",
                len(rows_to_insert),
                series.metric_name,
                job_idx,
                job_id,
                metric_id,
            )

            return (len(rows_to_insert), max_timestamp)

        except SQLAlchemyError as exc:
            self.logger.error(
                "Database error writing metrics for %s: %s", series.metric_name, exc
            )
            # Rollback on error
            if conn:
                conn.rollback()
            return (0, None)
        except Exception as exc:
            self.logger.error(
                "Failed to write metrics to database for %s: %s",
                series.metric_name,
                exc,
            )
            return (0, None)

    def _get_prometheus_client(
        self, state: MetricsExtractState
    ) -> Optional[PrometheusConnect]:
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
        self, state: MetricsExtractState, metric_line: str, timeout: int = 60
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
  python -m victoria_metrics_jobs.jobs.metrics_extract --config victoria_metrics_jobs/victoria_metrics_jobs.yml --list-jobs

  # Run metrics extract job
  python -m victoria_metrics_jobs.jobs.metrics_extract --config victoria_metrics_jobs/victoria_metrics_jobs.yml --job-id metrics_extract
    """

    return MetricsExtractJob.main(
        description="Metrics Extract Job - Extract time-series metrics from VictoriaMetrics",
        epilog=epilog,
    )


if __name__ == "__main__":
    sys.exit(main())

