#!/usr/bin/env python3
"""
Metrics Forecast Job - trains Prophet models per metric series and publishes
business-day forecasts back to Victoria Metrics.
"""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
from prophet import Prophet
from prometheus_api_client import PrometheusConnect

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

    # Step 3: Train Prophet models and publish forecasts
    def _forecast_and_publish(
        self, state: MetricsForecastState
    ) -> Result[MetricsForecastState, Exception]:
        try:
            if not state.series_histories:
                self.logger.warning("No metric series discovered for forecasting")
                return Ok(state)

            prom = self._get_prometheus_client(state)
            if prom is None:
                raise ValueError("Prometheus client could not be initialized")
            if not state.vm_gateway_url:
                raise ValueError("victoria_metrics.gateway_url must be configured")

            for series_idx, series in enumerate(state.series_histories):
                try:
                    # Add small delay between series to avoid resource contention
                    if series_idx > 0:
                        import time
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
                            error_msg_lower = error_msg.lower()
                            
                            # Check for cmdstanpy/Stan errors
                            is_cmdstan_error = (
                                "cmdstanpy" in error_msg_lower
                                or "cmdstan" in error_msg_lower
                                or "signal" in error_msg_lower
                                or "32212256857" in error_msg
                                or "3221225657" in error_msg
                                or "terminated by signal" in error_msg_lower
                            )
                            
                            if is_cmdstan_error:
                                if attempt < max_retries - 1:
                                    wait_seconds = (2 ** attempt) + 1  # Exponential backoff, min 2s
                                    self.logger.warning(
                                        "Prophet fit failed (attempt %s/%s) for %s due to cmdstanpy crash: %s. "
                                        "Retrying in %s seconds...",
                                        attempt + 1,
                                        max_retries,
                                        series.metric_name,
                                        error_msg[:200],
                                        wait_seconds,
                                    )
                                    import time
                                    time.sleep(wait_seconds)
                                    
                                    # Aggressive cleanup before retry
                                    import gc
                                    del model
                                    gc.collect()
                                    time.sleep(0.5)  # Additional small delay for cleanup
                                    
                                    # Recreate model for retry to clear any corrupted state
                                    model = self._create_prophet_model(state)
                                else:
                                    self.logger.error(
                                        "Prophet fit failed after %s attempts for %s due to cmdstanpy crash: %s",
                                        max_retries,
                                        series.metric_name,
                                        error_msg[:200],
                                    )
                            else:
                                # Non-cmdstanpy error, don't retry
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

                    # Extract unique forecast dates for batch timestamp lookup
                    forecast_dates = [row.ds.date() for row in forecast_df.itertuples()]
                    unique_forecast_dates = sorted(set(forecast_dates))
                    
                    # Batch lookup existing forecast timestamps once per series
                    timestamp_map = self._batch_lookup_forecast_timestamps(
                        state,
                        prom,
                        series.metric_name,
                        series.labels,
                        state.forecast_types,
                        unique_forecast_dates,
                    )
                    
                    # Log timestamp map contents for debugging
                    self.logger.debug(
                        "Timestamp map for %s: %s entries",
                        series.metric_name,
                        len([v for v in timestamp_map.values() if v is not None]),
                    )
                    for (fd, ft), ts in timestamp_map.items():
                        if ts is not None:
                            self.logger.debug(
                                "  %s/%s on %s: existing_ts=%s",
                                series.metric_name,
                                ft,
                                fd,
                                ts,
                            )

                    publish_rows: List[Dict[str, Any]] = []
                    for future_row in forecast_df.itertuples():
                        forecast_date = future_row.ds.date()
                        for forecast_type in state.forecast_types:
                            name = forecast_type.get("name")
                            field = forecast_type.get("field")
                            if not name or not field or not hasattr(future_row, field):
                                continue
                            value = getattr(future_row, field)
                            if value is None:
                                continue

                            labels = series.labels.copy()
                            labels["forecast"] = name

                            # Look up timestamp from batch lookup map
                            existing_max_ts = timestamp_map.get((forecast_date, name))
                            
                            if existing_max_ts is not None:
                                # Increment by 1 minute for deterministic overwriting (matches query step)
                                end_dt = datetime.combine(forecast_date, datetime.max.time()).replace(tzinfo=timezone.utc)
                                timestamp = min(existing_max_ts + 60, int(end_dt.timestamp()))
                                self.logger.info(
                                    "Using incremented timestamp for %s/%s on %s: existing_max_ts=%s -> new_ts=%s (increment=%s)",
                                    series.metric_name,
                                    name,
                                    forecast_date,
                                    existing_max_ts,
                                    timestamp,
                                    timestamp - existing_max_ts,
                                )
                            else:
                                # No existing forecast, use midnight timestamp
                                start_dt = datetime.combine(forecast_date, datetime.min.time()).replace(tzinfo=timezone.utc)
                                timestamp = int(start_dt.timestamp())
                                self.logger.warning(
                                    "No existing forecast found in map for %s/%s on %s, using midnight timestamp: %s (map keys: %s)",
                                    series.metric_name,
                                    name,
                                    forecast_date,
                                    timestamp,
                                    list(timestamp_map.keys())[:10] if timestamp_map else "empty",
                                )
                            
                            publish_rows.append(
                                {
                                    "metric_name": series.metric_name,
                                    "labels": labels,
                                    "value": float(value),
                                    "timestamp": timestamp,
                                }
                            )

                    if publish_rows:
                        # Log timestamps being written for debugging
                        timestamps_by_date = {}
                        for row in publish_rows:
                            date_key = (row["metric_name"], row["labels"].get("forecast"), row["timestamp"])
                            if date_key not in timestamps_by_date:
                                timestamps_by_date[date_key] = []
                            timestamps_by_date[date_key].append(row)
                        
                        # Check for duplicate timestamps in the same batch
                        duplicates = {k: v for k, v in timestamps_by_date.items() if len(v) > 1}
                        if duplicates:
                            self.logger.warning(
                                "Found duplicate timestamps in batch for %s: %s",
                                series.metric_name,
                                list(duplicates.keys())[:5],
                            )
                        
                        forecast_batch_df = pd.DataFrame(publish_rows)
                        self.logger.info(
                            "Writing %s forecast points for %s (sample timestamps: %s)",
                            len(forecast_batch_df),
                            series.metric_name,
                            [row["timestamp"] for row in publish_rows[:3]],
                        )
                        if self._write_metrics_dataframe_to_vm(
                            state, forecast_batch_df, timeout=60
                        ):
                            state.forecasts_written += len(forecast_batch_df)

                    state.series_processed += 1
                    
                    # Clean up model to free memory (helps prevent cmdstanpy crashes)
                    del model
                    import gc
                    gc.collect()

                except Exception as series_exc:
                    state.failed_series += 1
                    self.logger.error(
                        "Failed to forecast series %s labels=%s: %s",
                        series.metric_name,
                        series.labels,
                        series_exc,
                    )
                    # Clean up memory on failure to help prevent cascading crashes
                    import gc
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
        # Clean up any lingering Stan processes/resources before creating new model
        import gc
        gc.collect()
        
        model = Prophet(**state.prophet_config)
        # Some downstream environments ship Prophet builds that forget to set this attribute.
        if not hasattr(model, "stan_backend"):
            model.stan_backend = None  # Prophet.fit() will populate a backend when None
            self.logger.debug("Prophet instance missing stan_backend attribute; initialized to None")
        
        return model

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
        return self._post_payload_to_vm(
            state, metric_line, timeout=timeout, context="metric"
        )

    def _write_metrics_dataframe_to_vm(
        self, state: MetricsForecastState, dataframe: pd.DataFrame, timeout: int = 60
    ) -> bool:
        """Publish a pandas DataFrame of forecast rows in a single VM import."""
        if dataframe is None or dataframe.empty:
            return True

        metric_lines: List[str] = []
        for row in dataframe.itertuples(index=False):
            metric_lines.append(
                self._build_metric_line(
                    getattr(row, "metric_name"),
                    getattr(row, "labels"),
                    float(getattr(row, "value")),
                    int(getattr(row, "timestamp")),
                )
            )

        payload = "\n".join(metric_lines)
        return self._post_payload_to_vm(
            state, payload, timeout=timeout, context="forecast batch"
        )

    def _post_payload_to_vm(
        self, state: MetricsForecastState, payload: str, timeout: int, context: str
    ) -> bool:
        try:
            if not payload:
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
                data=payload,
                headers=headers,
                timeout=timeout,
            )
            response.raise_for_status()
            return True
        except Exception as exc:
            self.logger.error("Failed to write %s to VM: %s", context, exc)
            return False

    def _batch_lookup_forecast_timestamps(
        self,
        state: MetricsForecastState,
        prom: PrometheusConnect,
        metric_name: str,
        base_labels: Dict[str, str],
        forecast_types: List[Dict[str, str]],
        forecast_dates: List[date],
    ) -> Dict[Tuple[date, str], Optional[int]]:
        """Batch lookup existing forecast timestamps for all forecast dates and types.
        
        Queries VictoriaMetrics per date per forecast_type to avoid overwhelming VM
        with large range queries. Returns a map of (forecast_date, forecast_type_name) -> max_timestamp.
        
        Args:
            state: Job state object
            prom: Prometheus client for querying VM
            metric_name: Name of the metric
            base_labels: Labels without the forecast type label
            forecast_types: List of forecast type configs with 'name' field
            forecast_dates: List of forecast dates to check
            
        Returns:
            Dictionary mapping (forecast_date, forecast_type_name) -> max_timestamp (or None)
        """
        if not forecast_dates:
            return {}
        
        # Initialize result map with None for all combinations
        timestamp_map: Dict[Tuple[date, str], Optional[int]] = {}
        for forecast_date in forecast_dates:
            for forecast_type in forecast_types:
                forecast_type_name = forecast_type.get("name")
                if forecast_type_name:
                    timestamp_map[(forecast_date, forecast_type_name)] = None
        
        # Query per date per forecast_type to avoid overwhelming VM with large range queries
        # This is still batched compared to per-point queries
        for forecast_type in forecast_types:
            forecast_type_name = forecast_type.get("name")
            if not forecast_type_name:
                continue
            
            # Build labels with forecast type
            labels = base_labels.copy()
            labels["forecast"] = forecast_type_name
            
            label_pairs = [f'{k}="{v}"' for k, v in sorted(labels.items())]
            query = f'{metric_name}{{{",".join(label_pairs)}}}'
            
            # Query each date individually to get max timestamp (same approach as business_date_converter)
            for forecast_date in forecast_dates:
                start_dt = datetime.combine(forecast_date, datetime.min.time()).replace(tzinfo=timezone.utc)
                end_dt = datetime.combine(forecast_date, datetime.max.time()).replace(tzinfo=timezone.utc)
                
                try:
                    # Query the entire day with 1m step to find max timestamp
                    # This is manageable (~1440 points per day) and ensures we find all timestamps
                    self.logger.debug(
                        "Querying for existing forecasts: %s on %s (range: %s to %s)",
                        query,
                        forecast_date,
                        start_dt,
                        end_dt,
                    )
                    
                    query_result = prom.custom_query_range(
                        query=query,
                        start_time=start_dt,
                        end_time=end_dt,
                        step="1m",
                    )
                    
                    # Parse response to find max timestamp (same structure as business_date_converter)
                    max_ts = None
                    value_count = 0
                    if query_result and isinstance(query_result, dict):
                        if query_result.get("status") == "success" and "data" in query_result:
                            data = query_result["data"]
                            if "result" in data and isinstance(data["result"], list):
                                for series in data["result"]:
                                    if "values" in series and isinstance(series["values"], list):
                                        value_count += len(series["values"])
                                        for value_pair in series["values"]:
                                            if isinstance(value_pair, list) and len(value_pair) >= 1:
                                                # value_pair format: [timestamp, value]
                                                timestamp = float(value_pair[0])
                                                if max_ts is None or timestamp > max_ts:
                                                    max_ts = int(timestamp)
                        else:
                            self.logger.warning(
                                "Query failed or unexpected format for %s/%s on %s: status=%s",
                                metric_name,
                                forecast_type_name,
                                forecast_date,
                                query_result.get("status"),
                            )
                    
                    self.logger.info(
                        "Lookup result for %s/%s on %s: found %s values, max_ts=%s",
                        metric_name,
                        forecast_type_name,
                        forecast_date,
                        value_count,
                        max_ts,
                    )
                    
                    if max_ts is not None:
                        timestamp_map[(forecast_date, forecast_type_name)] = max_ts
                        self.logger.debug(
                            "Found existing forecast for %s/%s on %s with max_ts=%s",
                            metric_name,
                            forecast_type_name,
                            forecast_date,
                            max_ts,
                        )
                    else:
                        self.logger.debug(
                            "No existing forecast found for %s/%s on %s",
                            metric_name,
                            forecast_type_name,
                            forecast_date,
                        )
                        
                except Exception as exc:
                    self.logger.warning(
                        "Failed to lookup timestamp for %s/%s on %s: %s",
                        metric_name,
                        forecast_type_name,
                        forecast_date,
                        exc,
                    )
                    # Continue with other dates even if one fails
        
        return timestamp_map

    def _calculate_forecast_timestamp(
        self,
        state: MetricsForecastState,
        prom: PrometheusConnect,
        metric_name: str,
        labels: Dict[str, str],
        forecast_date: date,
    ) -> int:
        """Assign deterministic timestamps per forecasted business day."""
        start_dt = datetime.combine(forecast_date, datetime.min.time()).replace(tzinfo=timezone.utc)
        end_dt = datetime.combine(forecast_date, datetime.max.time()).replace(tzinfo=timezone.utc)

        label_pairs = [f'{k}="{v}"' for k, v in sorted(labels.items())]
        query = f'{metric_name}{{{",".join(label_pairs)}}}'

        try:
            query_result = prom.custom_query_range(
                query=query,
                start_time=start_dt,
                end_time=end_dt,
                step="1s",
            )

            max_ts = None
            if isinstance(query_result, dict):
                data = query_result.get("data", {})
                results = data.get("result", [])
            else:
                results = query_result or []

            for series in results:
                for value_pair in series.get("values", []):
                    if not isinstance(value_pair, (list, tuple)) or not value_pair:
                        continue
                    ts_value = int(float(value_pair[0]))
                    if max_ts is None or ts_value > max_ts:
                        max_ts = ts_value

            midnight_ts = int(start_dt.timestamp())
            if max_ts is None:
                return midnight_ts
            return min(max_ts + 1, int(end_dt.timestamp()))
        except Exception:
            return int(start_dt.timestamp())

    def _build_metric_line(
        self, metric_name: str, labels: Dict[str, str], value: float, timestamp: int
    ) -> str:
        label_pairs = [f'{key}="{value}"' for key, value in sorted(labels.items())]
        return f'{metric_name}{{{",".join(label_pairs)}}} {value} {timestamp}'


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

