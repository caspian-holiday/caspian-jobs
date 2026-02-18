#!/usr/bin/env python3
"""
Business Date Converter Job - Converts metrics with biz_date labels to timestamps
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, date, timedelta, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
from prometheus_api_client import PrometheusConnect

# Add the scheduler module to the path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from victoria_metrics_jobs.jobs.common import BaseJob, BaseJobState, Result, Ok, Err


@dataclass
class BusinessDateConverterState(BaseJobState):
    """State object for business date converter job execution.
    
    This state object extends BaseJobState and adds converter-specific fields
    that are passed through the functional pipeline and accumulate data as
    each step is executed. The state contains:
    
    - current_business_date: Derived business date for processing (UTC)
    - jobs: List of job names to process (selector {job="X", biz_date!=""} per job)
    - source_watermarks: Mapping of job -> last processed input timestamp in ms
    - max_processed_timestamps: Mapping of job -> max timestamp from inputs (for source watermark)
    - metrics_processed: Count of metrics processed
    - metrics_converted: Count of metrics successfully converted
    - failed_count: Count of failed conversions
    - vm_query_url: URL for VM (export and watermark reads)
    - vm_gateway_url: URL to write metrics to VM
    - vm_token: Authentication token for VM
    """
    current_business_date: date = None
    jobs: List[str] = None
    source_watermarks: Dict[str, Optional[int]] = None  # job -> wm_ms
    max_processed_timestamps: Dict[str, Optional[int]] = None  # job -> max ts (ms)
    metrics_processed: int = 0
    metrics_converted: int = 0
    failed_count: int = 0
    vm_query_url: str = ""
    vm_gateway_url: str = ""
    vm_token: str = ""
    
    def to_results(self) -> Dict[str, Any]:
        """Convert state to job results dictionary with converter-specific fields."""
        results = super().to_results()
        results.update({
            'metrics_processed': self.metrics_processed,
            'metrics_converted': self.metrics_converted,
            'failed_count': self.failed_count,
            'success_rate': self.metrics_converted / max(self.metrics_processed, 1) * 100,
            'current_business_date': self.current_business_date.isoformat() if self.current_business_date else None,
            'jobs': self.jobs,
        })
        return results


class BusinessDateConverterJob(BaseJob):
    """Business date converter job class with step-by-step workflow."""
    
    def __init__(self, config_path: str = None, verbose: bool = False):
        """Initialize the business date converter job.
        
        Args:
            config_path: Path to configuration file
            verbose: Whether to enable verbose logging
        """
        super().__init__('business_date_converter', config_path, verbose)
    
    def create_initial_state(self, job_id: str) -> Result[BusinessDateConverterState, Exception]:
        """Create the initial state for business date converter job execution.
        
        Args:
            job_id: Job ID to use for configuration selection
        
        Returns:
            Result containing the initial state or an error
        """
        try:
            job_config = self.get_job_config(job_id)
            jobs_list = job_config.get('jobs', [])
            if isinstance(jobs_list, str):
                jobs_list = [s.strip() for s in jobs_list.split(',') if s.strip()]
            if not jobs_list:
                raise ValueError("jobs must be configured (list of job names)")
            initial_state = BusinessDateConverterState(
                job_id=job_id,
                job_config=job_config,
                started_at=datetime.now(),
                current_business_date=None,
                jobs=jobs_list,
                source_watermarks={},
                max_processed_timestamps={},
                metrics_processed=0,
                metrics_converted=0,
                failed_count=0,
                vm_query_url=job_config.get('victoria_metrics', {}).get('query_url', ''),
                vm_gateway_url=job_config.get('victoria_metrics', {}).get('gateway_url', ''),
                vm_token=job_config.get('victoria_metrics', {}).get('token', '')
            )
            return Ok(initial_state)
        except Exception as e:
            return Err(e)
    
    def get_workflow_steps(self) -> List[callable]:
        """Get the list of workflow steps for the business date converter job.
        
        Returns:
            List of step functions that take a state and return Result[State, Exception]
        """
        return [
            self._derive_current_business_date,   # Step 1: Derive current business date
            self._read_job_watermarks,            # Step 2: Read job watermarks
            self._query_and_convert_metrics,      # Step 3: Export bd-aware metrics and convert
            self._update_job_watermarks,          # Step 4: Update job watermarks
            self._publish_job_status_metric       # Step 5: Publish job status metric
        ]
    
    def finalize_state(self, state: BusinessDateConverterState) -> BusinessDateConverterState:
        """Finalize the business date converter state before converting to results.
        
        Args:
            state: The final state after all steps
            
        Returns:
            Finalized state ready for conversion to results
        """
        state.completed_at = datetime.now()
        
        # Determine status based on processing results
        if state.failed_count > 0 and state.metrics_converted == 0:
            state.status = 'error'
            state.message = f'Business date conversion failed for job: {state.job_id} - {state.failed_count} failures, 0 successes'
        elif state.failed_count > 0:
            state.status = 'partial_success'
            state.message = f'Business date conversion completed with warnings for job: {state.job_id} - {state.metrics_converted} successes, {state.failed_count} failures'
        else:
            state.status = 'success'
            state.message = f'Business date conversion completed for job: {state.job_id} - {state.metrics_converted} metrics converted'
        
        return state
    
    # Step 1: Derive current business date
    def _derive_current_business_date(self, state: BusinessDateConverterState) -> Result[BusinessDateConverterState, Exception]:
        """Derive current business date from configuration (UTC timezone)."""
        try:
            # Get cutoff hour from config (default 6:00 UTC)
            cutoff_hour = state.job_config.get('cutoff_hour', 6)
            
            # Current UTC time
            now = datetime.utcnow()
            
            # If before cutoff or weekend, use previous business day
            if now.hour < cutoff_hour or now.weekday() >= 5:  # 5=Saturday, 6=Sunday
                # Calculate previous business day
                days_back = 1
                if now.weekday() == 5:  # Saturday
                    days_back = 1  # Friday
                elif now.weekday() == 6:  # Sunday
                    days_back = 2  # Friday
                elif now.hour < cutoff_hour and now.weekday() == 0:  # Monday before cutoff
                    days_back = 3  # Friday
                
                state.current_business_date = (now - timedelta(days=days_back)).date()
            else:
                # Use today if it's a weekday and after cutoff
                state.current_business_date = now.date()
            
            self.logger.info(f"Current business date: {state.current_business_date}")
            return Ok(state)
            
        except Exception as e:
            self.logger.error(f"Failed to derive current business date: {e}")
            return Err(e)
    
    # Step 2: Read job watermarks (v2: source watermark per job)
    def _read_job_watermarks(self, state: BusinessDateConverterState) -> Result[BusinessDateConverterState, Exception]:
        """Read source watermarks from VictoriaMetrics: business_date_converter_source_wm{job="X"} (value = wm_ms)."""
        try:
            if not state.vm_query_url:
                self.logger.warning("No VM query URL configured, will process all metrics")
                state.source_watermarks = {j: None for j in state.jobs}
                return Ok(state)
            watermark_lookback_days = state.job_config.get('watermark_lookback_days', 30)
            lookback_duration = f'{watermark_lookback_days}d'
            prom = self._get_prometheus_client(state)
            source_watermarks = {}
            for job in state.jobs:
                try:
                    wm_metric = f'business_date_converter_source_wm{{job="{job}"}}'
                    promql_query = f'last_over_time({wm_metric}[{lookback_duration}])'
                    query_result = prom.custom_query(query=promql_query)
                    if query_result and len(query_result) > 0 and query_result[0].get('value'):
                        val = query_result[0]['value'][1]
                        source_watermarks[job] = int(float(val))
                        self.logger.info(f"Source watermark for job {job}: {source_watermarks[job]} ms")
                    else:
                        source_watermarks[job] = None
                        self.logger.info(f"No source watermark for job {job}")
                except Exception as e:
                    self.logger.warning(f"Failed to read source watermark for job {job}: {e}")
                    source_watermarks[job] = None
            state.source_watermarks = source_watermarks
            return Ok(state)
        except Exception as e:
            self.logger.error(f"Failed to read job watermarks: {e}")
            return Err(e)
    
    # Step 3: Export bd-aware metrics and convert with per-day watermark
    def _query_and_convert_metrics(self, state: BusinessDateConverterState) -> Result[BusinessDateConverterState, Exception]:
        """Fetch bd-aware metrics via single export call (overlap window), filter by source watermark, allocate output ts with per-day watermark."""
        try:
            if not state.vm_query_url:
                self.logger.error("No VM query URL configured")
                return Err(Exception("VM query URL not configured"))
            overlap_s = self._parse_duration_seconds(state.job_config.get('source_overlap_duration', '10m'), 600)
            initial_lookback_s = self._parse_duration_seconds(state.job_config.get('source_initial_lookback', '30d'), 30 * 86400)
            prom = self._get_prometheus_client(state)
            for job in state.jobs:
                try:
                    self.logger.info(f"Processing job: {job}")
                    wm_ms = state.source_watermarks.get(job) if state.source_watermarks else None
                    now_s = int(datetime.now(timezone.utc).timestamp())
                    if wm_ms is not None:
                        wm_s = wm_ms // 1000
                        window_s = max(1, now_s - wm_s + overlap_s)
                    else:
                        window_s = initial_lookback_s
                    start_s = now_s - window_s
                    end_s = now_s
                    selector = f'{{job="{job}", biz_date!=""}}'
                    to_write = self._query_series_to_convert(state, prom, selector, wm_ms, start_s, end_s)
                    self.logger.info(f"Job {job}: {len(to_write)} series after watermark filter")
                    if not to_write:
                        state.max_processed_timestamps.pop(job, None)
                        continue
                    to_write.sort(key=lambda x: (x[0][2], x[2]))
                    max_ts_ms: Optional[int] = None
                    current_bd: Optional[date] = None
                    day_start_s = day_end_s = 0
                    biz_date_iso = ""
                    watermark = 0
                    for (metric_name, labels_tuple, biz_date_str), value, ts_ms in to_write:
                        try:
                            bd = datetime.strptime(biz_date_str, '%d/%m/%Y').date()
                        except ValueError:
                            self.logger.warning(f"Invalid biz_date {biz_date_str}, skipping")
                            continue
                        if bd != current_bd:
                            if current_bd is not None:
                                self._write_day_watermark(state, job, biz_date_iso, watermark)
                            current_bd = bd
                            day_start_s = int(datetime.combine(bd, datetime.min.time()).replace(tzinfo=timezone.utc).timestamp())
                            day_end_s = day_start_s + 86399
                            biz_date_iso = bd.isoformat()
                            watermark = self._read_day_watermark(state, prom, job, biz_date_iso, day_start_s)
                        watermark = max(watermark + 1, day_start_s)
                        output_ts = min(watermark, day_end_s)
                        labels_without_biz_date = dict(labels_tuple)
                        if self._write_converted_metric(state, metric_name, labels_without_biz_date, bd, value, output_ts):
                            state.metrics_converted += 1
                        else:
                            state.failed_count += 1
                        state.metrics_processed += 1
                        max_ts_ms = max(ts_ms, max_ts_ms or 0)
                    if current_bd is not None:
                        self._write_day_watermark(state, job, biz_date_iso, watermark)
                    state.max_processed_timestamps[job] = max_ts_ms
                except Exception as e:
                    self.logger.error(f"Failed to process job {job}: {e}")
                    state.failed_count += 1
            self.logger.info(f"Conversion complete: {state.metrics_converted} converted, {state.failed_count} failed")
            return Ok(state)
        except Exception as e:
            self.logger.error(f"Failed to query and convert metrics: {e}")
            return Err(e)

    def _parse_duration_seconds(self, s: str, default: int) -> int:
        """Parse duration string like 10m, 30d into seconds."""
        s = (s or "").strip().lower()
        if not s:
            return default
        try:
            if s.endswith("d"):
                return int(s[:-1]) * 86400
            if s.endswith("m"):
                return int(s[:-1]) * 60
            if s.endswith("h"):
                return int(s[:-1]) * 3600
            return int(s)
        except ValueError:
            return default

    def _query_series_to_convert(
        self,
        state: BusinessDateConverterState,
        prom: PrometheusConnect,
        selector: str,
        wm_ms: Optional[int],
        start_s: int,
        end_s: int,
    ) -> List[Tuple[Tuple[str, tuple, str], float, int]]:
        """Single export query: GET /api/v1/export for selector in [start_s, end_s]; parse each line, take latest point per series, filter ts_ms > wm_ms. Returns list of (key, value, ts_ms)."""
        to_write: List[Tuple[Tuple[str, tuple, str], float, int]] = []
        try:
            base_url = (state.vm_query_url or state.vm_gateway_url or "").rstrip("/")
            if base_url.endswith("/api/v1"):
                base_url = base_url[:-7]
            elif base_url.endswith("/api"):
                base_url = base_url[:-4]
            headers = {}
            if state.vm_token:
                headers["Authorization"] = f"Bearer {state.vm_token}"
            params = {"match[]": selector, "start": start_s, "end": end_s}
            session = prom._session
            resp = session.get(f"{base_url}/api/v1/export", params=params, headers=headers, timeout=60)
            resp.raise_for_status()
            for line in resp.text.strip().split("\n"):
                if not line.strip():
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                metric = row.get("metric", {})
                name = metric.get("__name__", "")
                if not name:
                    continue
                labels = {k: v for k, v in metric.items() if k != "__name__"}
                biz_date_str = labels.get("biz_date", "")
                if not biz_date_str:
                    continue
                labels_wo_biz = tuple(sorted((k, v) for k, v in labels.items() if k != "biz_date"))
                values = row.get("values", [])
                timestamps = row.get("timestamps", [])
                if not values or not timestamps:
                    continue
                value = float(values[-1])
                ts_ms = int(float(timestamps[-1]))
                if wm_ms is not None and ts_ms <= wm_ms:
                    continue
                key = (name, labels_wo_biz, biz_date_str)
                to_write.append((key, value, ts_ms))
        except Exception as e:
            self.logger.error(f"Export query failed: {e}")
        return to_write

    def _read_day_watermark(
        self, state: BusinessDateConverterState, prom: PrometheusConnect, job: str, biz_date_iso: str, day_start_s: int
    ) -> int:
        """Read business_date_converter_day_wm{job=..., biz_date=...}; return day_start_s - 1 if missing."""
        try:
            q = f'last_over_time(business_date_converter_day_wm{{job="{job}", biz_date="{biz_date_iso}"}}[30d])'
            res = prom.custom_query(query=q)
            if res and len(res) > 0 and res[0].get("value"):
                return int(float(res[0]["value"][1]))
        except Exception as e:
            self.logger.debug(f"Read day watermark for {job} {biz_date_iso}: {e}")
        return day_start_s - 1

    def _write_day_watermark(self, state: BusinessDateConverterState, job: str, biz_date_iso: str, watermark_s: int) -> None:
        """Write business_date_converter_day_wm{job=..., biz_date=...} = watermark_s."""
        line = f'business_date_converter_day_wm{{biz_date="{biz_date_iso}",job="{job}"}} {watermark_s} {watermark_s}'
        self._write_metric_to_vm(state, line, timeout=30)
    
    def _get_prometheus_client(self, state: BusinessDateConverterState) -> PrometheusConnect:
        """Get or create a PrometheusConnect instance (session used for export GET and watermark instant queries)."""
        headers = {}
        if state.vm_token:
            headers['Authorization'] = f'Bearer {state.vm_token}'
        url = state.vm_query_url or state.vm_gateway_url
        return PrometheusConnect(url=url, headers=headers, disable_ssl=True)
    
    def _write_metric_to_vm(
        self, state: BusinessDateConverterState, metric_line: str, timeout: int = 60
    ) -> bool:
        """Write metric line to VictoriaMetrics using PrometheusConnect session.
        
        Uses PrometheusConnect's internal session to write metrics to VictoriaMetrics'
        custom import endpoint. This provides a consistent Prometheus client API approach.
        
        Args:
            state: Job state with VM configuration
            metric_line: Metric line in Prometheus format
            timeout: Request timeout in seconds
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if not state.vm_gateway_url:
                self.logger.error("No VM gateway URL configured")
                return False
            
            # Get PrometheusConnect instance - it maintains a session internally
            prom = self._get_prometheus_client(state)
            
            # Access the session from PrometheusConnect for writing
            # prometheus-api-client 0.6.0 stores session as _session (private attribute)
            session = prom._session
            
            # Prepare headers for writing (VM-specific endpoint)
            write_headers = {'Content-Type': 'text/plain'}
            if state.vm_token:
                write_headers['Authorization'] = f'Bearer {state.vm_token}'
            
            # Use session to write metrics
            response = session.post(
                f"{state.vm_gateway_url}/api/v1/import/prometheus",
                data=metric_line,
                headers=write_headers,
                timeout=timeout
            )
            response.raise_for_status()
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to write metric to VM: {e}")
            return False
    
    def _write_converted_metric(
        self, state: BusinessDateConverterState, metric_name: str,
        labels_without_biz_date: Dict[str, str], business_date: date,
        value: float, timestamp: int
    ) -> bool:
        """Write converted metric to Victoria Metrics. Target: biz_date removed, job value becomes job=<original>_converted, all other labels preserved. No new labels added."""
        try:
            job_value = labels_without_biz_date.get('job')
            if not job_value:
                self.logger.error(f"Missing 'job' label when writing converted metric {metric_name}")
                return False
            labels_dict = labels_without_biz_date.copy()
            labels_dict.pop('biz_date', None)
            labels_dict['job'] = f'{job_value}_converted'
            # Build metric line in Prometheus format
            label_pairs = [f'{k}="{v}"' for k, v in sorted(labels_dict.items())]
            metric_line = f'{metric_name}{{{",".join(label_pairs)}}} {value} {timestamp}'
            
            # Write to VM gateway using Prometheus client session
            return self._write_metric_to_vm(state, metric_line, timeout=60)
            
        except Exception as e:
            self.logger.error(f"Failed to write converted metric: {e}")
            return False
    
    # Step 4: Update job watermarks (source watermark per job, in ms)
    def _update_job_watermarks(self, state: BusinessDateConverterState) -> Result[BusinessDateConverterState, Exception]:
        """Update source watermarks: business_date_converter_source_wm{job="X"} = max_ts_ms (value and sample ts in ms)."""
        try:
            if not state.vm_gateway_url:
                self.logger.warning("No VM gateway URL configured, skipping watermark update")
                return Ok(state)
            for job in state.jobs:
                max_ts_ms = state.max_processed_timestamps.get(job)
                if max_ts_ms is None:
                    self.logger.info(f"No input data found for job {job}, skipping source watermark update")
                    continue
                watermark_line = f'business_date_converter_source_wm{{job="{job}"}} {max_ts_ms} {max_ts_ms // 1000}'
                if self._write_metric_to_vm(state, watermark_line, timeout=30):
                    self.logger.info(f"Updated source watermark for job {job} to {max_ts_ms} ms")
                else:
                    self.logger.warning(f"Failed to write source watermark for job {job}")
            return Ok(state)
        except Exception as e:
            self.logger.error(f"Failed to update job watermarks: {e}")
            return Err(e)
    
    # Step 5: Publish job status metric
    def _publish_job_status_metric(self, state: BusinessDateConverterState) -> Result[BusinessDateConverterState, Exception]:
        """Publish job status metric to VictoriaMetrics for monitoring."""
        try:
            if not state.vm_gateway_url:
                return Ok(state)
            
            # Create job status metric
            status_value = 1 if state.status == 'success' else 0
            timestamp = int(datetime.utcnow().timestamp())
            
            # Get labels from config
            env = state.job_config.get('env', 'default')
            labels = state.job_config.get('labels', {})
            
            # Build metric line
            label_pairs = [
                f'job_id="{state.job_id}"',
                f'status="{state.status}"',
                f'env="{env}"'
            ]
            
            # Add custom labels
            for key, value in labels.items():
                label_pairs.append(f'{key}="{value}"')
            
            metric_line = f'business_date_converter_job_status{{{",".join(label_pairs)}}} {status_value} {timestamp}'
            
            # Send to VM gateway using Prometheus client session
            if self._write_metric_to_vm(state, metric_line, timeout=30):
                self.logger.info(f"Published job status metric: {state.status}")
            
            return Ok(state)
            
        except Exception as e:
            self.logger.warning(f"Failed to publish job status metric: {e}")
            return Ok(state)  # Don't fail the job if status metric fails


def main():
    """Main function for command-line execution."""
    epilog = """
Examples:
  # List available job configurations
  python business_date_converter.py --config config.yml --list-jobs
  
  # Run business date conversion for business_date_converter
  python business_date_converter.py --config config.yml --job-id business_date_converter
  
  # Run with verbose logging
  python business_date_converter.py --config config.yml --job-id business_date_converter --verbose
    """
    
    return BusinessDateConverterJob.main(
        description="Business Date Converter Job - Convert metrics with business_date labels to timestamps",
        epilog=epilog
    )


if __name__ == "__main__":
    sys.exit(main())

