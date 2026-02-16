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
    - metric_selectors: List of PromQL selector strings to process
    - selector_watermarks: Mapping of selector -> latest_timestamp processed
    - max_processed_timestamps: Mapping of selector -> max timestamp from input timeseries
    - metrics_processed: Count of metrics processed
    - metrics_converted: Count of metrics successfully converted
    - failed_count: Count of failed conversions
    - vm_query_url: URL to query VM
    - vm_gateway_url: URL to write metrics to VM
    - vm_token: Authentication token for VM
    """
    current_business_date: date = None
    metric_selectors: List[str] = None
    selector_watermarks: Dict[str, Optional[int]] = None
    max_processed_timestamps: Dict[str, Optional[int]] = None  # Track max timestamp per selector
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
            'metric_selectors': self.metric_selectors
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
            metric_selectors_raw = job_config.get('metric_selectors', [])
            
            # Handle both list and string formats
            if isinstance(metric_selectors_raw, str):
                # Parse comma-separated string or JSON array string
                import json
                try:
                    metric_selectors = json.loads(metric_selectors_raw)
                except:
                    # Fallback to comma-separated
                    metric_selectors = [s.strip() for s in metric_selectors_raw.split(',') if s.strip()]
            elif isinstance(metric_selectors_raw, list):
                metric_selectors = metric_selectors_raw
            else:
                metric_selectors = []
            
            if not metric_selectors:
                raise ValueError("metric_selectors must be configured")
            
            # Normalize selectors: ensure double quotes (PromQL requires double quotes)
            metric_selectors = [s.strip().replace("'", '"') for s in metric_selectors if s.strip()]
            
            initial_state = BusinessDateConverterState(
                job_id=job_id,
                job_config=job_config,
                started_at=datetime.now(),
                current_business_date=None,  # Will be updated in step 1
                metric_selectors=metric_selectors,
                selector_watermarks={},  # Will be populated in step 2
                max_processed_timestamps={},  # Will track max timestamp per selector
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
            self._query_and_convert_metrics,      # Step 3: Query updated metrics and convert
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
    
    # Step 2: Read selector watermarks
    def _read_job_watermarks(self, state: BusinessDateConverterState) -> Result[BusinessDateConverterState, Exception]:
        """Read selector watermarks from VictoriaMetrics to determine last processed timestamps.
        
        Uses last_over_time() to look back in time (default 30 days) to find the latest
        watermark value, even if it hasn't been updated recently.
        """
        try:
            import hashlib
            
            if not state.vm_query_url:
                self.logger.warning("No VM query URL configured, will process all metrics")
                state.selector_watermarks = {selector: None for selector in state.metric_selectors}
                return Ok(state)
            
            # Get lookback period from config (default 30 days)
            # This ensures we find watermarks even if they haven't been updated recently
            watermark_lookback_days = state.job_config.get('watermark_lookback_days', 30)
            lookback_duration = f'{watermark_lookback_days}d'
            
            # Initialize Prometheus client using helper method
            prom = self._get_prometheus_client(state)
            
            selector_watermarks = {}
            for selector in state.metric_selectors:
                try:
                    # Hash selector to create stable metric name for watermark
                    selector_hash = hashlib.md5(selector.encode()).hexdigest()
                    # Query for selector watermark metric using last_over_time() to look back in time
                    # This ensures we get the latest value even if it's old
                    watermark_metric = f'latest_converted_timestamp_by_selector_wm{{selector_hash="{selector_hash}"}}'
                    promql_query = f'last_over_time({watermark_metric}[{lookback_duration}])'
                    
                    # Query using PromQL instant query
                    query_result = prom.custom_query(query=promql_query)
                    
                    if query_result and len(query_result) > 0:
                        # Get the latest value
                        result = query_result[0]
                        if 'value' in result and result['value']:
                            timestamp = float(result['value'][1])  # Prometheus format: [timestamp, value]
                            selector_watermarks[selector] = int(timestamp)
                            self.logger.info(
                                f"Selector watermark for {selector}: {selector_watermarks[selector]} "
                                f"(found looking back {watermark_lookback_days} days)"
                            )
                        else:
                            selector_watermarks[selector] = None
                            self.logger.info(
                                f"No watermark found for selector {selector} "
                                f"(searched back {watermark_lookback_days} days)"
                            )
                    else:
                        selector_watermarks[selector] = None
                        self.logger.info(
                            f"No watermark found for selector {selector} "
                            f"(searched back {watermark_lookback_days} days)"
                        )
                        
                except Exception as e:
                    self.logger.warning(
                        f"Failed to read watermark for selector {selector} "
                        f"(lookback {watermark_lookback_days} days): {e}"
                    )
                    selector_watermarks[selector] = None
            
            state.selector_watermarks = selector_watermarks
            return Ok(state)
            
        except Exception as e:
            self.logger.error(f"Failed to read selector watermarks: {e}")
            return Err(e)
    
    # Step 3: Query updated metrics and convert
    def _query_and_convert_metrics(self, state: BusinessDateConverterState) -> Result[BusinessDateConverterState, Exception]:
        """Query updated original metrics and convert them."""
        try:
            if not state.vm_query_url:
                self.logger.error("No VM query URL configured")
                return Err(Exception("VM query URL not configured"))
            
            # Initialize Prometheus client using helper method
            prom = self._get_prometheus_client(state)
            
            # Process each selector
            for selector in state.metric_selectors:
                try:
                    self.logger.info(f"Processing metrics for selector: {selector}")
                    
                    # Get watermark for this selector
                    watermark_ts = state.selector_watermarks.get(selector)
                    
                    # Calculate time range: from watermark to now
                    if watermark_ts:
                        start_time = datetime.fromtimestamp(watermark_ts, tz=timezone.utc)
                    else:
                        # If no watermark, query from configured lookback period
                        lookback_days = state.job_config.get('watermark_lookback_days', 30)
                        # Calculate start_time as lookback_days ago from now
                        end_time = datetime.now(timezone.utc)
                        start_time = end_time - timedelta(days=lookback_days)
                        self.logger.info(f"No watermark found for selector {selector}, using lookback of {lookback_days} days from {end_time} to {start_time}")
                    
                    if watermark_ts:
                        end_time = datetime.now(timezone.utc)
                    
                    self.logger.info(f"Querying metrics for selector {selector} from {start_time} to {end_time}")
                    
                    # Query metrics using export API to get latest value per series+business_date with actual timestamps
                    grouped_metrics, max_timestamp_from_inputs = self._query_grouped_metrics(state, prom, selector, start_time, end_time)
                    
                    self.logger.info(f"Found {len(grouped_metrics)} unique metric series+business_date combinations for selector {selector}")
                    
                    # Store the max timestamp from all input timeseries for watermark
                    # This represents the latest timestamp seen on any input timeseries
                    # Only set if we actually found input data; clear if no data found
                    if max_timestamp_from_inputs is not None:
                        state.max_processed_timestamps[selector] = max_timestamp_from_inputs
                        self.logger.info(
                            f"Latest timestamp from all input timeseries for {selector}: {max_timestamp_from_inputs}"
                        )
                    else:
                        # Clear any previous value to prevent watermark update when no input found
                        state.max_processed_timestamps.pop(selector, None)
                        self.logger.info(
                            f"No input data found for selector {selector}, "
                            "cleared max_processed_timestamps to prevent watermark update"
                        )
                    
                    # Convert each metric
                    for (metric_name, labels_tuple, biz_date_str), (value, original_timestamp) in grouped_metrics.items():
                        try:
                            # Convert labels_tuple back to dict
                            labels_without_biz_date = dict(labels_tuple)
                            
                            # Extract job from labels (job label is always present)
                            job = labels_without_biz_date.get('job')
                            if not job:
                                self.logger.warning(f"Missing 'job' label in metric {metric_name}, skipping")
                                continue
                            
                            # Parse biz_date - format is "dd/mm/yyyy"
                            try:
                                business_date = datetime.strptime(biz_date_str, '%d/%m/%Y').date()
                            except ValueError:
                                self.logger.warning(f"Invalid biz_date format: {biz_date_str}, expected dd/mm/yyyy")
                                continue
                            
                            # Note: max_timestamp is already tracked in _query_grouped_metrics
                            # and stored in state.max_processed_timestamps above
                            
                            # Calculate converted timestamp
                            converted_ts = self._calculate_converted_timestamp(
                                state, prom, metric_name, business_date, labels_without_biz_date
                            )
                            
                            # Transform and write metric
                            success = self._write_converted_metric(
                                state, metric_name, labels_without_biz_date,
                                business_date, value, converted_ts
                            )
                            
                            if success:
                                state.metrics_converted += 1
                            else:
                                state.failed_count += 1
                            
                            state.metrics_processed += 1
                                
                        except Exception as e:
                            self.logger.error(f"Failed to convert metric {metric_name} for {biz_date_str}: {e}")
                            state.failed_count += 1
                            state.metrics_processed += 1
                    
                except Exception as e:
                    self.logger.error(f"Failed to process selector {selector}: {e}")
                    state.failed_count += 1
            
            self.logger.info(f"Conversion complete: {state.metrics_converted} converted, {state.failed_count} failed")
            return Ok(state)
            
        except Exception as e:
            self.logger.error(f"Failed to query and convert metrics: {e}")
            return Err(e)
    
    def _query_grouped_metrics(
        self, state: BusinessDateConverterState, prom: PrometheusConnect, selector: str, start_time: datetime, end_time: datetime
    ) -> Tuple[Dict[Tuple[str, tuple, str], Tuple[float, float]], Optional[int]]:
        """Query metrics from VM using export API to get latest value per series+business_date with actual timestamps.
        
        Uses Victoria Metrics /api/v1/export endpoint to get raw data points with actual timestamps.
        Groups metrics locally by series+business_date and keeps the latest value per group based on actual timestamp.
        
        Args:
            state: Job state
            prom: Prometheus client
            selector: PromQL selector string
            start_time: Start time for query
            end_time: End time for query
        
        Returns:
            Tuple of:
            - dict: {(metric_name, labels_tuple, biz_date_str): (value, timestamp)}
              where labels_tuple is tuple(sorted(labels.items())) for hashability
            - max_timestamp: Maximum actual timestamp seen across all input timeseries (for watermark)
        """
        grouped = {}
        max_timestamp = None
        
        try:
            if not state.vm_query_url:
                self.logger.error("No VM query URL configured")
                return {}, None
            
            # Ensure we have a positive time range
            if start_time >= end_time:
                self.logger.warning(f"Invalid time range: start_time={start_time}, end_time={end_time}")
                return {}, None
            
            # Get session from Prometheus client
            session = prom._session
            
            # Build export API URL
            # Extract base URL (remove /api/v1 if present)
            base_url = state.vm_query_url.rstrip("/")
            if base_url.endswith("/api/v1"):
                base_url = base_url[:-7]
            elif base_url.endswith("/api"):
                base_url = base_url[:-4]
            
            # Prepare headers
            headers = {}
            if state.vm_token:
                headers["Authorization"] = f"Bearer {state.vm_token}"
            
            # Build export query parameters
            # The export endpoint uses match[] parameter for metric selector
            # Normalize selector: ensure double quotes (PromQL requires double quotes)
            base_query = selector.strip().replace("'", '"')
            params = {
                "match[]": base_query,
                "start": int(start_time.timestamp()),
                "end": int(end_time.timestamp()),
            }
            
            # Make GET request to export endpoint
            response = session.get(
                f"{base_url}/api/v1/export",
                params=params,
                headers=headers,
                timeout=30,
            )
            response.raise_for_status()
            
            # Parse export response
            # Export format: newline-delimited JSON
            # Each line: {"metric": {...}, "values": [value1, value2, ...], "timestamps": [timestamp1_ms, timestamp2_ms, ...]}
            # Values and timestamps are separate arrays, aligned by index
            content = response.text
            
            # Parse newline-delimited JSON format
            for line in content.strip().split("\n"):
                if not line.strip():
                    continue
                try:
                    data_point = json.loads(line)
                    
                    # Export format: {"metric": {...}, "values": [...], "timestamps": [...]}
                    metric_data = data_point.get("metric", {})
                    values = data_point.get("values", [])
                    timestamps_ms = data_point.get("timestamps", [])
                    
                    # Extract metric name
                    metric_name = metric_data.get("__name__", "")
                    if not metric_name:
                        continue
                    
                    # Extract labels
                    labels = {k: v for k, v in metric_data.items() if k != "__name__"}
                    
                    # Extract biz_date
                    biz_date_str = labels.pop("biz_date", None)
                    if not biz_date_str:
                        continue
                    
                    # Process all data points for this series
                    # Values and timestamps arrays are aligned by index
                    for value_raw, timestamp_ms_raw in zip(values, timestamps_ms):
                        try:
                            value = float(value_raw)
                            timestamp_ms = float(timestamp_ms_raw)
                            timestamp = int(timestamp_ms / 1000)  # Convert from milliseconds to seconds
                            
                            # Track max timestamp seen across all input timeseries
                            if max_timestamp is None or timestamp > max_timestamp:
                                max_timestamp = timestamp
                            
                            # Create key: (metric_name, labels_without_biz_date, biz_date)
                            # Convert labels dict to tuple of sorted items for hashability
                            labels_tuple = tuple(sorted(labels.items()))
                            key = (metric_name, labels_tuple, biz_date_str)
                            
                            # Store value and timestamp
                            # If key already exists, keep the one with latest timestamp
                            if key not in grouped or timestamp > grouped[key][1]:
                                grouped[key] = (value, timestamp)
                                
                        except (ValueError, TypeError, IndexError) as e:
                            # Skip invalid data points
                            continue
                    
                except (json.JSONDecodeError, ValueError, KeyError, TypeError) as e:
                    # Skip invalid lines
                    continue
            
        except Exception as e:
            self.logger.error(f"Failed to query grouped metrics: {e}")
            # Return empty dict on error
        
        return grouped, int(max_timestamp) if max_timestamp is not None else None
    
    def _calculate_converted_timestamp(
        self, state: BusinessDateConverterState, prom: PrometheusConnect,
        metric_name: str, business_date: date, labels_without_biz_date: Dict[str, str]
    ) -> int:
        """Calculate converted timestamp for a metric+business_date combination.
        
        Queries the converted series itself using range queries to find the max existing timestamp.
        If no entry exists, returns 00:00:00.000 of the converted date.
        If entry exists, returns max existing timestamp + 1 second offset.
        """
        try:
            # Extract job from labels (job label is always present)
            job = labels_without_biz_date.get('job')
            if not job:
                self.logger.warning(f"Missing 'job' label when calculating timestamp for {metric_name}, using midnight")
                midnight = datetime.combine(business_date, datetime.min.time()).replace(tzinfo=timezone.utc)
                return int(midnight.timestamp())
            
            # Build query for the converted metric series (same as how it's written)
            # Converted metrics have: same metric_name, labels without biz_date, job label kept as-is, timeseries_type="converted"
            converted_labels = labels_without_biz_date.copy()
            converted_labels.pop('biz_date', None)  # Remove biz_date if present
            # job label is always present on input, keep it as-is
            converted_labels['timeseries_type'] = 'converted'
            
            # Build PromQL query for the converted metric series
            label_pairs = [f'{k}="{v}"' for k, v in sorted(converted_labels.items())]
            converted_metric_query = f'{metric_name}{{{",".join(label_pairs)}}}'
            
            # Calculate time range for the business_date day (00:00:00 to 23:59:59 UTC)
            start_datetime = datetime.combine(business_date, datetime.min.time()).replace(tzinfo=timezone.utc)
            end_datetime = datetime.combine(business_date, datetime.max.time().replace(microsecond=0)).replace(tzinfo=timezone.utc)
            
            start_timestamp = int(start_datetime.timestamp())
            end_timestamp = int(end_datetime.timestamp())
            
            # Query the converted series using export API to get all data points for the day
            max_existing_timestamp = self._query_export_max_timestamp(
                state, prom, converted_metric_query, start_datetime, end_datetime, business_date
            )
            
            # Calculate timestamp
            midnight_ts = start_timestamp  # 00:00:00.000 of the converted date
            
            if max_existing_timestamp is None:
                # No entry exists: return 00:00:00.000 of the converted date
                return midnight_ts
            
            # Entry exists: max existing timestamp + 1 second offset
            next_ts = max_existing_timestamp + 1
            
            # Ensure it stays in the same day (cap at 23:59:59)
            return min(next_ts, end_timestamp)
            
        except Exception as e:
            self.logger.warning(f"Failed to calculate converted timestamp, using midnight: {e}")
            midnight = datetime.combine(business_date, datetime.min.time()).replace(tzinfo=timezone.utc)
            return int(midnight.timestamp())
    
    def _query_export_max_timestamp(
        self,
        state: BusinessDateConverterState,
        prom: PrometheusConnect,
        query: str,
        start_dt: datetime,
        end_dt: datetime,
        business_date: date,
    ) -> Optional[int]:
        """Query VictoriaMetrics /api/v1/export endpoint to get raw data points with actual timestamps.
        
        The export endpoint returns actual data points without step sampling, allowing us to
        find the real max timestamp of written data points.
        
        Args:
            state: Job state with VM configuration
            prom: Prometheus client (used to get session)
            query: PromQL query string (metric with labels)
            start_dt: Start datetime for the query
            end_dt: End datetime for the query
            business_date: Business date to filter results
            
        Returns:
            Max timestamp found, or None if no data exists
        """
        try:
            if not state.vm_query_url:
                return None
            
            # Get session from Prometheus client
            session = prom._session
            
            # Build export API URL
            # Extract base URL (remove /api/v1 if present)
            base_url = state.vm_query_url.rstrip("/")
            if base_url.endswith("/api/v1"):
                base_url = base_url[:-7]
            elif base_url.endswith("/api"):
                base_url = base_url[:-4]
            
            # Prepare headers
            headers = {}
            if state.vm_token:
                headers["Authorization"] = f"Bearer {state.vm_token}"
            
            # Build export query parameters
            # The export endpoint uses match[] parameter for metric selector
            params = {
                "match[]": query,
                "start": int(start_dt.timestamp()),
                "end": int(end_dt.timestamp()),
            }
            
            # Make GET request to export endpoint
            response = session.get(
                f"{base_url}/api/v1/export",
                params=params,
                headers=headers,
                timeout=30,
            )
            response.raise_for_status()
            
            # Parse export response
            # Export format: newline-delimited JSON
            # Each line: {"metric": {...}, "values": [value1, value2, ...], "timestamps": [timestamp1_ms, timestamp2_ms, ...]}
            # Values and timestamps are separate arrays, aligned by index
            max_ts = None
            content = response.text
            
            # Parse newline-delimited JSON format
            for line in content.strip().split("\n"):
                if not line.strip():
                    continue
                try:
                    data_point = json.loads(line)
                    
                    # Export format: {"metric": {...}, "values": [...], "timestamps": [...]}
                    timestamps = data_point.get("timestamps", [])
                    
                    # Process timestamps array (values are not needed for timestamp lookup)
                    for timestamp_ms_raw in timestamps:
                        timestamp_ms = float(timestamp_ms_raw)
                        timestamp = int(timestamp_ms / 1000)  # Convert from milliseconds to seconds
                        
                        ts_dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
                        if ts_dt.date() == business_date:
                            if max_ts is None or timestamp > max_ts:
                                max_ts = timestamp
                                
                except (json.JSONDecodeError, ValueError, KeyError, TypeError, IndexError):
                    # Skip invalid lines
                    continue
            
            return max_ts
            
        except Exception as exc:
            self.logger.warning(
                "Failed to query export endpoint for timestamp lookup: %s",
                exc,
            )
            return None
    
    def _get_prometheus_client(self, state: BusinessDateConverterState) -> PrometheusConnect:
        """Get or create a PrometheusConnect instance for querying and writing metrics.
        
        Args:
            state: Job state with VM configuration
            
        Returns:
            PrometheusConnect instance configured with VM settings
        """
        headers = {}
        if state.vm_token:
            headers['Authorization'] = f'Bearer {state.vm_token}'
        
        # Use query URL if available, otherwise gateway URL
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
        """Write converted metric to Victoria Metrics."""
        try:
            # Extract job from labels (job label is always present)
            job = labels_without_biz_date.get('job')
            if not job:
                self.logger.error(f"Missing 'job' label when writing converted metric {metric_name}")
                return False
            
            # Copy labels and transform
            labels_dict = labels_without_biz_date.copy()
            
            # Remove biz_date if present (it's already removed, but check for safety)
            # job label is always present on input, keep it as-is
            labels_dict.pop('biz_date', None)
            labels_dict['timeseries_type'] = 'converted'
            
            # Build metric line in Prometheus format
            label_pairs = [f'{k}="{v}"' for k, v in sorted(labels_dict.items())]
            metric_line = f'{metric_name}{{{",".join(label_pairs)}}} {value} {timestamp}'
            
            # Write to VM gateway using Prometheus client session
            return self._write_metric_to_vm(state, metric_line, timeout=60)
            
        except Exception as e:
            self.logger.error(f"Failed to write converted metric: {e}")
            return False
    
    # Step 4: Update job watermarks
    def _update_job_watermarks(self, state: BusinessDateConverterState) -> Result[BusinessDateConverterState, Exception]:
        """Update selector watermarks with latest processed timestamps.
        
        Only updates watermarks if we actually found input data (max_ts is not None).
        This prevents updating the watermark when no new input data was found.
        """
        try:
            import hashlib
            
            if not state.vm_gateway_url:
                self.logger.warning("No VM gateway URL configured, skipping watermark update")
                return Ok(state)
            
            # Update watermarks with max processed timestamps
            for selector in state.metric_selectors:
                try:
                    # Only update watermark if we found input data
                    max_ts = state.max_processed_timestamps.get(selector)
                    if max_ts is None:
                        self.logger.info(
                            f"No input data found for selector {selector}, "
                            "skipping watermark update"
                        )
                        continue
                    
                    # Hash selector to create stable metric name for watermark (same as in _read_job_watermarks)
                    selector_hash = hashlib.md5(selector.encode()).hexdigest()
                    watermark_line = f'latest_converted_timestamp_by_selector_wm{{selector_hash="{selector_hash}"}} {max_ts} {max_ts}'
                    
                    # Write watermark using Prometheus client session
                    if self._write_metric_to_vm(state, watermark_line, timeout=30):
                        self.logger.info(f"Updated selector watermark for {selector} to {max_ts}")
                    else:
                        raise Exception("Failed to write watermark metric")
                    
                except Exception as e:
                    self.logger.warning(f"Failed to update selector watermark for {selector}: {e}")
            
            return Ok(state)
            
        except Exception as e:
            self.logger.error(f"Failed to update selector watermarks: {e}")
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

