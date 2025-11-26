#!/usr/bin/env python3
"""
Business Date Converter Job - Converts metrics with biz_date labels to timestamps
"""

from __future__ import annotations

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
    - source_job_names: List of job names to process
    - job_watermarks: Mapping of job_name -> latest_timestamp processed
    - metrics_processed: Count of metrics processed
    - metrics_converted: Count of metrics successfully converted
    - failed_count: Count of failed conversions
    - vm_query_url: URL to query VM
    - vm_gateway_url: URL to write metrics to VM
    - vm_token: Authentication token for VM
    """
    current_business_date: date = None
    source_job_names: List[str] = None
    job_watermarks: Dict[str, Optional[int]] = None
    max_processed_timestamps: Dict[str, Optional[int]] = None  # Track max timestamp per job
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
            'source_job_names': self.source_job_names
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
            source_job_names_raw = job_config.get('source_job_names', [])
            
            # Handle both list and string formats
            if isinstance(source_job_names_raw, str):
                # Parse comma-separated string or JSON array string
                import json
                try:
                    source_job_names = json.loads(source_job_names_raw)
                except:
                    # Fallback to comma-separated
                    source_job_names = [s.strip() for s in source_job_names_raw.split(',') if s.strip()]
            elif isinstance(source_job_names_raw, list):
                source_job_names = source_job_names_raw
            else:
                source_job_names = []
            
            if not source_job_names:
                raise ValueError("source_job_names must be configured")
            
            initial_state = BusinessDateConverterState(
                job_id=job_id,
                job_config=job_config,
                started_at=datetime.now(),
                current_business_date=None,  # Will be updated in step 1
                source_job_names=source_job_names,
                job_watermarks={},  # Will be populated in step 2
                max_processed_timestamps={},  # Will track max timestamp per job
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
    
    # Step 2: Read job watermarks
    def _read_job_watermarks(self, state: BusinessDateConverterState) -> Result[BusinessDateConverterState, Exception]:
        """Read job watermarks from VictoriaMetrics to determine last processed timestamps."""
        try:
            if not state.vm_query_url:
                self.logger.warning("No VM query URL configured, will process all metrics")
                state.job_watermarks = {job: None for job in state.source_job_names}
                return Ok(state)
            
            # Initialize Prometheus client using helper method
            prom = self._get_prometheus_client(state)
            
            job_watermarks = {}
            for job_name in state.source_job_names:
                try:
                    # Query for job watermark metric
                    watermark_metric = f'latest_converted_timestamp_by_job_wm{{source="{job_name}"}}'
                    
                    # Query using PromQL
                    query_result = prom.custom_query(query=watermark_metric)
                    
                    if query_result and len(query_result) > 0:
                        # Get the latest value
                        result = query_result[0]
                        if 'value' in result and result['value']:
                            timestamp = float(result['value'][1])  # Prometheus format: [timestamp, value]
                            job_watermarks[job_name] = int(timestamp)
                            self.logger.info(f"Job watermark for {job_name}: {job_watermarks[job_name]}")
                        else:
                            job_watermarks[job_name] = None
                            self.logger.info(f"No watermark found for job {job_name}")
                    else:
                        job_watermarks[job_name] = None
                        self.logger.info(f"No watermark found for job {job_name}")
                        
                except Exception as e:
                    self.logger.warning(f"Failed to read watermark for job {job_name}: {e}")
                    job_watermarks[job_name] = None
            
            state.job_watermarks = job_watermarks
            return Ok(state)
            
        except Exception as e:
            self.logger.error(f"Failed to read job watermarks: {e}")
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
            
            # Process each job
            for job_name in state.source_job_names:
                try:
                    self.logger.info(f"Processing metrics for job: {job_name}")
                    
                    # Get watermark for this job
                    watermark_ts = state.job_watermarks.get(job_name)
                    
                    # Calculate time range: from watermark to now
                    if watermark_ts:
                        start_time = datetime.fromtimestamp(watermark_ts, tz=timezone.utc)
                    else:
                        # If no watermark, query from configured lookback period
                        lookback_days = state.job_config.get('watermark_lookback_days', 30)
                        # Calculate start_time as lookback_days ago from now
                        end_time = datetime.now(timezone.utc)
                        start_time = end_time - timedelta(days=lookback_days)
                        self.logger.info(f"No watermark found for job {job_name}, using lookback of {lookback_days} days from {end_time} to {start_time}")
                    
                    if watermark_ts:
                        end_time = datetime.now(timezone.utc)
                    
                    self.logger.info(f"Querying metrics for job {job_name} from {start_time} to {end_time}")
                    
                    # Query metrics using PromQL with grouping to get latest value per series+business_date
                    # This is more efficient than fetching all data points and grouping locally
                    grouped_metrics = self._query_grouped_metrics(prom, job_name, start_time, end_time)
                    
                    self.logger.info(f"Found {len(grouped_metrics)} unique metric series+business_date combinations for job {job_name}")
                    
                    # Convert each metric
                    for (metric_name, labels_tuple, biz_date_str), (value, original_timestamp) in grouped_metrics.items():
                        try:
                            # Convert labels_tuple back to dict
                            labels_without_biz_date = dict(labels_tuple)
                            
                            # Parse biz_date - format is "dd/mm/yyyy"
                            try:
                                business_date = datetime.strptime(biz_date_str, '%d/%m/%Y').date()
                            except ValueError:
                                self.logger.warning(f"Invalid biz_date format: {biz_date_str}, expected dd/mm/yyyy")
                                continue
                            
                            # Track max timestamp for this job
                            if job_name not in state.max_processed_timestamps:
                                state.max_processed_timestamps[job_name] = int(original_timestamp)
                            else:
                                state.max_processed_timestamps[job_name] = max(
                                    state.max_processed_timestamps[job_name],
                                    int(original_timestamp)
                                )
                            
                            # Calculate converted timestamp
                            converted_ts = self._calculate_converted_timestamp(
                                state, prom, metric_name, business_date, job_name, labels_without_biz_date
                            )
                            
                            # Transform and write metric
                            success = self._write_converted_metric(
                                state, metric_name, labels_without_biz_date, job_name,
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
                    self.logger.error(f"Failed to process job {job_name}: {e}")
                    state.failed_count += 1
            
            self.logger.info(f"Conversion complete: {state.metrics_converted} converted, {state.failed_count} failed")
            return Ok(state)
            
        except Exception as e:
            self.logger.error(f"Failed to query and convert metrics: {e}")
            return Err(e)
    
    def _query_grouped_metrics(
        self, prom: PrometheusConnect, job_name: str, start_time: datetime, end_time: datetime
    ) -> Dict[Tuple[str, tuple, str], Tuple[float, float]]:
        """Query metrics from VM with server-side grouping to get latest value per series+business_date.
        
        Uses PromQL last_over_time() to get the latest value per metric series and business_date.
        This is much more efficient than fetching all data points and grouping locally.
        
        Returns dict: {(metric_name, labels_tuple, biz_date_str): (value, timestamp)}
        where labels_tuple is tuple(sorted(labels.items())) for hashability
        """
        grouped = {}
        
        try:
            # Calculate time range in seconds for PromQL
            # last_over_time() looks back from the evaluation time (now), so we need to calculate
            # how far back to look from end_time to cover start_time
            time_range_seconds = int((end_time - start_time).total_seconds())
            
            # Ensure we have a positive time range
            if time_range_seconds <= 0:
                self.logger.warning(f"Invalid time range: start_time={start_time}, end_time={end_time}")
                return {}
            
            # Query all metrics with job label using last_over_time to get latest value per series
            # This efficiently groups server-side and returns only the latest value per series+biz_date
            # last_over_time looks back from now, so we use the calculated range
            base_query = f'{{job="{job_name}"}}'
            query = f'last_over_time({base_query}[{time_range_seconds}s])'
            
            try:
                # Execute instant query - last_over_time returns latest value per series in the time range
                query_result = prom.custom_query(query=query)
                    
                if query_result:
                    # Process results - each result represents one series with its latest value
                    for result in query_result:
                        metric_data = result.get('metric', {})
                        metric_name = metric_data.get('__name__', '')
                        if not metric_name:
                            continue
                        
                        # Extract labels
                        labels = {k: v for k, v in metric_data.items() if k != '__name__'}
                        
                        # Extract biz_date
                        biz_date_str = labels.pop('biz_date', None)
                        if not biz_date_str:
                            continue
                        
                        # Get value - custom_query returns instant query format: {'value': [timestamp, value]}
                        value_data = result.get('value')
                        if not value_data or len(value_data) < 2:
                            continue
                        
                        timestamp = float(value_data[0])
                        value = float(value_data[1])
                        
                        # Create key: (metric_name, labels_without_biz_date, biz_date)
                        # Convert labels dict to tuple of sorted items for hashability
                        labels_tuple = tuple(sorted(labels.items()))
                        key = (metric_name, labels_tuple, biz_date_str)
                        
                        # Store value and timestamp
                        # If key already exists, keep the one with latest timestamp
                        if key not in grouped or timestamp > grouped[key][1]:
                            grouped[key] = (value, timestamp)
            
            except Exception as e:
                self.logger.error(f"Failed to query grouped metrics: {e}")
                # Return empty dict on error
        
        except Exception as e:
            self.logger.error(f"Failed to query grouped metrics: {e}")
            # Return empty dict on error
        
        return grouped
    
    def _calculate_converted_timestamp(
        self, state: BusinessDateConverterState, prom: PrometheusConnect,
        metric_name: str, business_date: date, job_name: str, labels_without_biz_date: Dict[str, str]
    ) -> int:
        """Calculate converted timestamp for a metric+business_date combination.
        
        Queries the converted series itself using range queries to find the max existing timestamp.
        If no entry exists, returns 00:00:00.000 of the converted date.
        If entry exists, returns max existing timestamp + 1 second offset.
        """
        try:
            # Build query for the converted metric series (same as how it's written)
            # Converted metrics have: same metric_name, labels without biz_date, source instead of job
            converted_labels = labels_without_biz_date.copy()
            converted_labels.pop('biz_date', None)  # Remove biz_date if present
            if 'job' in converted_labels:
                converted_labels['source'] = converted_labels.pop('job')
            else:
                converted_labels['source'] = job_name
            
            # Build PromQL query for the converted metric series
            label_pairs = [f'{k}="{v}"' for k, v in sorted(converted_labels.items())]
            converted_metric_query = f'{metric_name}{{{",".join(label_pairs)}}}'
            
            # Calculate time range for the business_date day (00:00:00 to 23:59:59 UTC)
            start_datetime = datetime.combine(business_date, datetime.min.time()).replace(tzinfo=timezone.utc)
            end_datetime = datetime.combine(business_date, datetime.max.time().replace(microsecond=0)).replace(tzinfo=timezone.utc)
            
            start_timestamp = int(start_datetime.timestamp())
            end_timestamp = int(end_datetime.timestamp())
            
            # Query the converted series using range query to get all data points for the day
            max_existing_timestamp = self._query_max_timestamp_from_converted_series(
                state, converted_metric_query, start_timestamp, end_timestamp
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
    
    def _query_max_timestamp_from_converted_series(
        self, state: BusinessDateConverterState, metric_query: str, start_timestamp: int, end_timestamp: int
    ) -> Optional[int]:
        """Query the converted series using range query and return the max timestamp.
        
        Args:
            state: Job state with VM configuration
            metric_query: PromQL query for the converted metric series
            start_timestamp: Start timestamp for the range query (Unix timestamp)
            end_timestamp: End timestamp for the range query (Unix timestamp)
            
        Returns:
            Max timestamp found in the range, or None if no data exists
        """
        try:
            if not state.vm_query_url:
                return None
            
            # Initialize Prometheus client using helper method
            prom = self._get_prometheus_client(state)
            
            # Convert timestamps to datetime objects for custom_query_range
            start_time = datetime.fromtimestamp(start_timestamp, tz=timezone.utc)
            end_time = datetime.fromtimestamp(end_timestamp, tz=timezone.utc)
            
            # Execute range query using Prometheus client API
            # Use 1 minute step for day queries to avoid too many data points
            # This is sufficient to find the max timestamp while keeping query size reasonable
            query_result = prom.custom_query_range(
                query=metric_query,
                start_time=start_time,
                end_time=end_time,
                step='1m'  # 1 minute step - sufficient for finding max timestamp
            )
            
            # Parse response to find max timestamp
            max_timestamp = None
            if query_result and isinstance(query_result, dict):
                if query_result.get('status') == 'success' and 'data' in query_result:
                    data = query_result['data']
                    if 'result' in data and isinstance(data['result'], list):
                        for series in data['result']:
                            if 'values' in series and isinstance(series['values'], list):
                                for value_pair in series['values']:
                                    if isinstance(value_pair, list) and len(value_pair) >= 1:
                                        # value_pair format: [timestamp, value]
                                        timestamp = float(value_pair[0])
                                        if max_timestamp is None or timestamp > max_timestamp:
                                            max_timestamp = int(timestamp)
            
            return max_timestamp
            
        except Exception as e:
            self.logger.warning(f"Failed to query max timestamp from converted series: {e}")
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
            # Some versions don't expose session, so fall back to creating a new one
            session = getattr(prom, "session", None)
            if session is None:
                import requests
                session = requests.Session()
            
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
        labels_without_biz_date: Dict[str, str], job_name: str, business_date: date,
        value: float, timestamp: int
    ) -> bool:
        """Write converted metric to Victoria Metrics."""
        try:
            # Copy labels and transform
            labels_dict = labels_without_biz_date.copy()
            
            # Remove biz_date if present (it's already removed, but check for safety), convert job to source
            labels_dict.pop('biz_date', None)
            if 'job' in labels_dict:
                labels_dict['source'] = labels_dict.pop('job')
            else:
                labels_dict['source'] = job_name
            
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
        """Update job watermarks with latest processed timestamps."""
        try:
            if not state.vm_gateway_url:
                self.logger.warning("No VM gateway URL configured, skipping watermark update")
                return Ok(state)
            
            # Update watermarks with max processed timestamps
            for job_name in state.source_job_names:
                try:
                    # Use max processed timestamp if available, otherwise use current time
                    max_ts = state.max_processed_timestamps.get(job_name)
                    if max_ts is None:
                        max_ts = int(datetime.now(timezone.utc).timestamp())
                    
                    watermark_line = f'latest_converted_timestamp_by_job_wm{{source="{job_name}"}} {max_ts} {max_ts}'
                    
                    # Write watermark using Prometheus client session
                    if self._write_metric_to_vm(state, watermark_line, timeout=30):
                        self.logger.info(f"Updated job watermark for {job_name} to {max_ts}")
                    else:
                        raise Exception("Failed to write watermark metric")
                    
                except Exception as e:
                    self.logger.warning(f"Failed to update job watermark for {job_name}: {e}")
            
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

