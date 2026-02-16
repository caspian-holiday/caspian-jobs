#!/usr/bin/env python3
"""
Extractor Job - Functional data extraction script with step-by-step workflow
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, date, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from prometheus_api_client import PrometheusConnect

# Add the scheduler module to the path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from victoria_metrics_jobs.jobs.common import BaseJob, BaseJobState, Result, Ok, Err


@dataclass
class ExtractorState(BaseJobState):
    """State object for extractor job execution.
    
    This state object extends BaseJobState and adds extractor-specific fields
    that are passed through the functional pipeline and accumulate data as
    each step is executed. The state contains:
    
    - current_business_date: Derived business date for processing
    - weekdays: List of weekdays to process (Monday-Friday)
    - db_timestamps: Mapping of weekday -> last extraction timestamp in database
    - vm_timestamps: Mapping of weekday -> max timestamp in Victoria Metrics
    - weekdays_to_update: List of weekdays that need updating (db < vm or db is None)
    - metrics_saved_count: Counter of metrics successfully saved
    """
    current_business_date: date = None
    weekdays: List[date] = None
    db_timestamps: Dict[date, Optional[datetime]] = None
    vm_timestamps: Dict[date, Optional[datetime]] = None
    weekdays_to_update: List[date] = None
    metrics_saved_count: int = 0
    
    def to_results(self) -> Dict[str, Any]:
        """Convert state to job results dictionary with extractor-specific fields."""
        results = super().to_results()
        results.update({
            'metrics_saved_count': self.metrics_saved_count,
            'weekdays_processed': len(self.weekdays) if self.weekdays else 0,
            'weekdays_updated': len(self.weekdays_to_update) if self.weekdays_to_update else 0
        })
        return results


class ExtractorJob(BaseJob):
    """Extractor job class for data extraction with step-by-step workflow."""
    
    def __init__(self, config_path: str = None, verbose: bool = False):
        """Initialize the extractor job.
        
        Args:
            config_path: Path to configuration file
            verbose: Whether to enable verbose logging
        """
        super().__init__('extractor', config_path, verbose)
    
    def create_initial_state(self, job_id: str) -> Result[ExtractorState, Exception]:
        """Create the initial state for extractor job execution.
        
        Args:
            job_id: Job ID to use for configuration selection
            
        Returns:
            Result containing the initial state or an error
        """
        try:
            job_config = self.get_job_config(job_id)
            initial_state = ExtractorState(
                job_id=job_id,
                job_config=job_config,
                started_at=datetime.now(),
                current_business_date=date.today(),  # Will be updated in step 1
                weekdays=[],  # Will be populated in step 2
                db_timestamps={},  # Will be populated in step 3
                vm_timestamps={},  # Will be populated in step 4
                weekdays_to_update=[],  # Will be populated in step 5
                metrics_saved_count=0
            )
            return Ok(initial_state)
        except Exception as e:
            return Err(e)
    
    def get_workflow_steps(self) -> List[callable]:
        """Get the list of workflow steps for the extractor job.
        
        Returns:
            List of step functions that take a state and return Result[State, Exception]
        """
        return [
            self._derive_current_business_date,      # Step 1
            self._derive_weekdays_list,              # Step 2  
            self._associate_db_timestamps,           # Step 3
            self._associate_vm_timestamps,           # Step 4
            self._derive_weekdays_to_update,         # Step 5
            self._process_weekdays_to_update         # Step 6
        ]
    
    def finalize_state(self, state: ExtractorState) -> ExtractorState:
        """Finalize the extractor state before converting to results.
        
        Args:
            state: The final state after all steps
            
        Returns:
            Finalized state ready for conversion to results
        """
        state.completed_at = datetime.now()
        
        # Determine status based on processing results
        if state.metrics_saved_count == 0 and state.weekdays_to_update:
            state.status = 'error'
            state.message = f'Extraction failed for job: {state.job_id} - 0 metrics saved'
        elif state.metrics_saved_count < len(state.weekdays_to_update):
            state.status = 'partial_success'
            state.message = f'Extraction completed with warnings for job: {state.job_id} - {state.metrics_saved_count}/{len(state.weekdays_to_update)} weekdays processed'
        else:
            state.status = 'success'
            state.message = f'Extraction completed for job: {state.job_id} - {state.metrics_saved_count} weekdays processed'
        
        # Push job status metric to VM for observability
        self._publish_job_status_metric(state)
        
        return state
    
    def _publish_job_status_metric(self, state: ExtractorState) -> None:
        """Publish job status metric to VictoriaMetrics for monitoring."""
        try:
            import requests
            
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
            
            metric_line = f'extractor_job_status{{{",".join(label_pairs)}}} {status_value} {timestamp}'
            
            # Send to VM gateway
            vm_gateway_url = state.job_config.get('victoria_metrics', {}).get('gateway_url', '')
            vm_token = state.job_config.get('victoria_metrics', {}).get('token', '')
            
            if vm_gateway_url:
                headers = {'Content-Type': 'text/plain'}
                if vm_token:
                    headers['Authorization'] = f'Bearer {vm_token}'
                
                response = requests.post(
                    f"{vm_gateway_url}/api/v1/import/prometheus",
                    data=metric_line,
                    headers=headers,
                    timeout=30
                )
                response.raise_for_status()
                
                self.logger.info(f"Published job status metric: {state.status}")
            
        except Exception as e:
            self.logger.warning(f"Failed to publish job status metric: {e}")
    
    # Step 1: Derive current business date
    def _derive_current_business_date(self, state: ExtractorState) -> Result[ExtractorState, Exception]:
        """Derive current business date from configuration (UTC timezone)."""
        try:
            from datetime import timedelta
            
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
    
    # Step 2: Derive list of weekdays from start_date_offset_days and current business_date
    def _derive_weekdays_list(self, state: ExtractorState) -> Result[ExtractorState, Exception]:
        """Derive list of weekdays starting from start_date_offset_days and current business_date."""
        try:
            from datetime import timedelta
            
            # Get offset days from config
            offset_days = state.job_config.get('start_date_offset_days', 30)
            start_date = state.current_business_date - timedelta(days=offset_days)
            
            # Generate weekdays from start_date to current_business_date
            weekdays = []
            current_date = start_date
            
            while current_date <= state.current_business_date:
                # Only include weekdays (Monday=0, Friday=4)
                if current_date.weekday() < 5:
                    weekdays.append(current_date)
                current_date += timedelta(days=1)
            
            state.weekdays = weekdays
            self.logger.info(f"Generated {len(weekdays)} weekdays from {start_date} to {state.current_business_date}")
            
            return Ok(state)
            
        except Exception as e:
            self.logger.error(f"Failed to derive weekdays list: {e}")
            return Err(e)
    
    # Step 3: Associate each weekday with timestamp saved in database for that date
    def _associate_db_timestamps(self, state: ExtractorState) -> Result[ExtractorState, Exception]:
        """Associate each weekday with timestamp saved in database for that date."""
        try:
            if not state.db_manager:
                self.logger.warning("No database manager available, skipping DB timestamp lookup")
                state.db_timestamps = {weekday: None for weekday in state.weekdays}
                return Ok(state)
            
            db_timestamps = {}
            
            for weekday in state.weekdays:
                try:
                    # Query database for max_data_timestamp from the latest execution for this date
                    query = """
                    SELECT max_data_timestamp
                    FROM vm_extraction_jobs 
                    WHERE biz_date = :biz_date AND job_id = :job_id
                    ORDER BY execution_timestamp DESC
                    LIMIT 1
                    """
                    
                    result = state.db_manager.execute_query(query, {
                        'biz_date': weekday,
                        'job_id': state.job_id
                    })
                    
                    self.logger.debug(f"DB query for {weekday}: result={result}")
                    
                    if result and len(result) > 0 and result[0][0]:
                        db_timestamps[weekday] = result[0][0]
                        self.logger.debug(f"DB timestamp for {weekday}: {result[0][0]}")
                    else:
                        db_timestamps[weekday] = None
                        self.logger.debug(f"No DB timestamp found for {weekday}")
                        
                except Exception as e:
                    self.logger.warning(f"Failed to query DB timestamp for {weekday}: {e}")
                    db_timestamps[weekday] = None
            
            state.db_timestamps = db_timestamps
            self.logger.info(f"Retrieved DB timestamps for {len(state.weekdays)} weekdays")
            
            return Ok(state)
            
        except Exception as e:
            self.logger.error(f"Failed to associate DB timestamps: {e}")
            return Err(e)
    
    # Step 4: Associate each weekday with max timestamp saved in Victoria Metrics for that date
    def _associate_vm_timestamps(self, state: ExtractorState) -> Result[ExtractorState, Exception]:
        """Associate each weekday with max timestamp saved in Victoria Metrics for that date."""
        try:
            vm_timestamps = {}
            
            # Get VM query URL and token from config
            vm_query_url = state.job_config.get('victoria_metrics', {}).get('query_url', '')
            vm_token = state.job_config.get('victoria_metrics', {}).get('token', '')
            
            if not vm_query_url:
                self.logger.warning("No VM query URL configured, skipping VM timestamp lookup")
                vm_timestamps = {weekday: None for weekday in state.weekdays}
                state.vm_timestamps = vm_timestamps
                return Ok(state)
            
            # Initialize Prometheus client
            headers = {}
            if vm_token:
                headers['Authorization'] = f'Bearer {vm_token}'
            
            prom = PrometheusConnect(url=vm_query_url, headers=headers, disable_ssl=True)
            
            for weekday in state.weekdays:
                try:
                    # Get configuration parameters for time range query (same as extraction)
                    chunk_size_days = state.job_config.get('chunk_size_days', 1)
                    start_date_offset_days = state.job_config.get('start_date_offset_days', 0)
                    
                    # Calculate time range for this weekday (same logic as extraction)
                    from datetime import timedelta
                    start_datetime = datetime.combine(weekday, datetime.min.time())
                    
                    # End time is tomorrow to capture recently inserted retrospective metrics
                    # This assumes metrics can be inserted just recently but retrospectively to past dates
                    end_datetime = datetime.combine(datetime.now().date() + timedelta(days=1), datetime.min.time())
                    
                    # Format date as dd/mm/yyyy for biz_date label
                    formatted_date = weekday.strftime('%d/%m/%Y')
                    
                    # Use prometheus_api_client to get max timestamp
                    metric_data = prom.get_metric_range_data(
                        metric_name='',  # Empty metric name to get all metrics
                        label_config={'biz_date': formatted_date, 'job': state.job_id},
                        start_time=start_datetime,
                        end_time=end_datetime,
                        chunk_size=timedelta(days=chunk_size_days)
                    )
                    
                    self.logger.debug(f"VM query for {weekday}: found {len(metric_data)} metric series")
                    
                    if metric_data:
                        # Find the maximum timestamp across all metrics
                        max_timestamp = None
                        for metric in metric_data:
                            if metric.get('values'):
                                for value_pair in metric['values']:
                                    if len(value_pair) >= 2:
                                        timestamp = float(value_pair[0])
                                        if max_timestamp is None or timestamp > max_timestamp:
                                            max_timestamp = timestamp
                        
                        if max_timestamp is not None:
                            # VM timestamps are UTC without timezone info, set UTC timezone
                            vm_timestamps[weekday] = datetime.utcfromtimestamp(max_timestamp).replace(tzinfo=timezone.utc)
                            self.logger.debug(f"VM timestamp for {weekday}: {vm_timestamps[weekday]}")
                        else:
                            vm_timestamps[weekday] = None
                            self.logger.debug(f"No VM timestamp values found for {weekday}")
                    else:
                        vm_timestamps[weekday] = None
                        self.logger.debug(f"No VM timestamp found for {weekday}")
                        
                except Exception as e:
                    self.logger.warning(f"Failed to query VM timestamp for {weekday}: {e}")
                    vm_timestamps[weekday] = None
            
            state.vm_timestamps = vm_timestamps
            self.logger.info(f"Retrieved VM timestamps for {len(state.weekdays)} weekdays")
            
            return Ok(state)
            
        except Exception as e:
            self.logger.error(f"Failed to associate VM timestamps: {e}")
            return Err(e)
    
    # Step 5: Derive weekdays that need to be updated by comparing database vs VM timestamps
    def _derive_weekdays_to_update(self, state: ExtractorState) -> Result[ExtractorState, Exception]:
        """Derive weekdays that need to be updated by comparing database vs VM timestamps."""
        try:
            weekdays_to_update = []
            
            for weekday in state.weekdays:
                db_timestamp = state.db_timestamps.get(weekday)
                vm_timestamp = state.vm_timestamps.get(weekday)
                
                self.logger.debug(f"Weekday {weekday}: DB={db_timestamp}, VM={vm_timestamp}")
                
                # If no DB timestamp, need to extract
                if db_timestamp is None:
                    self.logger.debug(f"  -> Adding to extraction (no DB timestamp)")
                    weekdays_to_update.append(weekday)
                    continue
                
                # If no VM timestamp, skip (nothing to extract)
                if vm_timestamp is None:
                    self.logger.debug(f"  -> Skipping (no VM timestamp)")
                    continue
                
                # If DB timestamp is older than VM timestamp, need to extract
                if db_timestamp < vm_timestamp:
                    self.logger.debug(f"  -> Adding to extraction (DB < VM: {db_timestamp} < {vm_timestamp})")
                    weekdays_to_update.append(weekday)
                else:
                    self.logger.debug(f"  -> Skipping (DB >= VM: {db_timestamp} >= {vm_timestamp})")
            
            state.weekdays_to_update = weekdays_to_update
            self.logger.info(f"Selected {len(weekdays_to_update)} weekdays for extraction")
            
            return Ok(state)
            
        except Exception as e:
            self.logger.error(f"Failed to derive weekdays to update: {e}")
            return Err(e)
    
    # Step 6: Process all weekdays that need updating
    def _process_weekdays_to_update(self, state: ExtractorState) -> Result[ExtractorState, Exception]:
        """Process all weekdays that need updating."""
        try:
            if not state.weekdays_to_update:
                self.logger.info("No weekdays to update")
                return Ok(state)
            
            # Process each weekday
            for weekday in state.weekdays_to_update:
                try:
                    self.logger.info(f"Extracting metrics for weekday: {weekday}")
                    
                    # Extract metrics for this weekday
                    success = self._extract_metrics_for_weekday(state, weekday)
                    if success:
                        state.metrics_saved_count += 1
                        self.logger.info(f"Successfully extracted metrics for {weekday}")
                    else:
                        self.logger.error(f"Failed to extract metrics for {weekday}")
                        
                except Exception as e:
                    self.logger.error(f"Error extracting metrics for {weekday}: {e}")
            
            self.logger.info(f"Extraction complete: {state.metrics_saved_count} weekdays processed")
            return Ok(state)
            
        except Exception as e:
            self.logger.error(f"Failed to process weekdays: {e}")
            return Err(e)
    
    def _extract_metrics_for_weekday(self, state: ExtractorState, weekday: date) -> bool:
        """Extract metrics for a specific weekday using prometheus_api_client."""
        try:
            from datetime import timedelta
            
            # Get VM query URL and token from config
            vm_query_url = state.job_config.get('victoria_metrics', {}).get('query_url', '')
            vm_token = state.job_config.get('victoria_metrics', {}).get('token', '')
            
            if not vm_query_url:
                self.logger.error("No VM query URL configured")
                return False
            
            # Initialize Prometheus client
            headers = {}
            if vm_token:
                headers['Authorization'] = f'Bearer {vm_token}'
            
            prom = PrometheusConnect(url=vm_query_url, headers=headers, disable_ssl=True)
            
            # Get configuration parameters for time range query
            chunk_size_days = state.job_config.get('chunk_size_days', 1)
            start_date_offset_days = state.job_config.get('start_date_offset_days', 0)
            
            # Calculate time range for this weekday
            # Start from the minimum time for the weekday
            start_datetime = datetime.combine(weekday, datetime.min.time())
            
            # End time is tomorrow to capture recently inserted retrospective metrics
            # This assumes metrics can be inserted just recently but retrospectively to past dates
            end_datetime = datetime.combine(datetime.now().date() + timedelta(days=1), datetime.min.time())
            
            self.logger.debug(f"Time range for {weekday}: start={start_datetime}, end={end_datetime}")
            
            # Format date as dd/mm/yyyy for biz_date label
            formatted_date = weekday.strftime('%d/%m/%Y')
            
            # Use prometheus_api_client to get metric range data
            # This preserves original timestamps and provides latest values per metric
            self.logger.info(f"Extracting metrics for {weekday} using prometheus_api_client with {chunk_size_days}d window")
            
            # Query for all metrics with the specific labels
            metric_data = prom.get_metric_range_data(
                metric_name='',  # Empty metric name to get all metrics
                label_config={'biz_date': formatted_date, 'job': state.job_id},
                start_time=start_datetime,
                end_time=end_datetime,
                chunk_size=timedelta(days=chunk_size_days)
            )
            
            if not metric_data:
                self.logger.warning(f"No metrics found in VM for {weekday} in time range {start_datetime} to {end_datetime}")
                return False
            
            # Save to database
            if state.db_manager:
                self._save_metrics_to_database(state, weekday, metric_data)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to extract metrics for {weekday}: {e}")
            return False
    
    def _save_metrics_to_database(self, state: ExtractorState, weekday: date, metrics_data: List[Dict[str, Any]]) -> bool:
        """Save extracted metrics to database, then create job record with statistics."""
        try:
            if not state.db_manager:
                self.logger.warning("No database manager available, skipping DB save")
                return False
            
            # Calculate execution time and timestamp
            start_time = state.started_at
            current_time = datetime.utcnow()
            execution_time = (current_time - start_time).total_seconds()
            execution_timestamp = current_time  # Use current time as execution timestamp for this extraction
            
            # Start transaction
            state.db_manager.begin_transaction()
            
            try:
                # Prepare all metrics data for batch insert
                metric_records = []
                max_timestamp = None
                
                for metric in metrics_data:
                    metric_name = metric['metric'].get('__name__', 'unknown')
                    labels = metric['metric']
                    
                    # Extract standard labels (similar to metrics_forecast job)
                    auid = labels.get('auid')
                    if not auid:
                        self.logger.warning(f"Metric {metric_name} missing auid label, skipping")
                        continue
                    
                    # Extract source (from 'job' label) and biz_date
                    source = labels.get('job')  # 'job' label maps to 'source' in database
                    biz_date_str = labels.get('biz_date')
                    
                    # Parse biz_date from dd/mm/yyyy format
                    biz_date = None
                    if biz_date_str:
                        try:
                            biz_date = datetime.strptime(biz_date_str, '%d/%m/%Y').date()
                        except ValueError:
                            self.logger.warning(f"Invalid biz_date format: {biz_date_str}")
                    
                    # Build remaining labels JSON (exclude standard labels)
                    # Similar pattern to metrics_forecast job
                    excluded_labels = {'job', 'source', 'auid', 'biz_date', '__name__'}
                    remaining_labels = {
                        k: v for k, v in labels.items() 
                        if k not in excluded_labels
                    }
                    metric_labels_json = json.dumps(remaining_labels, sort_keys=True)
                    
                    # Handle values array from prometheus_api_client (may contain single element)
                    values = metric.get('values', [])
                    if not values and 'value' in metric:
                        # Fallback to single value if values array is empty
                        values = [metric['value']]
                    
                    # Find the value with the maximum timestamp for this metric
                    max_timestamp_value = None
                    metric_max_timestamp = None
                    
                    self.logger.debug(f"Processing {len(values)} values for metric {metric_name}")
                    
                    for value_data in values:
                        if isinstance(value_data, list) and len(value_data) >= 2:
                            timestamp = float(value_data[0])
                            value = value_data[1]
                            
                            self.logger.debug(f"  Checking value: timestamp={timestamp}, value={value}")
                            
                            if metric_max_timestamp is None or timestamp > metric_max_timestamp:
                                metric_max_timestamp = timestamp
                                max_timestamp_value = value_data
                                self.logger.debug(f"    -> New max for this metric: {timestamp}")
                    
                    # Save only the value with the maximum timestamp
                    if max_timestamp_value is not None:
                        timestamp = max_timestamp_value[0]
                        value = max_timestamp_value[1]
                        
                        self.logger.debug(f"Raw timestamp: {timestamp} (type: {type(timestamp)})")
                        
                        # Convert timestamp to float to handle decimal timestamps
                        # VM timestamps are UTC without timezone info, set UTC timezone
                        timestamp_float = float(timestamp)
                        metric_timestamp = datetime.utcfromtimestamp(timestamp_float).replace(tzinfo=timezone.utc)
                        
                        self.logger.debug(f"Parsed timestamp: {metric_timestamp} (original: {timestamp_float})")
                        
                        # Prepare metric record for batch insert
                        metric_record = {
                            'biz_date': biz_date if biz_date else weekday,  # Use extracted biz_date or fallback to weekday
                            'auid': auid,  # Renamed from audit_id
                            'metric_name': metric_name,
                            'value': value,
                            'timestamp': metric_timestamp,
                            'metric_labels': metric_labels_json,  # New field
                            'extracted_at': current_time,
                            'job_id': state.job_id,
                            'job_execution_timestamp': execution_timestamp
                        }
                        
                        metric_records.append(metric_record)
                        
                        # Track max timestamp across all metrics for job record
                        if max_timestamp is None or metric_timestamp > max_timestamp:
                            max_timestamp = metric_timestamp
                
                # Batch insert all metrics using executemany
                if metric_records:
                    batch_insert_query = """
                    INSERT INTO vm_extracted_metrics (
                        biz_date, auid, metric_name, value, timestamp,
                        metric_labels, extracted_at, job_id, job_execution_timestamp
                    )
                    VALUES (
                        :biz_date, :auid, :metric_name, :value, :timestamp,
                        CAST(:metric_labels AS jsonb), :extracted_at, :job_id, :job_execution_timestamp
                    )
                    """
                    
                    state.db_manager.execute_batch_insert(batch_insert_query, metric_records)
                    records_saved = len(metric_records)
                else:
                    records_saved = 0
                
                # Now create job record with calculated statistics
                job_info_query = """
                INSERT INTO vm_extraction_jobs (
                    job_id, biz_date, execution_timestamp, started_at, completed_at, 
                    records_processed, execution_time_seconds, max_data_timestamp
                )
                VALUES (
                    :job_id, :biz_date, :execution_timestamp, :started_at, :completed_at,
                    :records_processed, :execution_time_seconds, :max_data_timestamp
                )
                """
                
                job_info_values = { 'job_id': state.job_id,
                    'biz_date': weekday,
                    'execution_timestamp': execution_timestamp,
                    'started_at': start_time,
                    'completed_at': current_time,
                    'records_processed': records_saved,
                    'execution_time_seconds': execution_time,
                    'max_data_timestamp': max_timestamp
                }
                
                state.db_manager.execute_query(job_info_query, job_info_values)
                
                # Commit the entire transaction
                state.db_manager.commit_transaction()
                
                self.logger.info(f"Successfully saved {records_saved} metrics to database for {weekday}")
                return True
                    
            except Exception as error:
                # Critical failure - rollback entire transaction
                state.db_manager.rollback_transaction()
                self.logger.error(f"Failed to save metrics for {weekday}: {error}")
                return False
            
        except Exception as e:
            self.logger.error(f"Failed to save metrics to database for {weekday}: {e}")
            return False
    


def main():
    """Main function for command-line execution."""
    epilog = """
Examples:
  # List available job configurations
  python extractor.py --config config.yml --list-jobs
  
  # Run extraction for system_a_extractor
  python extractor.py --config config.yml --job-id system_a_extractor
  
  # Run extraction for system_b_extractor
  python extractor.py --config config.yml --job-id system_b_extractor
    """
    
    return ExtractorJob.main(
        description="Extractor Job - Data extraction and processing",
        epilog=epilog
    )


if __name__ == "__main__":
    sys.exit(main())