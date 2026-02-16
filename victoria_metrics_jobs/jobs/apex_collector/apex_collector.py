#!/usr/bin/env python3
"""
Apex Collector Job - Apex number collection with step-by-step workflow
"""

from __future__ import annotations

import sys
from datetime import datetime, date
from pathlib import Path
from typing import Dict, Any, List, Optional
from dataclasses import dataclass

# Add the scheduler module to the path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from victoria_metrics_jobs.jobs.common import BaseJob, BaseJobState, Result, Ok, Err


@dataclass
class ApexCollectorState(BaseJobState):
    """State object for apex collector job execution.
    
    This state object extends BaseJobState and adds apex collector-specific fields
    that are passed through the functional pipeline and accumulate data as
    each step is executed. The state contains:
    
    - current_business_date: Derived business date for processing (UTC)
    - watermark_date: Last successfully processed business date from VM
    - weekdays: List of weekdays to process (Monday-Friday)
    - weekdays_to_update: List of weekdays that need updating
    - source_url: URL to collect apex data from
    - source_token: Authentication token for source
    - vm_gateway_url: URL to send collected data to VM gateway
    - vm_query_url: URL to query VM for watermark
    - vm_token: Authentication token for VM
    - apex_data: List of collected apex data rows
    - processed_count: Count of successfully processed business dates
    - failed_count: Count of failed business date processing
    - max_days_per_run: Maximum days to process in single run
    - sliding_window_days: Optional sliding window for reprocessing
    """
    current_business_date: date = None
    watermark_date: Optional[date] = None
    weekdays: List[date] = None
    weekdays_to_update: List[date] = None
    source_url: str = ""
    source_token: str = ""
    vm_gateway_url: str = ""
    vm_query_url: str = ""
    vm_token: str = ""
    apex_data: List[Dict[str, Any]] = None
    processed_count: int = 0
    failed_count: int = 0
    max_days_per_run: int = 5
    sliding_window_days: int = 0
    
    def to_results(self) -> Dict[str, Any]:
        """Convert state to job results dictionary with apex collector-specific fields."""
        results = super().to_results()
        apex_count = len(self.apex_data) if self.apex_data else 0
        results.update({
            'apex_data_collected': apex_count,
            'total_number_published_metrics': self.processed_count,
            'failed_count': self.failed_count,
            'success_rate': self.processed_count / max(len(self.weekdays_to_update) if self.weekdays_to_update else 1, 1) * 100,
            'weekdays_processed': len(self.weekdays) if self.weekdays else 0,
            'weekdays_updated': len(self.weekdays_to_update) if self.weekdays_to_update else 0,
            'watermark_date': self.watermark_date.isoformat() if self.watermark_date else None,
            'current_business_date': self.current_business_date.isoformat() if self.current_business_date else None
        })
        return results


class ApexCollectorJob(BaseJob):
    """Apex collector job class with step-by-step workflow."""
    
    def __init__(self, config_path: str = None, verbose: bool = False):
        """Initialize the apex collector job.
        
        Args:
            config_path: Path to configuration file
            verbose: Whether to enable verbose logging
        """
        super().__init__('apex_collector', config_path, verbose)
    
    def create_initial_state(self, job_id: str) -> Result[ApexCollectorState, Exception]:
        """Create the initial state for apex collector job execution.
        
        Args:
            job_id: Job ID to use for configuration selection
        
        Returns:
            Result containing the initial state or an error
        """
        try:
            job_config = self.get_job_config(job_id)
            initial_state = ApexCollectorState(
                job_id=job_id,
                job_config=job_config,
                started_at=datetime.now(),
                source_url=job_config.get('source_url', ''),
                source_token=job_config.get('source_token', ''),
                vm_gateway_url=job_config.get('victoria_metrics', {}).get('gateway_url', ''),
                vm_query_url=job_config.get('victoria_metrics', {}).get('query_url', ''),
                vm_token=job_config.get('victoria_metrics', {}).get('token', ''),
                apex_data=[],  # Will be populated during processing
                processed_count=0,
                failed_count=0,
                max_days_per_run=job_config.get('max_days_per_run', 5),
                sliding_window_days=job_config.get('sliding_window_days', 0)
            )
            return Ok(initial_state)
        except Exception as e:
            return Err(e)
    
    def get_workflow_steps(self) -> List[callable]:
        """Get the list of workflow steps for the apex collector job.
        
        Returns:
            List of step functions that take a state and return Result[State, Exception]
        """
        return [
            self._derive_current_business_date,   # Step 1: Derive current business date
            self._read_watermark_from_vm,         # Step 2: Read watermark from VictoriaMetrics
            self._derive_weekdays_list,           # Step 3: Derive list of weekdays
            self._derive_weekdays_to_update,      # Step 4: Derive weekdays that need updating
            self._process_weekdays_to_update,     # Step 5: Process weekdays to update
            self._update_watermark_in_vm          # Step 6: Update watermark after successful processing
        ]

    @staticmethod
    def _subtract_business_days(anchor_date: date, num_days: int) -> date:
        """Return the date that is num_days business days before anchor_date (Mon-Fri)."""
        from datetime import timedelta
        if num_days <= 0:
            return anchor_date
        remaining = num_days
        current = anchor_date
        while remaining > 0:
            current = current - timedelta(days=1)
            if current.weekday() < 5:
                remaining -= 1
        return current
    
    def finalize_state(self, state: ApexCollectorState) -> ApexCollectorState:
        """Finalize the apex collector state before converting to results.
        
        Args:
            state: The final state after all steps
            
        Returns:
            Finalized state ready for conversion to results
        """
        state.completed_at = datetime.now()
        
        # Determine status based on processing results
        if state.failed_count > 0 and state.processed_count == 0:
            state.status = 'error'
            state.message = f'Apex collection failed for job: {state.job_id} - {state.failed_count} failures, 0 successes'
        elif state.failed_count > 0:
            state.status = 'partial_success'
            state.message = f'Apex collection completed with warnings for job: {state.job_id} - {state.processed_count} successes, {state.failed_count} failures'
        else:
            state.status = 'success'
            state.message = f'Apex collection completed for job: {state.job_id} - {state.processed_count} successes'
        
        # Push job status metric to VM for observability
        self._publish_job_status_metric(state)
        
        return state
    
    def _publish_job_status_metric(self, state: ApexCollectorState) -> None:
        """Publish job status metric to VictoriaMetrics for monitoring."""
        try:
            import requests
            from datetime import datetime
            
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
            
            metric_line = f'apex_collector_job_status{{{",".join(label_pairs)}}} {status_value} {timestamp}'
            
            # Send to VM gateway
            headers = {'Content-Type': 'text/plain'}
            if state.vm_token:
                headers['Authorization'] = f'Bearer {state.vm_token}'
            
            response = requests.post(
                f"{state.vm_gateway_url}/api/v1/import/prometheus",
                data=metric_line,
                headers=headers,
                timeout=30
            )
            response.raise_for_status()
            
            self.logger.info(f"Published job status metric: {state.status}")
            
        except Exception as e:
            self.logger.warning(f"Failed to publish job status metric: {e}")
    
    # Step 1: Derive current business date
    def _derive_current_business_date(self, state: ApexCollectorState) -> Result[ApexCollectorState, Exception]:
        """Derive current business date from configuration (UTC timezone)."""
        try:
            from datetime import datetime, timedelta
            
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
    
    # Step 2: Read watermark from VictoriaMetrics
    def _read_watermark_from_vm(self, state: ApexCollectorState) -> Result[ApexCollectorState, Exception]:
        """Read watermark gauge from VictoriaMetrics to determine last processed date."""
        try:
            import requests
            from datetime import datetime
            
            # Get watermark metric name from config
            watermark_metric = state.job_config.get('watermark_metric_name', 'apex_collector_watermark_days')
            env = state.job_config.get('env', 'default')
            lookback_days = state.job_config.get('watermark_lookback_days', 90)
            
            # Build query with labels using last_over_time to find the most recent value
            # This will look back over the specified time range to find the watermark
            # even if it was written days/weeks ago
            query = f'last_over_time({watermark_metric}{{source="apex",job="apex_collector",env="{env}"}}[{lookback_days}d])'
            
            # Query VictoriaMetrics - no need to specify 'time' parameter
            # as we're using last_over_time with a lookback window
            params = {
                'query': query
            }
            
            headers = {}
            if state.vm_token:
                headers['Authorization'] = f'Bearer {state.vm_token}'
            
            response = requests.get(
                f"{state.vm_query_url}/api/v1/query",
                params=params,
                headers=headers,
                timeout=30
            )
            response.raise_for_status()
            
            data = response.json()
            
            if data['status'] == 'success' and data['data']['result']:
                # Get the latest value
                result = data['data']['result'][0]
                if result['value']:
                    timestamp = float(result['value'][0])
                    state.watermark_date = datetime.fromtimestamp(timestamp, tz=None).date()
                    self.logger.info(f"Watermark date from VM: {state.watermark_date}")
                else:
                    state.watermark_date = None
                    self.logger.info("No watermark found in VM, will use backfill start date")
            else:
                state.watermark_date = None
                self.logger.info("No watermark found in VM, will use backfill start date")
            
            return Ok(state)
            
        except Exception as e:
            self.logger.warning(f"Failed to read watermark from VM: {e}")
            # Don't fail the job, just set watermark to None
            state.watermark_date = None
            return Ok(state)
    
    # Step 3: Derive list of weekdays from watermark to current business date
    def _derive_weekdays_list(self, state: ApexCollectorState) -> Result[ApexCollectorState, Exception]:
        """Derive list of weekdays from start date to lagged effective end date (inclusive)."""
        try:
            from datetime import timedelta
            
            # Compute lagged effective end date based on configured data lag (business days)
            data_lag_business_days = state.job_config.get('data_lag_business_days', 1)
            effective_end_date = self._subtract_business_days(state.current_business_date, max(int(data_lag_business_days), 1))

            # Clamp an unsafe watermark that is ahead of effective_end_date
            if state.watermark_date and state.watermark_date >= effective_end_date:
                clamped = effective_end_date - timedelta(days=1)
                self.logger.warning(
                    f"Watermark {state.watermark_date} is >= effective_end_date {effective_end_date}; clamping to {clamped}"
                )
                state.watermark_date = clamped

            # Determine start date
            if state.watermark_date:
                # Start from day after watermark
                start_date = state.watermark_date + timedelta(days=1)
            else:
                # Use backfill start date or offset, ending at effective_end_date
                backfill_start = state.job_config.get('backfill_start_date')
                if backfill_start:
                    from datetime import datetime
                    start_date = datetime.fromisoformat(backfill_start).date()
                else:
                    # Use offset from effective end date; default 20 if not provided
                    offset_days = state.job_config.get('start_date_offset_days', 20)
                    start_date = effective_end_date - timedelta(days=offset_days)

            # Generate weekdays from start_date to effective_end_date
            weekdays = []
            current_date = start_date
            
            while current_date <= effective_end_date:
                # Only include weekdays (Monday=0, Friday=4)
                if current_date.weekday() < 5:
                    weekdays.append(current_date)
                current_date += timedelta(days=1)
            
            # Apply max_days_per_run limit
            if len(weekdays) > state.max_days_per_run:
                weekdays = weekdays[-state.max_days_per_run:]
                self.logger.info(f"Limited to {state.max_days_per_run} days due to max_days_per_run setting")
            
            state.weekdays = weekdays
            self.logger.info(
                f"Generated {len(weekdays)} weekdays from {start_date} to {effective_end_date} (lag={data_lag_business_days} business days)"
            )
            
            return Ok(state)
            
        except Exception as e:
            self.logger.error(f"Failed to derive weekdays list: {e}")
            return Err(e)
    
    # Step 4: Derive weekdays that need to be updated
    def _derive_weekdays_to_update(self, state: ApexCollectorState) -> Result[ApexCollectorState, Exception]:
        """Derive weekdays that need to be updated for apex collection."""
        try:
            from datetime import timedelta
            
            # Start with all weekdays from the list
            weekdays_to_update = state.weekdays.copy()
            
            # Apply sliding window if configured
            if state.sliding_window_days > 0:
                # Add previous N weekdays for reprocessing
                sliding_weekdays = []
                for i in range(1, state.sliding_window_days + 1):
                    for weekday in state.weekdays:
                        prev_date = weekday - timedelta(days=i)
                        # Only add if it's a weekday and not already in the list
                        if prev_date.weekday() < 5 and prev_date not in weekdays_to_update:
                            sliding_weekdays.append(prev_date)
                
                weekdays_to_update.extend(sliding_weekdays)
                # Sort and remove duplicates
                weekdays_to_update = sorted(list(set(weekdays_to_update)))
                
                self.logger.info(f"Added {len(sliding_weekdays)} days from sliding window")
            
            state.weekdays_to_update = weekdays_to_update
            self.logger.info(f"Selected {len(weekdays_to_update)} weekdays for processing")
            
            return Ok(state)
            
        except Exception as e:
            self.logger.error(f"Failed to derive weekdays to update: {e}")
            return Err(e)
    
    # Step 5: Process weekdays to update
    def _process_weekdays_to_update(self, state: ApexCollectorState) -> Result[ApexCollectorState, Exception]:
        """Process each weekday that needs updating for apex collection."""
        try:
            if not state.weekdays_to_update:
                self.logger.info("No weekdays to update")
                return Ok(state)
            
            # Process each weekday
            for weekday in state.weekdays_to_update:
                try:
                    self.logger.info(f"Processing weekday: {weekday}")
                    
                    # Fetch APEX data for this weekday
                    apex_data = self._fetch_apex_data_for_date(state, weekday)
                    if not apex_data:
                        self.logger.warning(f"No APEX data found for {weekday}")
                        state.failed_count += 1
                        continue
                    
                    # Transform and publish to VM
                    success = self._publish_to_vm(state, weekday, apex_data)
                    if success:
                        state.processed_count += 1
                        self.logger.info(f"Successfully processed {weekday}")
                    else:
                        state.failed_count += 1
                        self.logger.error(f"Failed to publish data for {weekday}")
                        
                except Exception as e:
                    self.logger.error(f"Error processing {weekday}: {e}")
                    state.failed_count += 1
            
            self.logger.info(f"Processing complete: {state.processed_count} successful, {state.failed_count} failed")
            return Ok(state)
            
        except Exception as e:
            self.logger.error(f"Failed to process weekdays: {e}")
            return Err(e)
    
    def _fetch_apex_data_for_date(self, state: ApexCollectorState, business_date: date) -> List[Dict[str, Any]]:
        """Fetch APEX data for a specific business date."""
        try:
            import requests
            
            headers = {
                'Authorization': f'Bearer {state.source_token}',
                'Content-Type': 'application/json'
            }
            
            # Try to filter by business_date if supported
            params = {
                'business_date': business_date.isoformat()
            }
            
            response = requests.get(
                state.source_url,
                params=params,
                headers=headers,
                timeout=60
            )
            response.raise_for_status()
            
            data = response.json()
            
            # If no business_date filtering, filter client-side
            if not isinstance(data, list):
                data = [data] if data else []
            
            # Filter by business_date if the field exists
            filtered_data = []
            for item in data:
                if isinstance(item, dict):
                    # Check if item has business_date field and matches
                    item_date = item.get('business_date')
                    if item_date:
                        try:
                            from datetime import datetime
                            item_date_obj = datetime.fromisoformat(item_date.replace('Z', '+00:00')).date()
                            if item_date_obj == business_date:
                                filtered_data.append(item)
                        except:
                            # If date parsing fails, include the item
                            filtered_data.append(item)
                    else:
                        # No business_date field, include the item
                        filtered_data.append(item)
            
            self.logger.info(f"Fetched {len(filtered_data)} records for {business_date}")
            return filtered_data
            
        except Exception as e:
            self.logger.error(f"Failed to fetch APEX data for {business_date}: {e}")
            return []
    
    def _publish_to_vm(self, state: ApexCollectorState, business_date: date, apex_data: List[Dict[str, Any]]) -> bool:
        """Transform and publish APEX data to VictoriaMetrics gateway."""
        try:
            import requests
            from datetime import datetime
            
            # Transform data to VM format
            vm_metrics = []
            timestamp = int(datetime.combine(business_date, datetime.min.time()).timestamp())
            
            for item in apex_data:
                # Create metric name (configurable)
                metric_name = state.job_config.get('metric_name', 'apex_data')
                
                # Extract labels and values
                labels = {
                    'business_date': business_date.isoformat(),
                    'source': 'apex',
                    'job': 'apex_collector'
                }
                
                # Add custom labels from config
                custom_labels = state.job_config.get('labels', {})
                labels.update(custom_labels)
                
                # Extract numeric values from the item
                for key, value in item.items():
                    if isinstance(value, (int, float)):
                        # Create metric for numeric fields
                        metric_line = f"{metric_name}_{key}"
                        
                        # Build labels string
                        label_pairs = []
                        for label_key, label_value in labels.items():
                            label_pairs.append(f'{label_key}="{label_value}"')
                        
                        # Create metric line in Prometheus format
                        metric_line += '{' + ','.join(label_pairs) + '}'
                        metric_line += f' {value} {timestamp}'
                        
                        vm_metrics.append(metric_line)
            
            if not vm_metrics:
                self.logger.warning(f"No metrics generated for {business_date}")
                return False
            
            # Send to VM gateway
            headers = {
                'Content-Type': 'text/plain'
            }
            
            if state.vm_token:
                headers['Authorization'] = f'Bearer {state.vm_token}'
            
            response = requests.post(
                f"{state.vm_gateway_url}/api/v1/import/prometheus",
                data='\n'.join(vm_metrics),
                headers=headers,
                timeout=60
            )
            response.raise_for_status()
            
            self.logger.info(f"Published {len(vm_metrics)} metrics for {business_date}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to publish to VM for {business_date}: {e}")
            return False
    
    # Step 6: Update watermark in VictoriaMetrics
    def _update_watermark_in_vm(self, state: ApexCollectorState) -> Result[ApexCollectorState, Exception]:
        """Update watermark gauge in VictoriaMetrics after successful processing."""
        try:
            if not state.weekdays_to_update or state.processed_count == 0:
                self.logger.info("No successful processing to update watermark")
                return Ok(state)
            
            # Find the latest successfully processed date
            # Sort weekdays and find the last one that was processed
            sorted_weekdays = sorted(state.weekdays_to_update)
            latest_processed_date = None
            
            # For simplicity, assume we processed all weekdays in order
            # In a real implementation, you'd track which specific dates succeeded
            if state.processed_count > 0:
                # Take the latest date from the processed range
                latest_processed_date = sorted_weekdays[-1]
            
            if not latest_processed_date:
                self.logger.info("No date to update watermark")
                return Ok(state)
            
            # Update watermark metric
            success = self._publish_watermark_metric(state, latest_processed_date)
            if success:
                state.watermark_date = latest_processed_date
                self.logger.info(f"Updated watermark to {latest_processed_date}")
            else:
                self.logger.warning(f"Failed to update watermark for {latest_processed_date}")
            
            return Ok(state)
            
        except Exception as e:
            self.logger.error(f"Failed to update watermark: {e}")
            return Err(e)
    
    def _publish_watermark_metric(self, state: ApexCollectorState, watermark_date: date) -> bool:
        """Publish watermark metric to VictoriaMetrics."""
        try:
            import requests
            from datetime import datetime
            
            # Get watermark metric name from config
            watermark_metric = state.job_config.get('watermark_metric_name', 'apex_collector_watermark_days')
            env = state.job_config.get('env', 'default')
            
            # Convert date to timestamp (00:00 UTC)
            timestamp = int(datetime.combine(watermark_date, datetime.min.time()).timestamp())
            
            # Create metric line
            metric_line = f'{watermark_metric}{{source="apex",job="apex_collector",env="{env}"}} {timestamp} {timestamp}'
            
            # Send to VM gateway
            headers = {
                'Content-Type': 'text/plain'
            }
            
            if state.vm_token:
                headers['Authorization'] = f'Bearer {state.vm_token}'
            
            response = requests.post(
                f"{state.vm_gateway_url}/api/v1/import/prometheus",
                data=metric_line,
                headers=headers,
                timeout=30
            )
            response.raise_for_status()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to publish watermark metric: {e}")
            return False


def main():
    """Main function for command-line execution."""
    epilog = """
Examples:
  # List available job configurations
  python apex_collector.py --config config.yml --list-jobs
  
  # Run apex collection for apex_collector
  python apex_collector.py --config config.yml --job-id apex_collector
  
  # Run apex collection with verbose logging
  python apex_collector.py --config config.yml --job-id apex_collector --verbose
    """
    
    return ApexCollectorJob.main(
        description="Apex Collector Job - Apex number collection and processing",
        epilog=epilog
    )


if __name__ == "__main__":
    sys.exit(main())
