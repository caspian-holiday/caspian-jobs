"""
Prometheus/VictoriaMetrics query helper functions.

This module provides utilities for parsing and processing Prometheus query results
that are shared across different forecasting model notebooks.
"""

from typing import List, Tuple, Dict, Any
from datetime import datetime, timezone


def parse_all_series(query_result: List[Dict[str, Any]]) -> List[Tuple[List[Tuple[datetime, float]], Dict[str, Any]]]:
    """Parse all series from Prometheus query result.
    
    Parses a Prometheus/VictoriaMetrics query result and returns a list of
    time series data. Each series is returned as a tuple containing:
    - samples: List of (datetime, float) tuples representing the time series data
    - series_info: Dictionary with 'metric_name' and 'labels' keys
    
    Args:
        query_result: List of Prometheus query result items, where each item
                     has a 'metric' dict and 'values' list
    
    Returns:
        List of tuples: (samples, series_info) for each series found
        - samples: List of (datetime, float) tuples
        - series_info: Dict with 'metric_name' (str) and 'labels' (Dict[str, str])
    
    Example:
        >>> query_result = [
        ...     {
        ...         'metric': {'__name__': 'cpu_usage', 'instance': 'server1'},
        ...         'values': [[1609459200.0, '0.5'], [1609545600.0, '0.6']]
        ...     }
        ... ]
        >>> series_list = parse_all_series(query_result)
        >>> samples, info = series_list[0]
        >>> info['metric_name']
        'cpu_usage'
        >>> info['labels']
        {'instance': 'server1'}
    """
    if not query_result:
        return []
    
    series_list = []
    for item in query_result:
        metric = item.get('metric', {})
        metric_name = metric.get('__name__')
        if not metric_name:
            continue
        
        labels = {k: v for k, v in metric.items() if k != '__name__'}
        values = item.get('values', [])
        
        # Convert to list of tuples
        samples = []
        for ts, value in values:
            dt = datetime.fromtimestamp(float(ts), tz=timezone.utc)
            samples.append((dt, float(value)))
        
        if samples:  # Only include series with data
            series_list.append((samples, {'metric_name': metric_name, 'labels': labels}))
    
    return series_list

