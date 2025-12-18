"""
Victoria Metrics query helper functions.

This module provides utilities for connecting to Victoria Metrics and querying
historical metrics that are shared across different forecasting model notebooks.
"""

from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta, timezone
from prometheus_api_client import PrometheusConnect


def create_victoria_metrics_client(
    query_url: str,
    token: Optional[str] = None,
) -> PrometheusConnect:
    """Create a Prometheus/Victoria Metrics client connection.
    
    Args:
        query_url: Victoria Metrics query URL (e.g., 'http://victoria-metrics:8428')
        token: Optional bearer token for authentication
        
    Returns:
        PrometheusConnect client instance configured for Victoria Metrics
        
    Example:
        >>> client = create_victoria_metrics_client(
        ...     query_url='http://victoria-metrics:8428',
        ...     token='my-token'
        ... )
        >>> print(f"Connected to {client.url}")
    """
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    
    client = PrometheusConnect(url=query_url, headers=headers, disable_ssl=True)
    return client


def query_historical_metrics(
    client: PrometheusConnect,
    selector: str,
    history_days: int,
    end_date: Optional[datetime] = None,
    step: str = "24h",
) -> List[Dict[str, Any]]:
    """Query historical metrics from Victoria Metrics.
    
    Args:
        client: PrometheusConnect client instance
        selector: PromQL selector string (e.g., '{job="extractor"}')
        history_days: Number of days of history to fetch
        end_date: End date for the query (defaults to now UTC)
        step: Query step interval (default: "24h" for daily data)
        
    Returns:
        List of query result items, where each item has 'metric' and 'values'
        
    Example:
        >>> client = create_victoria_metrics_client('http://vm:8428')
        >>> results = query_historical_metrics(
        ...     client=client,
        ...     selector='{job="extractor"}',
        ...     history_days=365
        ... )
        >>> print(f"Found {len(results)} time series")
    """
    if end_date is None:
        end_date = datetime.now(timezone.utc)
    
    start_date = end_date - timedelta(days=history_days)
    
    # Ensure selector uses double quotes (PromQL requirement)
    selector = selector.replace("'", '"')
    
    query_result = client.custom_query_range(
        query=selector,
        start_time=start_date,
        end_time=end_date,
        step=step
    )
    
    return query_result

