"""
Database helper functions for forecast notebooks.

This module provides reusable database interaction functions extracted from
the metrics_forecast job, allowing notebooks to save forecasts to the database
without duplicating code.
"""

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from datetime import date, datetime, timezone
from urllib.parse import quote_plus

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

# Add the scheduler module to the path for imports
# This allows notebooks to use ConfigLoader
# Find project root: look for directory containing victoria_metrics_jobs/
_helper_dir = Path(__file__).parent.resolve()
_project_root = None

# Try current working directory first (most reliable for notebooks)
_cwd = Path.cwd()
if (_cwd / 'victoria_metrics_jobs').exists():
    _project_root = _cwd
else:
    # Walk up from helper directory to find project root
    _current = _helper_dir
    for _ in range(6):  # Max 6 levels up
        if (_current / 'victoria_metrics_jobs').exists():
            _project_root = _current
            break
        _current = _current.parent
        if _current == _current.parent:  # Reached filesystem root
            break

if _project_root and str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from victoria_metrics_jobs.scheduler.config import ConfigLoader


def _json_serializer(obj: Any) -> Any:
    """Convert date/datetime and other non-JSON types for json.dumps."""
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    if isinstance(obj, (np.integer, np.floating)):
        return float(obj) if isinstance(obj, np.floating) else int(obj)
    if isinstance(obj, np.bool_):
        return bool(obj)
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def _json_serializer_for_metadata(obj: Any) -> Any:
    """JSON serializer for forecast metadata; handles NaN/inf and uses _json_serializer for rest."""
    try:
        if isinstance(obj, (float, np.floating)) and (np.isnan(obj) or np.isinf(obj)):
            return None
    except (TypeError, ValueError):
        pass
    return _json_serializer(obj)


def load_database_config_from_yaml(
    config_path: Optional[str] = None,
    environment: Optional[str] = None
) -> Dict[str, Any]:
    """Load database configuration from YAML config file using VM_JOBS_ENVIRONMENT.
    
    This function:
    1. Loads the environment-specific configuration from the YAML file
    2. Extracts the database section
    3. Overrides the password with VM_JOBS_DB_PASSWORD environment variable
    
    Args:
        config_path: Path to the YAML configuration file (required). Notebooks should read from VM_JOBS_CONFIG_PATH env var and pass it here.
        environment: Environment name ('local', 'dev', 'stg', 'prod'). If None, uses VM_JOBS_ENVIRONMENT env var.
    
    Returns:
        Dictionary with database configuration keys: host, port, name, user, password, ssl_mode
        
    Raises:
        ValueError: If environment or config_path is not set
        FileNotFoundError: If config file cannot be found
    """
    # Get environment from parameter or environment variable
    if environment is None:
        environment = os.getenv('VM_JOBS_ENVIRONMENT')
    
    if not environment:
        raise ValueError(
            "Environment must be specified. Provide 'environment' parameter or set "
            "VM_JOBS_ENVIRONMENT environment variable to 'local', 'dev', 'stg', or 'prod'"
        )
    
    # Config file path must be provided as parameter (notebooks read from env var)
    if not config_path:
        raise ValueError(
            "Configuration file path must be specified. Provide 'config_path' parameter."
        )
    
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    # Load configuration using ConfigLoader
    config_loader = ConfigLoader()
    env_config = config_loader.load(config_path, environment=environment)
    
    # Extract database configuration
    database_config = env_config.get('database', {})
    if not database_config:
        raise ValueError(
            f"No 'database' section found in environment '{environment}' configuration"
        )
    
    # Override password with VM_JOBS_DB_PASSWORD environment variable
    password = os.getenv('VM_JOBS_DB_PASSWORD')
    if not password:
        raise ValueError(
            "VM_JOBS_DB_PASSWORD environment variable must be set"
        )
    
    # Build database config dict
    db_config = {
        'host': database_config.get('host', 'localhost'),
        'port': int(database_config.get('port', 5432)),
        'name': database_config.get('name', 'scheduler_local'),
        'user': database_config.get('user', 'scheduler'),
        'password': password,  # Always use env var, override YAML value
        'ssl_mode': database_config.get('ssl_mode', 'prefer'),
    }
    
    return db_config


def build_database_connection_string(
    connection_string: Optional[str] = None,
    host: Optional[str] = None,
    port: Optional[int] = None,
    dbname: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    ssl_mode: Optional[str] = None,
) -> str:
    """Build PostgreSQL connection string from components or return existing one.
    
    Args:
        connection_string: Full connection string (if provided, returned as-is)
        host: Database host (default: 'localhost')
        port: Database port (default: 5432)
        dbname: Database name (default: 'forecasts_db')
        user: Database user (default: 'forecast_user')
        password: Database password (will be URL-encoded)
        ssl_mode: SSL mode (default: 'prefer')
        
    Returns:
        PostgreSQL connection string
    """
    if connection_string:
        return connection_string
    
    # Build from components with defaults
    host = host or "localhost"
    port = port or 5432
    dbname = dbname or "forecasts_db"
    user = user or "forecast_user"
    password = password or ""
    ssl_mode = ssl_mode or "prefer"
    
    # URL-encode password to handle special characters
    if password:
        password = quote_plus(password)
    
    return f"postgresql://{user}:{password}@{host}:{port}/{dbname}?sslmode={ssl_mode}"


def create_database_connection(
    connection_string: Optional[str] = None,
    host: Optional[str] = None,
    port: Optional[int] = None,
    dbname: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    ssl_mode: Optional[str] = None,
    config_path: Optional[str] = None,
    environment: Optional[str] = None,
) -> Tuple[Engine, Any]:
    """Create database connection and engine.
    
    If connection parameters are not provided, loads them from the YAML config file
    using VM_JOBS_ENVIRONMENT (from parameter or env var) and VM_JOBS_DB_PASSWORD environment variable.
    
    Args:
        connection_string: Full connection string (if provided, used as-is, ignores other params)
        host: Database host (used if connection_string not provided)
        port: Database port (used if connection_string not provided)
        dbname: Database name (used if connection_string not provided)
        user: Database user (used if connection_string not provided)
        password: Database password (used if connection_string not provided)
        ssl_mode: SSL mode (used if connection_string not provided)
        config_path: Path to YAML config file (used if connection_string and individual params not provided)
        environment: Environment name ('local', 'dev', 'stg', 'prod'). If None, uses VM_JOBS_ENVIRONMENT env var.
        
    Returns:
        Tuple of (engine, connection)
    """
    # If connection_string provided, use it directly
    if connection_string:
        engine = create_engine(connection_string)
        conn = engine.connect()
        return engine, conn
    
    # If individual params not provided, load from config
    if not all([host, port, dbname, user, password]):
        db_config = load_database_config_from_yaml(config_path=config_path, environment=environment)
        host = host or db_config['host']
        port = port or db_config['port']
        dbname = dbname or db_config['name']
        user = user or db_config['user']
        password = password or db_config['password']
        ssl_mode = ssl_mode or db_config['ssl_mode']
    
    conn_str = build_database_connection_string(
        connection_string=None,
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
        ssl_mode=ssl_mode,
    )
    
    engine = create_engine(conn_str)
    conn = engine.connect()
    
    return engine, conn


def normalize_metric_labels_for_comparison(labels: Dict[str, str]) -> str:
    """Normalize metric labels for consistent comparison.
    
    Sorts labels by key and converts to JSON string to ensure consistent
    matching when searching for existing metric_id entries.
    
    Args:
        labels: Dictionary of metric labels
        
    Returns:
        JSON string with sorted keys
    """
    sorted_labels = dict(sorted(labels.items()))
    return json.dumps(sorted_labels, sort_keys=True)


def find_or_get_job_idx(conn: Any, job_id: str) -> Optional[int]:
    """Find existing job_idx for a given job_id.
    
    Returns None if no job_idx exists - the first metric entry will create it automatically.
    
    Args:
        conn: Database connection
        job_id: Job ID to look up
        
    Returns:
        job_idx value if found, None if not found (will be created with first metric)
    """
    try:
        query = text("""
            SELECT DISTINCT job_idx
            FROM public.vm_metric_metadata
            WHERE job_id = :job_id
            LIMIT 1
        """)
        
        result = conn.execute(query, {"job_id": job_id})
        row = result.fetchone()
        
        if row:
            return row[0]
        
        # No existing job_idx found - return None
        # The first metric entry will auto-generate the job_idx via BIGSERIAL
        return None
        
    except SQLAlchemyError as exc:
        raise RuntimeError(f"Database error finding job_idx for job_id='{job_id}': {exc}") from exc
    except Exception as exc:
        raise RuntimeError(f"Failed to find job_idx for job_id='{job_id}': {exc}") from exc


def find_or_get_metric_id(
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
        normalized_labels_json = normalize_metric_labels_for_comparison(metric_labels)
        
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
            
            return (new_job_idx, new_metric_id)
        
    except SQLAlchemyError as exc:
        if conn:
            conn.rollback()
        raise RuntimeError(
            f"Database error finding/creating metric_id for job_id='{job_id}', metric_name='{metric_name}': {exc}"
        ) from exc
    except Exception as exc:
        if conn:
            conn.rollback()
        raise RuntimeError(
            f"Failed to find/create metric_id for job_id='{job_id}', metric_name='{metric_name}': {exc}"
        ) from exc


def create_forecast_run_record(
    conn: Any,
    job_id: str,
    selection_value: str,
    model_type: str,
    model_config: Dict[str, Any],
    model_fit_config: Optional[Dict[str, Any]] = None,
    history_days: Optional[int] = None,
    forecast_horizon_days: Optional[int] = None,
    min_history_points: Optional[int] = None,
    business_date: Optional[datetime] = None,
    config_source: str = "notebook",
) -> Optional[int]:
    """Create a forecast run record in vm_forecast_job table.
    
    This function creates a run record that stores model parameters for auditing
    and reproducibility. The run_id can then be used when saving forecast data.
    
    Args:
        conn: Database connection
        job_id: Job identifier (e.g., 'metrics_forecast_notebooks')
        selection_value: PromQL selector string
        model_type: Type of model used ('prophet', 'arima', etc.)
        model_config: Model configuration parameters (e.g., PROPHET_PARAMS or ARIMA_PARAMS)
        model_fit_config: Optional model fit parameters (e.g., PROPHET_FIT_PARAMS)
        history_days: Days of history used (optional)
        forecast_horizon_days: Forecast horizon in days (optional)
        min_history_points: Minimum history points required (optional)
        business_date: Business date for this run (optional, defaults to today)
        config_source: Source of configuration (default: 'notebook')
        
    Returns:
        run_id if successful, None otherwise
    """
    try:
        if business_date is None:
            business_date = datetime.now(timezone.utc).date()
        elif isinstance(business_date, datetime):
            business_date = business_date.date()
        
        # Store model config in the appropriate field based on model type
        # For backward compatibility, Prophet uses prophet_config field
        # For other models, we'll store in a generic model_config JSONB field
        # Note: The schema may need to be extended to support this properly
        
        # For now, we'll use prophet_config for Prophet and store model_type + config
        # In a real implementation, you might want to add a model_type column
        # or use a generic model_config JSONB field
        
        # Build the config JSON - include model_type for clarity
        config_json = {
            "model_type": model_type,
            **model_config  # Merge model-specific parameters
        }
        
        # For Prophet, use the existing prophet_config field
        # For other models, we'll also use prophet_config but include model_type
        prophet_config_json = json.dumps(config_json, default=_json_serializer)
        prophet_fit_config_json = json.dumps(model_fit_config, default=_json_serializer) if model_fit_config else None
        
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
            "job_id": job_id,
            "selection_value": selection_value,
            "prophet_config": prophet_config_json,
            "prophet_fit_config": prophet_fit_config_json,
            "config_source": config_source,
            "history_days": history_days,
            "forecast_horizon_days": forecast_horizon_days,
            "min_history_points": min_history_points,
            "business_date": business_date,
            "started_at": datetime.now(timezone.utc),
            "status": "running",
        })
        
        conn.commit()
        run_id = result.fetchone()[0]
        
        return run_id
        
    except SQLAlchemyError as exc:
        if conn:
            conn.rollback()
        raise RuntimeError(f"Database error creating forecast run record: {exc}") from exc
    except Exception as exc:
        if conn:
            conn.rollback()
        raise RuntimeError(f"Failed to create forecast run record: {exc}") from exc


def check_forecast_metadata_table_exists(conn: Any) -> bool:
    """Return True if public.vm_metrics_forecast_metadata table exists."""
    try:
        result = conn.execute(text("""
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = 'vm_metrics_forecast_metadata'
            LIMIT 1
        """))
        return result.fetchone() is not None
    except Exception:
        return False


def update_forecast_run_record(
    conn: Any,
    run_id: int,
    series_count: Optional[int] = None,
    success_count: Optional[int] = None,
    failed_count: Optional[int] = None,
    status: Optional[str] = None,
    completed_at: Optional[datetime] = None,
    duration_seconds: Optional[float] = None,
    error_message: Optional[str] = None,
) -> None:
    """Update a forecast run record with counts and completion status."""
    try:
        updates = []
        params = {"run_id": run_id}
        if series_count is not None:
            updates.append("series_count = :series_count")
            params["series_count"] = series_count
        if success_count is not None:
            updates.append("success_count = :success_count")
            params["success_count"] = success_count
        if failed_count is not None:
            updates.append("failed_count = :failed_count")
            params["failed_count"] = failed_count
        if status is not None:
            updates.append("status = :status")
            params["status"] = status
        if completed_at is not None:
            updates.append("completed_at = :completed_at")
            params["completed_at"] = completed_at
        if duration_seconds is not None:
            updates.append("duration_seconds = :duration_seconds")
            params["duration_seconds"] = duration_seconds
        if error_message is not None:
            updates.append("error_message = :error_message")
            params["error_message"] = error_message
        if not updates:
            return
        update_sql = text(
            "UPDATE public.vm_forecast_job SET " + ", ".join(updates) + " WHERE run_id = :run_id"
        )
        conn.execute(update_sql, params)
        conn.commit()
    except SQLAlchemyError as exc:
        if conn:
            conn.rollback()
        raise RuntimeError(f"Database error updating forecast run record: {exc}") from exc


def save_forecast_metadata_for_metric(
    conn: Any,
    run_id: int,
    job_id: str,
    metric_name: str,
    metric_labels: Dict[str, str],
    tsfel_features: Dict[str, Any],
    classification: Dict[str, str],
    prophet_params: Optional[Dict[str, Any]] = None,
) -> None:
    """Insert or update one row in vm_metrics_forecast_metadata for a metric in a run.

    Resolves the source metric (job_idx, metric_id) via find_or_get_metric_id,
    then upserts a row with metadata JSON containing tsfel_features,
    classification (category, reason), and prophet_params (or null).

    Uses the same metric_labels convention as the extractor: "job" is stored
    in the job_id column, not in metric_labels, so we exclude it before lookup
    to resolve the same (job_idx, metric_id) as the extractor would.

    Args:
        conn: Database connection
        run_id: Forecast run id from vm_forecast_job
        job_id: Source job id (e.g. labels["job"], e.g. "extractor")
        metric_name: Metric name
        metric_labels: Metric labels from the series (will be normalized for lookup)
        tsfel_features: Dict of TSFEL/stat features (serializable)
        classification: Dict with "category" and "reason" keys
        prophet_params: Prophet parameters for this metric, or None when Not Suitable
    """
    try:
        # Match extractor convention: job is stored in job_id column, not in metric_labels.
        # Excluding "job" ensures we resolve the same (job_idx, metric_id) as the
        # extractor, avoiding duplicate source metrics in vm_metric_metadata.
        source_metric_labels = {k: v for k, v in metric_labels.items() if k != "job"}

        job_idx = find_or_get_job_idx(conn, job_id)
        job_idx, metric_id = find_or_get_metric_id(
            conn,
            job_idx,
            job_id,
            metric_name,
            source_metric_labels,
        )
        if job_idx is None or metric_id is None:
            raise RuntimeError(
                f"Failed to resolve (job_idx, metric_id) for job_id='{job_id}', metric_name='{metric_name}'"
            )

        metadata_payload = {
            "tsfel_features": tsfel_features,
            "classification": classification,
            "prophet_params": prophet_params,
        }
        metadata_json = json.dumps(metadata_payload, default=_json_serializer_for_metadata)

        upsert_sql = text("""
            INSERT INTO public.vm_metrics_forecast_metadata (
                job_idx, metric_id, run_id, metadata
            )
            VALUES (
                :job_idx, :metric_id, :run_id, CAST(:metadata AS jsonb)
            )
            ON CONFLICT (job_idx, metric_id, run_id)
            DO UPDATE SET metadata = EXCLUDED.metadata
        """)
        conn.execute(upsert_sql, {
            "job_idx": job_idx,
            "metric_id": metric_id,
            "run_id": run_id,
            "metadata": metadata_json,
        })
        conn.commit()
    except SQLAlchemyError as exc:
        if conn:
            conn.rollback()
        raise RuntimeError(
            f"Database error saving forecast metadata for {metric_name}: {exc}"
        ) from exc
    except Exception as exc:
        if conn:
            conn.rollback()
        raise RuntimeError(
            f"Failed to save forecast metadata for {metric_name}: {exc}"
        ) from exc


def save_forecast_metadata_for_metric_by_id(
    conn: Any,
    run_id: int,
    job_idx: int,
    metric_id: int,
    tsfel_features: Dict[str, Any],
    classification: Dict[str, str],
    prophet_params: Optional[Dict[str, Any]] = None,
) -> None:
    """Insert or update one row in vm_metrics_forecast_metadata using known (job_idx, metric_id).

    Use this when (job_idx, metric_id) were already resolved (e.g. from
    save_forecasts_to_database) so no lookup is needed. The (job_idx, metric_id)
    must reference the forecast metric row in vm_metric_metadata (same as used
    for vm_metric_data forecast points).

    Args:
        conn: Database connection
        run_id: Forecast run id from vm_forecast_job
        job_idx: job_idx from vm_metric_metadata (forecast metric)
        metric_id: metric_id from vm_metric_metadata (forecast metric)
        tsfel_features: Dict of TSFEL/stat features (serializable)
        classification: Dict with "category" and "reason" keys
        prophet_params: Prophet parameters for this metric, or None when Not Suitable
    """
    try:
        metadata_payload = {
            "tsfel_features": tsfel_features,
            "classification": classification,
            "prophet_params": prophet_params,
        }
        metadata_json = json.dumps(metadata_payload, default=_json_serializer_for_metadata)

        upsert_sql = text("""
            INSERT INTO public.vm_metrics_forecast_metadata (
                job_idx, metric_id, run_id, metadata
            )
            VALUES (
                :job_idx, :metric_id, :run_id, CAST(:metadata AS jsonb)
            )
            ON CONFLICT (job_idx, metric_id, run_id)
            DO UPDATE SET metadata = EXCLUDED.metadata
        """)
        conn.execute(upsert_sql, {
            "job_idx": job_idx,
            "metric_id": metric_id,
            "run_id": run_id,
            "metadata": metadata_json,
        })
        conn.commit()
    except SQLAlchemyError as exc:
        if conn:
            conn.rollback()
        raise RuntimeError(
            f"Database error saving forecast metadata (job_idx={job_idx}, metric_id={metric_id}): {exc}"
        ) from exc
    except Exception as exc:
        if conn:
            conn.rollback()
        raise RuntimeError(
            f"Failed to save forecast metadata (job_idx={job_idx}, metric_id={metric_id}): {exc}"
        ) from exc


def save_forecasts_to_database(
    conn: Any,
    metric_name: str,
    labels: Dict[str, str],
    forecast_df: pd.DataFrame,
    forecast_types: List[Dict[str, str]],
    run_id: Optional[int] = None,
) -> Tuple[int, Optional[int], Optional[int]]:
    """Write forecast data to database (vm_metric_data and vm_metric_metadata).
    
    This function:
    1. Extracts job label and adds "_forecast" suffix
    2. Removes job label from metric_labels
    3. Adds forecast_type to metric_labels
    4. Finds or creates job_idx and metric_id in vm_metric_metadata
    5. Inserts forecast values into vm_metric_data
    
    Args:
        conn: Database connection
        metric_name: Name of the metric
        labels: Dictionary of metric labels (must include 'job' label)
        forecast_df: Forecast DataFrame with 'ds' column and forecast value columns
                     (e.g., 'yhat', 'yhat_lower', 'yhat_upper')
        forecast_types: List of dicts with 'name' and 'field' keys
                       Example: [{'name': 'trend', 'field': 'yhat'}, ...]
        run_id: Optional run_id from vm_forecast_job table for parameter tracking
        
    Returns:
        Tuple of (rows_written, job_idx, metric_id) for the first forecast type
        (e.g. "trend"), so the caller can use (job_idx, metric_id) for
        vm_metrics_forecast_metadata without looking up again.
        If no rows written, returns (0, None, None).

    Raises:
        ValueError: If 'job' label is missing from labels
    """
    try:
        # Extract job label (required for transformation)
        input_job = labels.get("job")
        if not input_job:
            raise ValueError(f"Missing 'job' label in metric labels for {metric_name}")
        
        # Transform job_id: add "_forecast" suffix
        forecast_job_id = f"{input_job}_forecast"
        
        # Prepare base metric_labels: remove job label and exclude system labels
        excluded_labels = {"job", "auid", "biz_date", "forecast"}
        base_metric_labels = {
            k: v for k, v in labels.items() 
            if k not in excluded_labels
        }
        
        # Find existing job_idx for the forecast_job_id (may be None if first metric)
        job_idx = find_or_get_job_idx(conn, forecast_job_id)
        
        # Each forecast_type becomes its own timeseries with its own metric_id
        # STEP 1: Look up or create ALL metadata entries FIRST (before inserting any data)
        forecast_type_metric_ids = {}
        
        for forecast_type in forecast_types:
            name = forecast_type.get("name")
            if not name:
                continue
            
            # Add forecast_type to metric_labels - this makes each forecast_type a separate timeseries
            metric_labels_with_type = dict(base_metric_labels)
            metric_labels_with_type["forecast_type"] = name
            
            # Find or get metric_id for this forecast_type timeseries
            job_idx, metric_id = find_or_get_metric_id(
                conn,
                job_idx,
                forecast_job_id,
                metric_name,
                metric_labels_with_type
            )
            
            if job_idx is None or metric_id is None:
                continue
            
            forecast_type_metric_ids[name] = metric_id
        
        if not forecast_type_metric_ids:
            return (0, None, None)

        # job_idx and first forecast_type's metric_id for caller (e.g. vm_metrics_forecast_metadata)
        first_type_name = forecast_types[0]["name"] if forecast_types else None
        metric_id_primary = forecast_type_metric_ids.get(first_type_name) if first_type_name else next(iter(forecast_type_metric_ids.values()), None)
        
        # STEP 2: Ensure all metadata entries are committed before inserting data
        conn.commit()
        
        # STEP 3: Now we can safely insert data (FK constraint will be satisfied)
        rows_to_insert = []
        
        for forecast_row in forecast_df.itertuples():
            # Use forecast date as-is (biz_date from Prophet forecast)
            forecast_date = forecast_row.ds.date()
            forecast_timestamp = datetime.combine(
                forecast_date, 
                datetime.min.time()
            ).replace(tzinfo=timezone.utc)
            
            for forecast_type in forecast_types:
                name = forecast_type.get("name")
                field = forecast_type.get("field")
                if not name or not field or not hasattr(forecast_row, field):
                    continue
                
                # Skip if we didn't get a metric_id for this forecast_type
                if name not in forecast_type_metric_ids:
                    continue
                
                value = getattr(forecast_row, field)
                if value is None or np.isnan(value):
                    continue
                
                # Use the pre-looked-up metric_id for this forecast_type
                metric_id = forecast_type_metric_ids[name]
                
                rows_to_insert.append({
                    "job_idx": job_idx,
                    "metric_id": metric_id,
                    "metric_timestamp": forecast_timestamp,
                    "metric_value": float(value),
                    "run_id": run_id,  # Store run_id to link to parameter record
                })
        
        if not rows_to_insert:
            return (0, job_idx, metric_id_primary)
        
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
        
        return (len(rows_to_insert), job_idx, metric_id_primary)
        
    except SQLAlchemyError as exc:
        if conn:
            conn.rollback()
        raise RuntimeError(f"Database error writing forecasts for {metric_name}: {exc}") from exc
    except Exception as exc:
        if conn:
            conn.rollback()
        raise RuntimeError(f"Failed to write forecasts to database for {metric_name}: {exc}") from exc
