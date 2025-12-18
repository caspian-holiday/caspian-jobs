"""
Darts wrapper for Prophet forecasting model.

This module provides helper functions to use Prophet via the darts library
for time series forecasting in Jupyter notebooks.
"""

from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, date
import pandas as pd
import numpy as np

try:
    from darts import TimeSeries
    from darts.models import Prophet as DartsProphet
    DARTS_AVAILABLE = True
except ImportError:
    DARTS_AVAILABLE = False


def dataframe_to_timeseries(
    df: pd.DataFrame,
    time_col: str = "ds",
    value_col: str = "y"
) -> "TimeSeries":
    """
    Convert pandas DataFrame to darts TimeSeries.
    
    Args:
        df: DataFrame with datetime index or time column and value column
        time_col: Name of the time/datetime column (default: "ds")
        value_col: Name of the value column (default: "y")
        
    Returns:
        darts TimeSeries object
        
    Raises:
        ImportError: If darts is not installed
    """
    if not DARTS_AVAILABLE:
        raise ImportError("darts library is required but not installed")
    
    # Ensure time column is datetime
    if time_col in df.columns:
        df = df.copy()
        df[time_col] = pd.to_datetime(df[time_col])
        df = df.set_index(time_col)
    
    # Create TimeSeries from DataFrame
    series = TimeSeries.from_dataframe(df[[value_col]])
    return series


def prepare_business_day_data(
    samples: List[Tuple[datetime, float]]
) -> pd.DataFrame:
    """
    Convert raw samples into a business-day indexed DataFrame.
    
    This is compatible with the format used in metrics_forecast job.
    
    Args:
        samples: List of (datetime, float) tuples
        
    Returns:
        DataFrame with columns ["ds", "y"] indexed by business days
    """
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

    # Reindex to business days and interpolate
    daily = (
        daily.set_index("ds")
        .reindex(all_business_days)
        .rename_axis("ds")
        .reset_index()
    )
    daily["y"] = daily["y"].interpolate(method="linear").ffill().bfill()
    
    return daily[["ds", "y"]]


def future_business_dates(last_history_date: date, periods: int) -> List[pd.Timestamp]:
    """
    Generate the next N business-day timestamps after last_history_date.
    
    Args:
        last_history_date: Last date in history
        periods: Number of business days to generate
        
    Returns:
        List of pd.Timestamp objects for business days
    """
    future_dates: List[pd.Timestamp] = []
    candidate = last_history_date
    while len(future_dates) < periods:
        candidate += pd.Timedelta(days=1)
        if candidate.weekday() >= 5:  # skip weekends
            continue
        future_dates.append(pd.Timestamp(candidate))
    return future_dates


def create_prophet_model(
    prophet_params: Optional[Dict[str, Any]] = None,
    prophet_fit_params: Optional[Dict[str, Any]] = None
) -> "DartsProphet":
    """
    Create a Prophet model using darts wrapper.
    
    Args:
        prophet_params: Dictionary of Prophet model parameters
            (e.g., {"yearly_seasonality": True, "changepoint_prior_scale": 0.05})
        prophet_fit_params: Dictionary of Prophet fit parameters (optional)
            
    Returns:
        DartsProphet model instance
        
    Raises:
        ImportError: If darts is not installed
    """
    if not DARTS_AVAILABLE:
        raise ImportError("darts library is required but not installed")
    
    # Default parameters
    default_params = {
        "yearly_seasonality": True,
        "weekly_seasonality": False,
        "daily_seasonality": False,
        "seasonality_mode": "additive",
    }
    
    if prophet_params:
        default_params.update(prophet_params)
    
    # Create model
    model = DartsProphet(**default_params)
    
    # Store fit params if provided (darts may not use these directly,
    # but we store them for reference)
    if prophet_fit_params:
        model.fit_params = prophet_fit_params
    
    return model


def train_and_forecast(
    training_data: pd.DataFrame,
    forecast_horizon_days: int,
    prophet_params: Optional[Dict[str, Any]] = None,
    prophet_fit_params: Optional[Dict[str, Any]] = None
) -> pd.DataFrame:
    """
    Train Prophet model and generate forecast.
    
    Args:
        training_data: DataFrame with columns ["ds", "y"] (business-day indexed)
        forecast_horizon_days: Number of business days to forecast ahead
        prophet_params: Prophet model parameters (optional)
        prophet_fit_params: Prophet fit parameters (optional)
        
    Returns:
        DataFrame with forecast results (columns: ds, yhat, yhat_lower, yhat_upper)
        
    Raises:
        ImportError: If darts is not installed
        ValueError: If training_data is insufficient
    """
    if not DARTS_AVAILABLE:
        raise ImportError("darts library is required but not installed")
    
    if training_data.empty or len(training_data) < 2:
        raise ValueError("Insufficient training data")
    
    # Convert to TimeSeries
    series = dataframe_to_timeseries(training_data, time_col="ds", value_col="y")
    
    # Create and train model
    model = create_prophet_model(prophet_params, prophet_fit_params)
    model.fit(series)
    
    # Generate forecast
    last_history_date = training_data["ds"].max().date()
    future_dates = future_business_dates(last_history_date, forecast_horizon_days)
    
    if not future_dates:
        return pd.DataFrame(columns=["ds", "yhat", "yhat_lower", "yhat_upper"])
    
    # Create future TimeSeries
    future_series = TimeSeries.from_times_and_values(
        pd.DatetimeIndex(future_dates),
        np.zeros(len(future_dates))  # Dummy values, will be overwritten
    )
    
    # Generate forecast
    forecast = model.predict(n=forecast_horizon_days)
    
    # Convert forecast back to DataFrame
    forecast_df = forecast.pd_dataframe().reset_index()
    forecast_df.columns = ["ds", "yhat"]
    
    # For Prophet via darts, uncertainty intervals may need to be extracted differently
    # This is a simplified version - adjust based on actual darts Prophet implementation
    # Some darts models may provide prediction_intervals
    try:
        # Try to get prediction intervals if available
        forecast_with_intervals = model.predict(
            n=forecast_horizon_days,
            num_samples=100  # For uncertainty estimation
        )
        if hasattr(forecast_with_intervals, "quantile_timeseries"):
            lower = forecast_with_intervals.quantile_timeseries(0.025)
            upper = forecast_with_intervals.quantile_timeseries(0.975)
            forecast_df["yhat_lower"] = lower.values().flatten()
            forecast_df["yhat_upper"] = upper.values().flatten()
        else:
            # Fallback: use yhat with some margin
            std = forecast_df["yhat"].std()
            forecast_df["yhat_lower"] = forecast_df["yhat"] - 1.96 * std
            forecast_df["yhat_upper"] = forecast_df["yhat"] + 1.96 * std
    except:
        # Fallback: estimate intervals from forecast variance
        std = forecast_df["yhat"].std() if len(forecast_df) > 1 else forecast_df["yhat"].iloc[0] * 0.1
        forecast_df["yhat_lower"] = forecast_df["yhat"] - 1.96 * std
        forecast_df["yhat_upper"] = forecast_df["yhat"] + 1.96 * std
    
    return forecast_df

