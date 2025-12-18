"""
Darts wrapper for ARIMA forecasting model.

This module provides helper functions to use ARIMA via the darts library
for time series forecasting in Jupyter notebooks.
"""

from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, date
import pandas as pd
import numpy as np

try:
    from darts import TimeSeries
    from darts.models import ARIMA
    DARTS_AVAILABLE = True
except ImportError:
    DARTS_AVAILABLE = False

# Reuse common functions from Prophet wrapper (same directory)
from darts_prophet_wrapper import (
    dataframe_to_timeseries,
    prepare_business_day_data,
    future_business_dates,
)


def create_arima_model(
    arima_params: Optional[Dict[str, Any]] = None
) -> "ARIMA":
    """
    Create an ARIMA model using darts wrapper.
    
    Args:
        arima_params: Dictionary of ARIMA parameters
            Common parameters:
            - p: Auto-regressive order (default: 1)
            - d: Differencing order (default: 1)
            - q: Moving average order (default: 1)
            - Other parameters as supported by darts ARIMA model
            
    Returns:
        ARIMA model instance
        
    Raises:
        ImportError: If darts is not installed
    """
    if not DARTS_AVAILABLE:
        raise ImportError("darts library is required but not installed")
    
    # Default ARIMA parameters (simple ARIMA(1,1,1))
    default_params = {
        "p": 1,
        "d": 1,
        "q": 1,
    }
    
    if arima_params:
        default_params.update(arima_params)
    
    # Extract p, d, q for ARIMA constructor
    p = default_params.pop("p", 1)
    d = default_params.pop("d", 1)
    q = default_params.pop("q", 1)
    
    # Create model
    model = ARIMA(p=p, d=d, q=q, **default_params)
    
    return model


def train_and_forecast(
    training_data: pd.DataFrame,
    forecast_horizon_days: int,
    arima_params: Optional[Dict[str, Any]] = None
) -> pd.DataFrame:
    """
    Train ARIMA model and generate forecast.
    
    Args:
        training_data: DataFrame with columns ["ds", "y"] (business-day indexed)
        forecast_horizon_days: Number of business days to forecast ahead
        arima_params: ARIMA model parameters (optional)
        
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
    model = create_arima_model(arima_params)
    model.fit(series)
    
    # Generate forecast
    last_history_date = training_data["ds"].max().date()
    future_dates = future_business_dates(last_history_date, forecast_horizon_days)
    
    if not future_dates:
        return pd.DataFrame(columns=["ds", "yhat", "yhat_lower", "yhat_upper"])
    
    # Generate forecast with prediction intervals
    try:
        forecast = model.predict(
            n=forecast_horizon_days,
            num_samples=100  # For uncertainty estimation
        )
        
        # Get prediction intervals if available
        if hasattr(forecast, "quantile_timeseries"):
            lower = forecast.quantile_timeseries(0.025)
            upper = forecast.quantile_timeseries(0.975)
            
            forecast_df = forecast.pd_dataframe().reset_index()
            forecast_df.columns = ["ds", "yhat"]
            forecast_df["yhat_lower"] = lower.values().flatten()
            forecast_df["yhat_upper"] = upper.values().flatten()
        else:
            # Fallback: just mean forecast
            forecast_df = forecast.pd_dataframe().reset_index()
            forecast_df.columns = ["ds", "yhat"]
            std = forecast_df["yhat"].std() if len(forecast_df) > 1 else abs(forecast_df["yhat"].iloc[0] * 0.1)
            forecast_df["yhat_lower"] = forecast_df["yhat"] - 1.96 * std
            forecast_df["yhat_upper"] = forecast_df["yhat"] + 1.96 * std
    except Exception as e:
        # Fallback: simple forecast
        forecast = model.predict(n=forecast_horizon_days)
        forecast_df = forecast.pd_dataframe().reset_index()
        forecast_df.columns = ["ds", "yhat"]
        std = forecast_df["yhat"].std() if len(forecast_df) > 1 else abs(forecast_df["yhat"].iloc[0] * 0.1)
        forecast_df["yhat_lower"] = forecast_df["yhat"] - 1.96 * std
        forecast_df["yhat_upper"] = forecast_df["yhat"] + 1.96 * std
    
    return forecast_df

