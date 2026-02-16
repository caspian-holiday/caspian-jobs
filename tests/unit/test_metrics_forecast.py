"""
Unit tests for metrics_forecast job helpers.
"""

from datetime import date, datetime, timezone
import pytest

from victoria_metrics_jobs.jobs.metrics_forecast.metrics_forecast import (
    MetricsForecastJob,
    MetricsForecastState,
)


@pytest.fixture
def job():
    return MetricsForecastJob()


def test_build_metric_selector_injects_source(job):
    selector = 'requests_total{env="dev"}'
    result = job._build_metric_selector(selector, "source", "forecast-job")
    assert 'source="forecast-job"' in result
    assert 'env="dev"' in result
    # Should include PromQL filter to exclude forecast labels
    assert 'forecast!~".+"' in result


def test_parse_range_query_excludes_forecast_labels(job):
    """Test that _parse_range_query filters out metrics with forecast label."""
    from datetime import datetime, timezone
    
    query_result = {
        "status": "success",
        "data": {
            "result": [
                {
                    "metric": {
                        "__name__": "requests_total",
                        "source": "apex_collector",
                        "env": "dev"
                    },
                    "values": [[1609459200, "100"]]
                },
                {
                    "metric": {
                        "__name__": "requests_total",
                        "source": "apex_collector",
                        "env": "dev",
                        "forecast": "trend"  # This should be excluded
                    },
                    "values": [[1609459200, "110"]]
                },
                {
                    "metric": {
                        "__name__": "requests_total",
                        "source": "apex_collector",
                        "env": "dev",
                        "forecast": "lower"  # This should also be excluded
                    },
                    "values": [[1609459200, "90"]]
                }
            ]
        }
    }
    
    histories = job._parse_range_query(query_result, "source", "apex_collector")
    # Should only return 1 series (the one without forecast label)
    assert len(histories) == 1
    assert "forecast" not in histories[0].labels


def test_prepare_training_frame_fills_business_days(job):
    samples = [
        (datetime(2024, 1, 1, tzinfo=timezone.utc), 10.0),
        (datetime(2024, 1, 3, tzinfo=timezone.utc), 30.0),
    ]
    df = job._prepare_training_frame(samples)
    assert len(df) == 3  # Jan 1, 2, 3 (business days)
    # Middle day should be interpolated between endpoints
    interpolated = df.iloc[1]["y"]
    assert interpolated == pytest.approx(20.0, rel=1e-3)


def test_future_business_dates_skip_weekends(job):
    future = job._future_business_dates(date(2024, 1, 5), periods=3)  # Friday
    assert [ts.date().weekday() for ts in future] == [0, 1, 2]  # Mon, Tue, Wed


def test_calculate_forecast_timestamp_increments(job):
    state = MetricsForecastState(
        job_id="test",
        job_config={},
        started_at=datetime.now(timezone.utc),
    )
    base_date = date(2024, 1, 1)
    midnight_ts = int(datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp())

    class DummyProm:
        def __init__(self, payload):
            self.payload = payload

        def custom_query_range(self, **kwargs):
            return self.payload

    # No existing values => midnight timestamp
    empty_prom = DummyProm({"status": "success", "data": {"result": []}})
    ts_first = job._calculate_forecast_timestamp(
        state, empty_prom, "metric", {"forecast": "trend"}, base_date
    )
    assert ts_first == midnight_ts

    # Existing values => increment by one second
    second_payload = {
        "status": "success",
        "data": {
            "result": [
                {
                    "values": [
                        [midnight_ts, "10"],
                        [midnight_ts + 5, "11"],
                    ]
                }
            ]
        },
    }
    prom_with_data = DummyProm(second_payload)
    ts_second = job._calculate_forecast_timestamp(
        state, prom_with_data, "metric", {"forecast": "trend"}, base_date
    )
    assert ts_second == midnight_ts + 6

