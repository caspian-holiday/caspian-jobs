"""
Unit tests for the biz_date input test data generator.
"""

import re
import io
import sys

import pytest

from tests.generators.biz_date_input_generator import (
    generate,
    _build_line,
    _parse_biz_dates,
    _biz_dates_last_n_days,
)


# Prometheus text: metric_name{label="value",...} value timestamp
_PROM_LINE = re.compile(
    r'^([a-zA-Z_][a-zA-Z0-9_]*)\{([^}]*)\}\s+([\d.eE+-]+)\s+(\d+)\s*$'
)


def _parse_prom_line(line: str) -> tuple[str, dict[str, str], float, int] | None:
    m = _PROM_LINE.match(line.strip())
    if not m:
        return None
    name, labels_str, value_str, ts_str = m.groups()
    labels = {}
    for part in labels_str.split(","):
        part = part.strip()
        if not part:
            continue
        kv = part.split("=", 1)
        if len(kv) != 2:
            return None
        k, v = kv[0].strip(), kv[1].strip().strip('"')
        labels[k] = v
    return (name, labels, float(value_str), int(ts_str))


@pytest.mark.unit
def test_parse_biz_dates():
    assert _parse_biz_dates("18/02/2025,17/02/2025") == ["18/02/2025", "17/02/2025"]
    assert _parse_biz_dates("01/01/2024") == ["01/01/2024"]


@pytest.mark.unit
def test_biz_dates_last_n_days():
    from datetime import datetime, timezone
    # Fixed end date for stability
    end = datetime(2025, 2, 18, 12, 0, 0, tzinfo=timezone.utc)
    days = _biz_dates_last_n_days(3, end)
    assert len(days) == 3
    assert "18/02/2025" in days
    assert "17/02/2025" in days
    assert "16/02/2025" in days


@pytest.mark.unit
def test_build_line():
    line = _build_line("revenue_total", {"job": "X", "biz_date": "18/02/2025"}, 42.0, 1739880000)
    assert "revenue_total" in line
    assert 'job="X"' in line
    assert 'biz_date="18/02/2025"' in line
    assert " 42.0 1739880000" in line
    parsed = _parse_prom_line(line)
    assert parsed is not None
    name, labels, value, ts = parsed
    assert name == "revenue_total"
    assert labels["job"] == "X"
    assert labels["biz_date"] == "18/02/2025"
    assert value == 42.0
    assert ts == 1739880000


@pytest.mark.unit
def test_generate_output_format():
    lines = generate(
        jobs=["test_job"],
        biz_dates=["18/02/2025", "17/02/2025"],
        metric_names=["revenue_total", "orders_count"],
        series_count=1,
        base_timestamp=1739880000,
    )
    assert len(lines) >= 1
    for line in lines:
        parsed = _parse_prom_line(line)
        assert parsed is not None, f"Invalid Prometheus line: {line!r}"
        name, labels, value, ts = parsed
        assert "job" in labels
        assert "biz_date" in labels
        assert re.match(r"\d{2}/\d{2}/\d{4}", labels["biz_date"]), (
            f"biz_date should be dd/mm/yyyy: {labels['biz_date']}"
        )
        assert isinstance(ts, int)
        assert ts == 1739880000


@pytest.mark.unit
def test_generate_expected_count():
    lines = generate(
        jobs=["j1"],
        biz_dates=["18/02/2025", "17/02/2025"],
        metric_names=["m1", "m2"],
        series_count=2,
        base_timestamp=1739880000,
    )
    # 1 job * 2 biz_dates * 2 metrics * 2 series = 8
    assert len(lines) == 8


@pytest.mark.unit
def test_generate_cli_stdout(capsys):
    # Run CLI with fixed args and capture stdout
    sys.argv = [
        "biz_date_input_generator.py",
        "-j", "cli_job",
        "--biz-dates", "18/02/2025",
        "--metrics", "only_metric",
        "--series-count", "1",
    ]
    from tests.generators.biz_date_input_generator import main
    rc = main()
    assert rc == 0
    out, err = capsys.readouterr()
    lines = [l for l in out.strip().split("\n") if l.strip()]
    assert len(lines) >= 1
    parsed = _parse_prom_line(lines[0])
    assert parsed is not None
    _, labels, _, _ = parsed
    assert labels.get("job") == "cli_job"
    assert labels.get("biz_date") == "18/02/2025"
