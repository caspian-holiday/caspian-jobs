#!/usr/bin/env python3
"""
Generate business-date (bd) side input metrics in Prometheus exposition format
for feeding VictoriaMetrics and running the business_date_converter job.

How to run:

  # Print metrics to stdout
  python3 tests/generators/biz_date_input_generator.py -j test_job --biz-dates 18/02/2025,17/02/2025 --metrics revenue_total

  # Add market_hour label variants (e.g. 1h,2h,3h)
  python3 tests/generators/biz_date_input_generator.py -j test_job --biz-dates 18/02/2025 --metrics revenue_total --market-hours 1h,2h,3h

  # Feed into VictoriaMetrics (single-node)
  python3 tests/generators/biz_date_input_generator.py -j test_job --biz-dates 18/02/2025 | \\
    curl -X POST -H \"Content-Type: text/plain\" --data-binary @- \"http://<vm>:8428/api/v1/import/prometheus\"
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path


def _parse_biz_dates(s: str) -> list[str]:
    """Parse comma-separated biz_dates in dd/mm/yyyy format; return as-is for validation."""
    return [d.strip() for d in s.split(",") if d.strip()]

def _parse_market_hours(s: str) -> list[str]:
    """Parse comma-separated market hours like 1h,2h,3h."""
    return [h.strip() for h in s.split(",") if h.strip()]


def _biz_dates_last_n_days(n: int, end_date: datetime | None = None) -> list[str]:
    """Return last n days as biz_date strings in dd/mm/yyyy."""
    end = end_date or datetime.now(timezone.utc)
    out = []
    for i in range(n):
        d = (end - timedelta(days=i)).date()
        out.append(d.strftime("%d/%m/%Y"))
    return out


def _build_line(metric_name: str, labels: dict[str, str], value: float, timestamp: int) -> str:
    """Build one Prometheus exposition format line. Timestamp in seconds."""
    pairs = [f'{k}="{v}"' for k, v in sorted(labels.items())]
    return f"{metric_name}{{{','.join(pairs)}}} {value} {timestamp}"


def generate(
    jobs: list[str],
    biz_dates: list[str],
    metric_names: list[str],
    series_count: int = 1,
    market_hours: list[str] | None = None,
    base_timestamp: int | None = None,
    spread_minutes: int | None = None,
    extra_labels: dict[str, str] | None = None,
) -> list[str]:
    """
    Generate Prometheus text lines for bd-side metrics.

    - jobs: e.g. ["test_job"]
    - biz_dates: list of "dd/mm/yyyy"
    - metric_names: e.g. ["revenue_total", "orders_count"]
    - series_count: number of label variants per (job, metric, biz_date) (e.g. env="test", region="eu")
    - market_hours: optional market hour label values (e.g. ["1h","2h"])
    - base_timestamp: Unix seconds; if None, use now.
    - spread_minutes: if set, spread samples over the last N minutes (one step per series).
    - extra_labels: optional extra labels to add to every series (e.g. {"env": "test"}).
    """
    now = int(datetime.now(timezone.utc).timestamp())
    base_ts = base_timestamp if base_timestamp is not None else now
    extra = dict(extra_labels or {})
    lines = []
    mh_count = len(market_hours) if market_hours else 1
    total_series = len(jobs) * len(biz_dates) * len(metric_names) * max(1, series_count) * mh_count
    step = (spread_minutes * 60) / max(1, total_series) if spread_minutes else 0

    idx = 0
    for job in jobs:
        for biz_date in biz_dates:
            for metric_name in metric_names:
                for s in range(max(1, series_count)):
                    for mh in (market_hours or [None]):
                        labels = {"job": job, "biz_date": biz_date, **extra}
                        if series_count > 1:
                            labels["series_id"] = str(s)
                        if mh is not None:
                            labels["market_hour"] = mh
                        ts = base_ts
                        if step > 0:
                            ts = base_ts - int(idx * step)
                        value = 100.0 + idx  # deterministic but distinct
                        line = _build_line(metric_name, labels, value, ts)
                        lines.append(line)
                        idx += 1
    return lines


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate business-date input metrics for the business_date_converter job."
    )
    parser.add_argument(
        "-j", "--job",
        default="test_biz_date_job",
        help="Job name (single). Default: test_biz_date_job",
    )
    parser.add_argument(
        "--biz-dates",
        type=str,
        help="Comma-separated biz_dates in dd/mm/yyyy, or leave unset and use --last-days",
    )
    parser.add_argument(
        "--last-days",
        type=int,
        metavar="N",
        help="Use last N days as biz_dates instead of --biz-dates",
    )
    parser.add_argument(
        "--metrics",
        type=str,
        default="revenue_total,orders_count",
        help="Comma-separated metric names. Default: revenue_total,orders_count",
    )
    parser.add_argument(
        "--series-count",
        type=int,
        default=1,
        metavar="N",
        help="Number of label variants per (job, metric, biz_date). Default: 1",
    )
    parser.add_argument(
        "--market-hours",
        type=str,
        help='Comma-separated market_hour label values like "1h,2h,3h". If set, emits one series per market_hour for each (job,biz_date,metric,series_id).',
    )
    parser.add_argument(
        "--timestamp",
        type=int,
        metavar="UNIX_SEC",
        help="Base timestamp in Unix seconds. Default: now",
    )
    parser.add_argument(
        "--spread-minutes",
        type=int,
        metavar="N",
        help="Spread samples over the last N minutes",
    )
    parser.add_argument(
        "-o", "--output",
        type=str,
        metavar="FILE",
        help="Write to file instead of stdout",
    )
    parser.add_argument(
        "--push-url",
        type=str,
        metavar="URL",
        help="POST generated body to this VM import URL (e.g. .../api/v1/import/prometheus)",
    )
    args = parser.parse_args()

    jobs_list = [args.job]
    if args.last_days is not None:
        biz_dates_list = _biz_dates_last_n_days(args.last_days)
    elif args.biz_dates:
        biz_dates_list = _parse_biz_dates(args.biz_dates)
    else:
        biz_dates_list = _biz_dates_last_n_days(3)  # default last 3 days

    metric_list = [m.strip() for m in args.metrics.split(",") if m.strip()]
    if not metric_list:
        metric_list = ["revenue_total"]

    lines = generate(
        jobs=jobs_list,
        biz_dates=biz_dates_list,
        metric_names=metric_list,
        series_count=args.series_count,
        market_hours=_parse_market_hours(args.market_hours) if args.market_hours else None,
        base_timestamp=args.timestamp,
        spread_minutes=args.spread_minutes,
    )
    body = "\n".join(lines) + "\n"

    if args.push_url:
        try:
            import urllib.request
            req = urllib.request.Request(
                args.push_url,
                data=body.encode("utf-8"),
                headers={"Content-Type": "text/plain"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                if resp.status not in (200, 204):
                    print(f"Push returned status {resp.status}", file=sys.stderr)
                    return 1
        except Exception as e:
            print(f"Push failed: {e}", file=sys.stderr)
            return 1
        if not args.output:
            print(f"Pushed {len(lines)} lines to {args.push_url}", file=sys.stderr)
            return 0

    if args.output:
        Path(args.output).write_text(body, encoding="utf-8")
        print(f"Wrote {len(lines)} lines to {args.output}", file=sys.stderr)
    else:
        sys.stdout.write(body)
    return 0


if __name__ == "__main__":
    sys.exit(main())
