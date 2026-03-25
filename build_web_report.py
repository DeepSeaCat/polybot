#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from src.polybot.report import build_html_report


def _to_ts(v: Any) -> float:
    if v is None:
        return -1.0
    if isinstance(v, (int, float)):
        return float(v) / 1000.0 if float(v) > 1e12 else float(v)
    if isinstance(v, str):
        t = v.strip()
        if not t:
            return -1.0
        if t.endswith("Z"):
            t = t[:-1] + "+00:00"
        try:
            from datetime import datetime

            return datetime.fromisoformat(t).timestamp()
        except ValueError:
            return -1.0
    return -1.0


def compute_near_open_metrics(
    trades: List[Dict[str, Any]],
    market_open_proxy: Dict[str, float],
) -> Dict[str, float]:
    delays = []
    for t in trades:
        if str(t.get("side", "")).upper() != "BUY":
            continue
        slug = str(t.get("slug", ""))
        open_ts = market_open_proxy.get(slug, -1.0)
        trade_ts = _to_ts(t.get("timestamp"))
        if open_ts < 0 or trade_ts < 0 or trade_ts < open_ts:
            continue
        delays.append((trade_ts - open_ts) / 60.0)

    if not delays:
        return {
            "buy_count_with_open_time": 0.0,
            "near_open_buy_ratio_10m": 0.0,
            "near_open_buy_ratio_30m": 0.0,
            "avg_open_delay_min": 0.0,
            "median_open_delay_min": 0.0,
        }

    delays_sorted = sorted(delays)
    n = len(delays_sorted)
    near10 = sum(1 for x in delays_sorted if x <= 10) / n
    near30 = sum(1 for x in delays_sorted if x <= 30) / n
    avg_delay = sum(delays_sorted) / n
    mid = n // 2
    median = (delays_sorted[mid - 1] + delays_sorted[mid]) / 2.0 if n % 2 == 0 else delays_sorted[mid]

    return {
        "buy_count_with_open_time": float(n),
        "near_open_buy_ratio_10m": near10,
        "near_open_buy_ratio_30m": near30,
        "avg_open_delay_min": avg_delay,
        "median_open_delay_min": median,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Build interactive web report from existing output data")
    parser.add_argument("--input-dir", default="outputs/month_crypto_top50_v2", help="Directory with trader_scores.csv and raw/")
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    csv_path = input_dir / "trader_scores.csv"
    raw_dir = input_dir / "raw"

    if not csv_path.exists() or not raw_dir.exists():
        raise RuntimeError(f"Missing expected files under {input_dir}")

    df = pd.read_csv(csv_path)
    if "address" not in df.columns:
        raise RuntimeError("trader_scores.csv missing address column")

    market_open_proxy: Dict[str, float] = {}
    metric_by_address: Dict[str, Dict[str, float]] = {}

    # Build market open proxy from the earliest observed trade timestamp by slug.
    for raw_file in raw_dir.glob("*.json"):
        with raw_file.open("r", encoding="utf-8") as f:
            obj = json.load(f)
        for t in obj.get("trades", []):
            slug = str(t.get("slug", ""))
            ts = _to_ts(t.get("timestamp"))
            if not slug or ts < 0:
                continue
            if slug not in market_open_proxy or ts < market_open_proxy[slug]:
                market_open_proxy[slug] = ts

    for raw_file in raw_dir.glob("*.json"):
        address = raw_file.stem
        with raw_file.open("r", encoding="utf-8") as f:
            obj = json.load(f)
        trades = obj.get("trades", [])
        metric_by_address[address] = compute_near_open_metrics(
            trades=trades,
            market_open_proxy=market_open_proxy,
        )

    for col in (
        "buy_count_with_open_time",
        "near_open_buy_ratio_10m",
        "near_open_buy_ratio_30m",
        "avg_open_delay_min",
        "median_open_delay_min",
    ):
        df[col] = 0.0

    for idx, row in df.iterrows():
        m = metric_by_address.get(str(row["address"]), {})
        for k, v in m.items():
            df.at[idx, k] = v

    df["high_win_near_open"] = (
        (df.get("win_rate_90d", 0) >= 0.70)
        & (df.get("near_open_buy_ratio_10m", 0) >= 0.60)
        & (df.get("buy_count_with_open_time", 0) >= 10)
    )

    df = df.sort_values(by=["win_rate_90d", "near_open_buy_ratio_10m", "copyability_score"], ascending=False)

    enriched_csv = input_dir / "trader_scores_web.csv"
    report_html = input_dir / "report.html"
    df.to_csv(enriched_csv, index=False)
    build_html_report(df, report_html)

    print(f"Saved enriched CSV: {enriched_csv}")
    print(f"Saved HTML report: {report_html}")


if __name__ == "__main__":
    main()
