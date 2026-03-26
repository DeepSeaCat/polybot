#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, Iterable, Optional

import pandas as pd


CYCLE_RE = re.compile(r"^(?P<symbol>[a-z0-9]+)-.*-(?P<cycle>5m|15m|30m|1h|2h)-(?P<epoch>\d{10})$")
DURATION_SEC = {
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "2h": 7200,
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Find low-friction Polymarket traders from an existing analysis output directory."
    )
    parser.add_argument(
        "--input",
        default="outputs/week_crypto_top1000_v2",
        help="Directory containing trader_scores.csv and raw/*.json",
    )
    parser.add_argument(
        "--min-closed-positions",
        type=int,
        default=10,
        help="Minimum number of closed positions for a trader to be considered",
    )
    parser.add_argument(
        "--min-win-rate",
        type=float,
        default=0.8,
        help="Minimum 90d win rate",
    )
    parser.add_argument(
        "--low-freq-cap",
        type=float,
        default=5.0,
        help="Max avg trades per day for the low-frequency open-style shortlist",
    )
    return parser


def parse_cycle_slug(slug: str) -> Optional[dict]:
    match = CYCLE_RE.match((slug or "").strip().lower())
    if not match:
        return None
    cycle = match.group("cycle")
    return {
        "symbol": match.group("symbol"),
        "cycle": cycle,
        "epoch": int(match.group("epoch")),
        "duration_sec": DURATION_SEC[cycle],
    }


def quantile(values: list[int], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, int((len(ordered) - 1) * q))
    return float(ordered[index])


def _decision_key(trade: dict, slug: str, side: str, ts: int) -> str:
    tx_hash = str(trade.get("transactionHash") or "").strip().lower()
    if tx_hash:
        return tx_hash
    return f"{side}:{slug}:{ts // 10}"


def analyze_raw_file(raw_file: Path) -> Dict[str, object]:
    with raw_file.open() as handle:
        payload = json.load(handle)

    buy_delays: list[int] = []
    abs_buy_delays: list[int] = []
    normalized_delays: list[float] = []
    pre_open_buy_count = 0
    cycle_first_by_decision: dict[str, dict[str, int]] = defaultdict(dict)
    symbol_counter: Counter[str] = Counter()
    cycle_counter: Counter[str] = Counter()

    for trade in payload.get("trades", []):
        side = str(trade.get("side") or "").upper()
        if side != "BUY":
            continue

        slug = str(trade.get("slug") or "")
        parsed = parse_cycle_slug(slug)
        if not parsed:
            continue

        try:
            ts = int(float(trade.get("timestamp", 0)))
        except (TypeError, ValueError):
            continue

        decision_key = _decision_key(trade, slug, side, ts)
        existing = cycle_first_by_decision[slug].get(decision_key)
        cycle_first_by_decision[slug][decision_key] = ts if existing is None else min(existing, ts)

        symbol_counter[parsed["symbol"]] += 1
        cycle_counter[parsed["cycle"]] += 1

        delay_sec = ts - parsed["epoch"]
        if delay_sec < 0:
            pre_open_buy_count += 1

        buy_delays.append(delay_sec)
        abs_buy_delays.append(abs(delay_sec))
        normalized_delays.append(max(delay_sec, 0) / parsed["duration_sec"])

    epoch_buy_count = len(buy_delays)
    epoch_cycle_count = len(cycle_first_by_decision)
    if not epoch_buy_count:
        return {
            "address": raw_file.stem.lower(),
            "epoch_buy_count": 0,
            "epoch_cycle_count": float(epoch_cycle_count),
            "pre_open_buy_count": float(pre_open_buy_count),
            "near_open_15s_ratio": 0.0,
            "near_open_30s_ratio": 0.0,
            "near_open_60s_ratio": 0.0,
            "near_anchor_15s_ratio": 0.0,
            "near_anchor_30s_ratio": 0.0,
            "near_anchor_60s_ratio": 0.0,
            "late_cycle_entry_ratio": 0.0,
            "avg_delay_sec": 0.0,
            "median_delay_sec": 0.0,
            "median_abs_delay_sec": 0.0,
            "delay_q10_sec": 0.0,
            "delay_q25_sec": 0.0,
            "delay_q75_sec": 0.0,
            "delay_q90_sec": 0.0,
            "median_delay_norm": 0.0,
            "single_buy_cycle_ratio": 0.0,
            "single_buy_open15s_ratio": 0.0,
            "single_buy_open30s_ratio": 0.0,
            "single_buy_open60s_ratio": 0.0,
            "single_buy_anchor15s_ratio": 0.0,
            "single_buy_anchor30s_ratio": 0.0,
            "single_buy_anchor60s_ratio": 0.0,
            "primary_symbol": "",
            "primary_cycle": "",
        }

    single_buy_cycles = 0
    single_buy_open15 = 0
    single_buy_open30 = 0
    single_buy_open60 = 0
    single_buy_anchor15 = 0
    single_buy_anchor30 = 0
    single_buy_anchor60 = 0
    late_first_entry_cycles = 0

    for slug, by_decision in cycle_first_by_decision.items():
        if not by_decision:
            continue
        times = sorted(by_decision.values())
        parsed = parse_cycle_slug(slug)
        if not parsed:
            continue
        first_delay = times[0] - parsed["epoch"]
        if len(times) == 1:
            single_buy_cycles += 1
        if first_delay <= 15 and len(times) == 1:
            single_buy_open15 += 1
        if first_delay <= 30 and len(times) == 1:
            single_buy_open30 += 1
        if first_delay <= 60 and len(times) == 1:
            single_buy_open60 += 1
        if abs(first_delay) <= 15 and len(times) == 1:
            single_buy_anchor15 += 1
        if abs(first_delay) <= 30 and len(times) == 1:
            single_buy_anchor30 += 1
        if abs(first_delay) <= 60 and len(times) == 1:
            single_buy_anchor60 += 1
        if first_delay >= parsed["duration_sec"] * 0.8:
            late_first_entry_cycles += 1

    primary_symbol = symbol_counter.most_common(1)[0][0] if symbol_counter else ""
    primary_cycle = cycle_counter.most_common(1)[0][0] if cycle_counter else ""

    return {
        "address": raw_file.stem.lower(),
        "epoch_buy_count": float(epoch_buy_count),
        "epoch_cycle_count": float(epoch_cycle_count),
        "pre_open_buy_count": float(pre_open_buy_count),
        "near_open_15s_ratio": sum(1 for x in buy_delays if x <= 15) / epoch_buy_count,
        "near_open_30s_ratio": sum(1 for x in buy_delays if x <= 30) / epoch_buy_count,
        "near_open_60s_ratio": sum(1 for x in buy_delays if x <= 60) / epoch_buy_count,
        "near_anchor_15s_ratio": sum(1 for x in abs_buy_delays if x <= 15) / epoch_buy_count,
        "near_anchor_30s_ratio": sum(1 for x in abs_buy_delays if x <= 30) / epoch_buy_count,
        "near_anchor_60s_ratio": sum(1 for x in abs_buy_delays if x <= 60) / epoch_buy_count,
        "late_cycle_entry_ratio": sum(1 for x in normalized_delays if x >= 0.8) / epoch_buy_count,
        "avg_delay_sec": sum(buy_delays) / epoch_buy_count,
        "median_delay_sec": quantile(buy_delays, 0.5),
        "median_abs_delay_sec": quantile(abs_buy_delays, 0.5),
        "delay_q10_sec": quantile(buy_delays, 0.1),
        "delay_q25_sec": quantile(buy_delays, 0.25),
        "delay_q75_sec": quantile(buy_delays, 0.75),
        "delay_q90_sec": quantile(buy_delays, 0.9),
        "median_delay_norm": float(pd.Series(normalized_delays).median()),
        "single_buy_cycle_ratio": single_buy_cycles / epoch_cycle_count if epoch_cycle_count else 0.0,
        "single_buy_open15s_ratio": single_buy_open15 / epoch_cycle_count if epoch_cycle_count else 0.0,
        "single_buy_open30s_ratio": single_buy_open30 / epoch_cycle_count if epoch_cycle_count else 0.0,
        "single_buy_open60s_ratio": single_buy_open60 / epoch_cycle_count if epoch_cycle_count else 0.0,
        "single_buy_anchor15s_ratio": single_buy_anchor15 / epoch_cycle_count if epoch_cycle_count else 0.0,
        "single_buy_anchor30s_ratio": single_buy_anchor30 / epoch_cycle_count if epoch_cycle_count else 0.0,
        "single_buy_anchor60s_ratio": single_buy_anchor60 / epoch_cycle_count if epoch_cycle_count else 0.0,
        "primary_symbol": primary_symbol,
        "primary_cycle": primary_cycle,
    }


def load_timing_metrics(raw_dir: Path) -> pd.DataFrame:
    rows = [analyze_raw_file(path) for path in sorted(raw_dir.glob("*.json"))]
    return pd.DataFrame(rows)


def fill_numeric(df: pd.DataFrame, columns: Iterable[str]) -> None:
    for col in columns:
        if col in df.columns:
            df[col] = df[col].fillna(0.0)


def build_candidate_flags(df: pd.DataFrame, min_closed_positions: int, min_win_rate: float, low_freq_cap: float) -> pd.DataFrame:
    enough_history = (df["closed_positions_count"] >= min_closed_positions) & (df["epoch_buy_count"] >= 10)
    strong_open = (
        enough_history
        & (df["win_rate_90d"] >= min_win_rate)
        & (df["near_anchor_60s_ratio"] >= 0.5)
        & (df["single_buy_anchor60s_ratio"] >= 0.4)
    )

    low_freq_late = (
        (df["avg_trades_per_day_30d"] <= 1.0)
        & (df["win_rate_90d"] >= min_win_rate)
        & (df["closed_positions_count"] >= min_closed_positions)
        & (df["cycle5_first_minute_single_ratio"] >= 0.5)
        & (df["near_anchor_60s_ratio"] < 0.1)
    )

    df = df.copy()
    df["is_true_open_candidate"] = strong_open
    df["is_low_freq_true_open_candidate"] = strong_open & (df["avg_trades_per_day_30d"] <= low_freq_cap)
    df["is_very_low_freq_true_open_candidate"] = strong_open & (df["avg_trades_per_day_30d"] <= 3.0)
    df["is_low_freq_late_entry_candidate"] = low_freq_late
    return df


def export_views(df: pd.DataFrame, output_dir: Path) -> dict[str, Path]:
    paths = {
        "full": output_dir / "followable_traders_full.csv",
        "true_open": output_dir / "followable_traders_true_open.csv",
        "low_freq_true_open": output_dir / "followable_traders_low_freq_true_open.csv",
        "low_freq_late": output_dir / "followable_traders_low_freq_late_entry.csv",
    }

    df.sort_values(["copyability_score", "realized_pnl_90d"], ascending=[False, False]).to_csv(paths["full"], index=False)

    df[df["is_true_open_candidate"]].sort_values(
        ["copyability_score", "realized_pnl_90d"], ascending=[False, False]
    ).to_csv(paths["true_open"], index=False)

    df[df["is_low_freq_true_open_candidate"]].sort_values(
        ["copyability_score", "realized_pnl_90d"], ascending=[False, False]
    ).to_csv(paths["low_freq_true_open"], index=False)

    df[df["is_low_freq_late_entry_candidate"]].sort_values(
        ["copyability_score", "realized_pnl_90d"], ascending=[False, False]
    ).to_csv(paths["low_freq_late"], index=False)

    return paths


def print_top(df: pd.DataFrame, title: str, mask: pd.Series, limit: int = 10) -> None:
    cols = [
        "address",
        "name",
        "copyability_score",
        "win_rate_90d",
        "realized_pnl_90d",
        "avg_trades_per_day_30d",
        "epoch_buy_count",
        "primary_symbol",
        "primary_cycle",
        "near_open_15s_ratio",
        "near_anchor_60s_ratio",
        "single_buy_anchor60s_ratio",
        "median_abs_delay_sec",
    ]
    subset = df[mask].sort_values(["copyability_score", "realized_pnl_90d"], ascending=[False, False])
    print(f"\n{title}: {len(subset)}")
    if subset.empty:
        return
    print(subset[cols].head(limit).to_string(index=False))


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    input_dir = Path(args.input)
    scores_path = input_dir / "trader_scores.csv"
    raw_dir = input_dir / "raw"
    if not scores_path.exists():
        raise SystemExit(f"Missing file: {scores_path}")
    if not raw_dir.exists():
        raise SystemExit(f"Missing directory: {raw_dir}")

    scores = pd.read_csv(scores_path)
    scores["address"] = scores["address"].str.lower()

    timing = load_timing_metrics(raw_dir)
    merged = scores.merge(timing, on="address", how="left")
    fill_numeric(
        merged,
        [
            "epoch_buy_count",
            "epoch_cycle_count",
            "pre_open_buy_count",
            "near_open_15s_ratio",
            "near_open_30s_ratio",
            "near_open_60s_ratio",
            "near_anchor_15s_ratio",
            "near_anchor_30s_ratio",
            "near_anchor_60s_ratio",
            "late_cycle_entry_ratio",
            "avg_delay_sec",
            "median_delay_sec",
            "median_abs_delay_sec",
            "delay_q10_sec",
            "delay_q25_sec",
            "delay_q75_sec",
            "delay_q90_sec",
            "median_delay_norm",
            "single_buy_cycle_ratio",
            "single_buy_open15s_ratio",
            "single_buy_open30s_ratio",
            "single_buy_open60s_ratio",
            "single_buy_anchor15s_ratio",
            "single_buy_anchor30s_ratio",
            "single_buy_anchor60s_ratio",
        ],
    )
    merged["primary_symbol"] = merged["primary_symbol"].fillna("")
    merged["primary_cycle"] = merged["primary_cycle"].fillna("")

    merged = build_candidate_flags(
        merged,
        min_closed_positions=args.min_closed_positions,
        min_win_rate=args.min_win_rate,
        low_freq_cap=args.low_freq_cap,
    )

    exported = export_views(merged, input_dir)

    print(f"Analyzed {len(merged)} traders from {input_dir}")
    print(f"Built-in open timing usable traders: {(merged['buy_count_with_open_time'] > 0).sum()}")
    print(f"Cycle-boundary / open-style candidates: {int(merged['is_true_open_candidate'].sum())}")
    print(f"Low-frequency cycle-boundary candidates (<= {args.low_freq_cap:g}/day): {int(merged['is_low_freq_true_open_candidate'].sum())}")
    print(f"Very low-frequency cycle-boundary candidates (<= 3/day): {int(merged['is_very_low_freq_true_open_candidate'].sum())}")
    print(f"Low-frequency late-entry candidates (<= 1/day): {int(merged['is_low_freq_late_entry_candidate'].sum())}")

    print_top(merged, "Top cycle-boundary / open-style candidates", merged["is_true_open_candidate"])
    print_top(merged, f"Top low-frequency cycle-boundary candidates (<= {args.low_freq_cap:g}/day)", merged["is_low_freq_true_open_candidate"])
    print_top(merged, "Top low-frequency late-entry candidates", merged["is_low_freq_late_entry_candidate"])

    print("\nExports:")
    for path in exported.values():
        print(path)


if __name__ == "__main__":
    main()
