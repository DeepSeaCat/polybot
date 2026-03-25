#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List

import pandas as pd

from src.polybot.report import build_html_report
from src.polybot.scoring import TraderScorer


def load_trades_by_address(raw_dir: Path) -> Dict[str, List[dict]]:
    out: Dict[str, List[dict]] = {}
    for raw_file in raw_dir.glob("*.json"):
        address = raw_file.stem
        with raw_file.open("r", encoding="utf-8") as f:
            obj = json.load(f)
        out[address] = obj.get("trades", [])
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Rebuild report from raw JSON using updated decision-based metrics")
    parser.add_argument("--input-dir", required=True, help="Directory containing trader_scores.csv and raw/")
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    csv_path = input_dir / "trader_scores.csv"
    raw_dir = input_dir / "raw"

    if not csv_path.exists() or not raw_dir.exists():
        raise RuntimeError(f"Missing files under {input_dir}")

    df = pd.read_csv(csv_path)
    if "address" not in df.columns:
        raise RuntimeError("trader_scores.csv missing address column")

    scorer = TraderScorer(weights={})
    trades_by_address = load_trades_by_address(raw_dir)

    for col in (
        "cycle5_window_count",
        "cycle5_first_minute_single_count",
        "cycle5_first_minute_single_ratio",
    ):
        if col not in df.columns:
            df[col] = 0.0

    for idx, row in df.iterrows():
        address = str(row["address"])
        metrics = scorer.compute_cycle5_timing_discipline(trades_by_address.get(address, []))
        for k, v in metrics.items():
            df.at[idx, k] = round(float(v), 4)

    df = df.sort_values(by=["copyability_score", "realized_pnl_90d"], ascending=False)

    df.to_csv(csv_path, index=False)
    df.to_json(input_dir / "trader_scores.json", orient="records", force_ascii=False, indent=2)
    build_html_report(df, input_dir / "report.html")

    print(f"Rebuilt report with decision metrics: {input_dir / 'report.html'}")


if __name__ == "__main__":
    main()
