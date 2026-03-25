#!/usr/bin/env python3
from pathlib import Path
import argparse

from src.polybot.config import AnalyzerConfig
from src.polybot.pipeline import PolyAnalyzer


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Polymarket trader copyability analyzer")
    parser.add_argument("--base-url", default="https://data-api.polymarket.com", help="Data API base URL")
    parser.add_argument("--category", default="CRYPTO", help="Leaderboard category: CRYPTO / ...")
    parser.add_argument("--interval", default="MONTH", help="Leaderboard interval: DAY/WEEK/MONTH/ALL")
    parser.add_argument("--order-by", default="PNL", help="Leaderboard orderBy: PNL or VOL")
    parser.add_argument("--limit", type=int, default=50, help="Candidate traders to pull from leaderboard")
    parser.add_argument("--output", default="outputs/latest", help="Output directory")
    parser.add_argument("--top", type=int, default=20, help="Top N to print")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    config = AnalyzerConfig(
        base_url=args.base_url,
        max_traders=args.limit,
        leaderboard_category=args.category,
        leaderboard_interval=args.interval,
        leaderboard_order_by=args.order_by,
    )

    analyzer = PolyAnalyzer(config)
    df = analyzer.analyze(Path(args.output))

    display_cols = [
        "address",
        "name",
        "x_username",
        "copyability_score",
        "realized_pnl_90d",
        "avg_trades_per_day_30d",
        "avg_hold_hours_90d",
        "crypto_ratio",
        "btc_ratio",
        "max_drawdown_90d",
    ]
    cols = [c for c in display_cols if c in df.columns]
    print("\nTop traders by Copyability Score:")
    print(df.head(args.top)[cols].to_string(index=False))


if __name__ == "__main__":
    main()
