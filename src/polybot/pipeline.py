from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from .client import PolyDataApiClient
from .config import AnalyzerConfig
from .report import build_html_report
from .scoring import TraderScorer


def _extract_address(item: Dict[str, Any]) -> str:
    for key in (
        "address",
        "proxyWallet",
        "proxy_wallet",
        "wallet",
        "user",
        "trader",
    ):
        value = item.get(key)
        if isinstance(value, str) and value:
            return value
    return ""


class PolyAnalyzer:
    def __init__(self, config: AnalyzerConfig):
        self.config = config
        self.client = PolyDataApiClient(config)
        self.scorer = TraderScorer(config.copyability_weights)
        self.market_open_cache: Dict[str, str] = {}

    def _get_market_open_time(self, slug: str) -> str:
        if not slug:
            return ""
        if slug in self.market_open_cache:
            return self.market_open_cache[slug]

        market = self.client.get_market_by_slug(slug)
        open_time = ""
        for key in ("startDate", "eventStartTime", "createdAt"):
            value = market.get(key)
            if isinstance(value, str) and value:
                open_time = value
                break

        self.market_open_cache[slug] = open_time
        return open_time

    def _build_open_time_map(self, trades: List[Dict[str, Any]]) -> Dict[str, str]:
        if self.config.max_market_lookups_per_trader <= 0:
            return {}

        slugs: List[str] = []
        seen = set()
        for trade in trades:
            if str(trade.get("side", "")).upper() != "BUY":
                continue
            slug = str(trade.get("slug", ""))
            if not slug or slug in seen:
                continue
            seen.add(slug)
            slugs.append(slug)
            if len(slugs) >= self.config.max_market_lookups_per_trader:
                break

        out: Dict[str, str] = {}
        for slug in slugs:
            out[slug] = self._get_market_open_time(slug)
        return out

    def fetch_candidates(self) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        offset = 0

        while len(out) < self.config.max_traders:
            page_size = min(50, self.config.max_traders - len(out))
            page = self.client.get_leaderboard(
                category=self.config.leaderboard_category,
                interval=self.config.leaderboard_interval,
                order_by=self.config.leaderboard_order_by,
                limit=page_size,
                offset=offset,
            )

            if not page:
                break

            out.extend(page)
            offset += len(page)

            if len(page) < page_size:
                break

        return out[: self.config.max_traders]

    def analyze(self, output_dir: Path) -> pd.DataFrame:
        output_dir.mkdir(parents=True, exist_ok=True)
        raw_dir = output_dir / "raw"
        raw_dir.mkdir(parents=True, exist_ok=True)

        leaderboard_rows = self.fetch_candidates()
        candidates = []
        seen = set()
        for row in leaderboard_rows:
            addr = _extract_address(row)
            if not addr or addr in seen:
                continue
            seen.add(addr)
            candidates.append((addr, row))

        if not candidates:
            raise RuntimeError("No candidate addresses found from leaderboard payload")

        records: List[Dict[str, Any]] = []

        for index, (address, lb_row) in enumerate(candidates, start=1):
            print(f"[{index}/{len(candidates)}] analyzing {address}")
            try:
                profile = self.client.get_public_profile(address)
                current_positions: List[Dict[str, Any]] = []
                closed_positions = self.client.get_closed_positions(address)
                trades = self.client.get_user_trades(address)
                activity: List[Dict[str, Any]] = []
                snapshot_url = None
                try:
                    snapshot_url = self.client.get_accounting_snapshot_url(address)
                except Exception:
                    snapshot_url = None

                score = self.scorer.score_trader(
                    public_profile=profile,
                    trades=trades,
                    current_positions=current_positions,
                    closed_positions=closed_positions,
                    activity=activity,
                )

                market_open_time_map = self._build_open_time_map(trades)
                open_timing = self.scorer.compute_open_timing_metrics(trades, market_open_time_map)
                cycle5_timing = self.scorer.compute_cycle5_timing_discipline(trades)

                high_win_near_open = (
                    score.get("win_rate_90d", 0) >= 0.70
                    and open_timing.get("near_open_buy_ratio_10m", 0) >= 0.60
                    and open_timing.get("buy_count_with_open_time", 0) >= 30
                )

                record = {
                    "address": address,
                    "leaderboard_pnl": lb_row.get("pnl", lb_row.get("PnL", 0)),
                    "leaderboard_volume": lb_row.get("volume", lb_row.get("vol", 0)),
                    "leaderboard_username": lb_row.get("username", ""),
                    "snapshot_url": snapshot_url or "",
                    **score,
                    **{k: round(v, 4) for k, v in open_timing.items()},
                    **{k: round(v, 4) for k, v in cycle5_timing.items()},
                    "high_win_near_open": high_win_near_open,
                }
                records.append(record)

                raw_payload = {
                    "leaderboard": lb_row,
                    "profile": profile,
                    "current_positions": current_positions,
                    "closed_positions": closed_positions,
                    "trades": trades,
                    "activity": activity,
                    "score": score,
                    "open_timing": open_timing,
                    "cycle5_timing": cycle5_timing,
                    "config": asdict(self.config),
                }
                with (raw_dir / f"{address}.json").open("w", encoding="utf-8") as f:
                    json.dump(raw_payload, f, ensure_ascii=False, indent=2)
            except Exception as exc:
                print(f"skip {address}: {exc}")

        if not records:
            raise RuntimeError("No trader analysis generated. Check API endpoint paths/config.")

        df = pd.DataFrame(records)
        df = df.sort_values(by=["copyability_score", "realized_pnl_90d"], ascending=False)

        summary_csv = output_dir / "trader_scores.csv"
        summary_json = output_dir / "trader_scores.json"
        df.to_csv(summary_csv, index=False)
        df.to_json(summary_json, orient="records", force_ascii=False, indent=2)

        report_html = output_dir / "report.html"
        build_html_report(df, report_html)

        print(f"Saved summary CSV: {summary_csv}")
        print(f"Saved summary JSON: {summary_json}")
        print(f"Saved HTML report: {report_html}")
        return df
