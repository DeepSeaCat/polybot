from __future__ import annotations

import math
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _extract_first(item: Dict[str, Any], keys: Iterable[str], default: Any = None) -> Any:
    for key in keys:
        if key in item and item[key] is not None:
            return item[key]
    return default


def _to_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        if value > 10**12:
            return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc)
        return datetime.fromtimestamp(value, tz=timezone.utc)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            return None
    return None


def _clip01(x: float) -> float:
    return max(0.0, min(1.0, x))


def _sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))


@dataclass
class ScoreBreakdown:
    profitability: float
    stability: float
    copyability: float
    focus: float
    execution: float


class TraderScorer:
    def __init__(self, weights: Dict[str, float]):
        self.weights = weights

    def _get_trade_ts(self, trade: Dict[str, Any]) -> Optional[datetime]:
        return _to_datetime(
            _extract_first(
                trade,
                (
                    "timestamp",
                    "timeStamp",
                    "createdAt",
                    "created_at",
                    "matchedAt",
                    "executedAt",
                    "date",
                ),
            )
        )

    def _get_position_open_close(
        self, position: Dict[str, Any]
    ) -> tuple[Optional[datetime], Optional[datetime]]:
        open_ts = _to_datetime(
            _extract_first(
                position,
                (
                    "openTime",
                    "openedAt",
                    "openTimestamp",
                    "startTime",
                    "firstBuyTimestamp",
                    "entryTimestamp",
                ),
            )
        )
        close_ts = _to_datetime(
            _extract_first(
                position,
                (
                    "closeTime",
                    "closedAt",
                    "closeTimestamp",
                    "endTime",
                    "timestamp",
                ),
            )
        )
        return open_ts, close_ts

    def _position_realized_pnl(self, position: Dict[str, Any]) -> float:
        return _safe_float(
            _extract_first(
                position,
                (
                    "realizedPnl",
                    "realized_pnl",
                    "pnl",
                    "profit",
                    "netPnl",
                ),
            )
        )

    def _trade_notional(self, trade: Dict[str, Any]) -> float:
        price = _safe_float(_extract_first(trade, ("price", "avgPrice", "executionPrice")))
        size = _safe_float(_extract_first(trade, ("size", "quantity", "amount", "shares")), 1.0)
        usdc = _safe_float(_extract_first(trade, ("notional", "usdcValue", "sizeUsd", "value")), -1)
        if usdc >= 0:
            return usdc
        return abs(price * size)

    def _filter_window(self, items: List[Dict[str, Any]], days: int, ts_fn) -> List[Dict[str, Any]]:
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=days)
        out: List[Dict[str, Any]] = []
        for item in items:
            ts = ts_fn(item)
            if ts and start <= ts <= end:
                out.append(item)
        return out

    def _max_drawdown_from_pnl(self, pnl_points: List[float]) -> float:
        peak = 0.0
        equity = 0.0
        max_dd = 0.0
        for p in pnl_points:
            equity += p
            peak = max(peak, equity)
            dd = peak - equity
            max_dd = max(max_dd, dd)
        return max_dd

    def _max_losing_streak(self, pnls: List[float]) -> int:
        streak = 0
        best = 0
        for p in pnls:
            if p < 0:
                streak += 1
                best = max(best, streak)
            else:
                streak = 0
        return best

    def _weekly_pnl_volatility(self, positions: List[Dict[str, Any]]) -> float:
        weekly: Dict[tuple[int, int], float] = {}
        for p in positions:
            close_ts = self._get_position_open_close(p)[1]
            if close_ts is None:
                continue
            year, week, _ = close_ts.isocalendar()
            key = (year, week)
            weekly[key] = weekly.get(key, 0.0) + self._position_realized_pnl(p)

        values = list(weekly.values())
        if len(values) <= 1:
            return 0.0
        mean = sum(values) / len(values)
        variance = sum((v - mean) ** 2 for v in values) / (len(values) - 1)
        return math.sqrt(max(variance, 0.0))

    def _focus_metrics(self, trades: List[Dict[str, Any]]) -> Dict[str, float]:
        if not trades:
            return {"crypto_ratio": 0.0, "btc_ratio": 0.0, "concentration": 0.0}

        market_names = []
        crypto_hits = 0
        btc_hits = 0
        for t in trades:
            title = str(_extract_first(t, ("marketTitle", "title", "question", "market"), "")).lower()
            slug = str(_extract_first(t, ("slug", "marketSlug", "market_id"), "")).lower()
            cat = str(_extract_first(t, ("category", "marketCategory"), "")).lower()
            text = f"{title} {slug} {cat}"
            if any(k in text for k in ("crypto", "btc", "bitcoin", "eth", "ethereum", "sol", "doge")):
                crypto_hits += 1
            if "btc" in text or "bitcoin" in text:
                btc_hits += 1
            market_key = slug or title or "unknown"
            market_names.append(market_key)

        total = len(trades)
        counts = Counter(market_names)
        shares = [c / total for c in counts.values()]
        hhi = sum(s * s for s in shares)

        return {
            "crypto_ratio": crypto_hits / total,
            "btc_ratio": btc_hits / total,
            "concentration": hhi,
        }

    def _copyability_metrics(self, trades: List[Dict[str, Any]], closed_positions: List[Dict[str, Any]]) -> Dict[str, float]:
        recent_30d_trades = self._filter_window(trades, 30, self._get_trade_ts)
        avg_trades_per_day = len(recent_30d_trades) / 30.0

        hold_hours: List[float] = []
        near_settlement_count = 0
        settlement_total = 0

        for p in closed_positions:
            open_ts, close_ts = self._get_position_open_close(p)
            if open_ts and close_ts and close_ts > open_ts:
                hold_hours.append((close_ts - open_ts).total_seconds() / 3600.0)

            entry_ts = open_ts
            settle_ts = _to_datetime(_extract_first(p, ("settleTime", "settledAt", "marketEnd", "endDate")))
            if entry_ts and settle_ts and settle_ts > entry_ts:
                settlement_total += 1
                remaining = (settle_ts - entry_ts).total_seconds() / 60.0
                if remaining <= 60:
                    near_settlement_count += 1

        avg_hold_hours = sum(hold_hours) / len(hold_hours) if hold_hours else 0.0
        near_settlement_ratio = near_settlement_count / settlement_total if settlement_total else 0.0

        return {
            "avg_trades_per_day_30d": avg_trades_per_day,
            "avg_hold_hours": avg_hold_hours,
            "near_settlement_ratio": near_settlement_ratio,
        }

    def _profitability_metrics(self, closed_positions: List[Dict[str, Any]]) -> Dict[str, float]:
        pnls = [self._position_realized_pnl(p) for p in closed_positions]
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p < 0]

        total_realized = sum(pnls)
        avg_trade = total_realized / len(pnls) if pnls else 0.0
        avg_win = sum(wins) / len(wins) if wins else 0.0
        avg_loss = abs(sum(losses) / len(losses)) if losses else 0.0
        profit_factor = (sum(wins) / abs(sum(losses))) if losses and sum(losses) != 0 else (2.0 if wins else 0.0)
        win_rate = len(wins) / len(pnls) if pnls else 0.0

        return {
            "realized_pnl": total_realized,
            "avg_trade_pnl": avg_trade,
            "profit_factor": profit_factor,
            "win_rate": win_rate,
            "avg_win": avg_win,
            "avg_loss": avg_loss,
            "pnls": pnls,
        }

    def _stability_metrics(self, closed_positions: List[Dict[str, Any]]) -> Dict[str, float]:
        sorted_positions = sorted(
            closed_positions,
            key=lambda p: self._get_position_open_close(p)[1] or datetime.min.replace(tzinfo=timezone.utc),
        )
        pnls = [self._position_realized_pnl(p) for p in sorted_positions]
        max_dd = self._max_drawdown_from_pnl(pnls)
        max_losing_streak = self._max_losing_streak(pnls)
        weekly_vol = self._weekly_pnl_volatility(sorted_positions)
        return {
            "max_drawdown": max_dd,
            "max_losing_streak": max_losing_streak,
            "weekly_pnl_volatility": weekly_vol,
        }

    def _execution_metrics(self, trades: List[Dict[str, Any]]) -> Dict[str, float]:
        notionals = [self._trade_notional(t) for t in trades if self._trade_notional(t) > 0]
        if not notionals:
            return {"avg_notional": 0.0, "median_notional": 0.0}

        notionals_sorted = sorted(notionals)
        mid = len(notionals_sorted) // 2
        if len(notionals_sorted) % 2 == 0:
            median = (notionals_sorted[mid - 1] + notionals_sorted[mid]) / 2.0
        else:
            median = notionals_sorted[mid]
        avg = sum(notionals_sorted) / len(notionals_sorted)
        return {"avg_notional": avg, "median_notional": median}

    def compute_cycle5_timing_discipline(self, trades: List[Dict[str, Any]]) -> Dict[str, float]:
        # Aggregate fills into decision events first, then evaluate 5-minute timing discipline.
        decisions: Dict[str, tuple[int, int]] = {}

        for trade in trades:
            ts = self._get_trade_ts(trade)
            if ts is None:
                continue

            epoch = int(ts.timestamp())
            tx_hash = str(_extract_first(trade, ("transactionHash", "txHash", "hash"), "")).lower().strip()
            slug = str(_extract_first(trade, ("slug", "eventSlug", "marketSlug"), "")).strip()
            side = str(_extract_first(trade, ("side",), "")).upper().strip() or "NA"

            if tx_hash:
                decision_key = f"tx:{tx_hash}"
            else:
                # Fallback: same side+market in a short 10s bucket is treated as one decision.
                short_bucket = epoch // 10
                decision_key = f"fb:{slug}:{side}:{short_bucket}"

            if decision_key not in decisions or epoch < decisions[decision_key][0]:
                decisions[decision_key] = (epoch, epoch // 300)

        window_stats: Dict[int, Dict[str, int]] = {}
        for epoch, window_id in decisions.values():
            sec_in_window = epoch % 300
            stat = window_stats.setdefault(window_id, {"count": 0, "first_min_count": 0})
            stat["count"] += 1
            if sec_in_window < 60:
                stat["first_min_count"] += 1

        if not window_stats:
            return {
                "cycle5_window_count": 0.0,
                "cycle5_first_minute_single_count": 0.0,
                "cycle5_first_minute_single_ratio": 0.0,
            }

        total_windows = len(window_stats)
        single_first_minute = sum(
            1
            for stat in window_stats.values()
            if stat["count"] == 1 and stat["first_min_count"] == 1
        )

        return {
            "cycle5_window_count": float(total_windows),
            "cycle5_first_minute_single_count": float(single_first_minute),
            "cycle5_first_minute_single_ratio": single_first_minute / total_windows,
        }

    def compute_open_timing_metrics(
        self,
        trades: List[Dict[str, Any]],
        market_open_times: Dict[str, Any],
    ) -> Dict[str, float]:
        buy_delays_min: List[float] = []

        for trade in trades:
            side = str(_extract_first(trade, ("side",), "")).upper()
            if side != "BUY":
                continue

            slug = str(_extract_first(trade, ("slug", "marketSlug"), ""))
            trade_ts = self._get_trade_ts(trade)
            open_ts = _to_datetime(market_open_times.get(slug))
            if not slug or trade_ts is None or open_ts is None:
                continue
            if trade_ts < open_ts:
                continue

            delay_min = (trade_ts - open_ts).total_seconds() / 60.0
            buy_delays_min.append(delay_min)

        if not buy_delays_min:
            return {
                "buy_count_with_open_time": 0.0,
                "near_open_buy_ratio_10m": 0.0,
                "near_open_buy_ratio_30m": 0.0,
                "avg_open_delay_min": 0.0,
                "median_open_delay_min": 0.0,
            }

        total = len(buy_delays_min)
        near_10 = sum(1 for x in buy_delays_min if x <= 10) / total
        near_30 = sum(1 for x in buy_delays_min if x <= 30) / total
        avg_delay = sum(buy_delays_min) / total

        delays_sorted = sorted(buy_delays_min)
        mid = len(delays_sorted) // 2
        if len(delays_sorted) % 2 == 0:
            median_delay = (delays_sorted[mid - 1] + delays_sorted[mid]) / 2.0
        else:
            median_delay = delays_sorted[mid]

        return {
            "buy_count_with_open_time": float(total),
            "near_open_buy_ratio_10m": near_10,
            "near_open_buy_ratio_30m": near_30,
            "avg_open_delay_min": avg_delay,
            "median_open_delay_min": median_delay,
        }

    def score_trader(
        self,
        public_profile: Dict[str, Any],
        trades: List[Dict[str, Any]],
        current_positions: List[Dict[str, Any]],
        closed_positions: List[Dict[str, Any]],
        activity: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        _ = current_positions, activity

        # Use recent closed positions for PnL-style metrics to keep recency.
        closed_90d = self._filter_window(closed_positions, 90, lambda p: self._get_position_open_close(p)[1])

        profitability = self._profitability_metrics(closed_90d)
        stability = self._stability_metrics(closed_90d)
        copyability = self._copyability_metrics(trades, closed_90d)
        focus = self._focus_metrics(trades)
        execution = self._execution_metrics(trades)

        profitability_score = _clip01(
            0.45 * _sigmoid(profitability["realized_pnl"] / 500.0)
            + 0.35 * _clip01(profitability["profit_factor"] / 3.0)
            + 0.20 * _clip01(profitability["avg_trade_pnl"] / 30.0)
        )
        stability_score = _clip01(
            0.45 * (1 - _clip01(stability["max_drawdown"] / 1000.0))
            + 0.30 * (1 - _clip01(stability["max_losing_streak"] / 8.0))
            + 0.25 * (1 - _clip01(stability["weekly_pnl_volatility"] / 400.0))
        )

        # Day frequency score peaks near 2-3 trades/day and declines both sides.
        freq = copyability["avg_trades_per_day_30d"]
        freq_score = math.exp(-((freq - 2.5) ** 2) / 8.0)
        hold_score = _clip01(copyability["avg_hold_hours"] / 6.0)
        rush_penalty = copyability["near_settlement_ratio"]
        copyability_score = _clip01(0.45 * freq_score + 0.40 * hold_score + 0.15 * (1 - rush_penalty))

        focus_score = _clip01(
            0.45 * focus["crypto_ratio"] + 0.25 * focus["btc_ratio"] + 0.30 * focus["concentration"]
        )

        # Moderate position sizing is easier for normal accounts to replicate.
        median_notional = execution["median_notional"]
        execution_score = _clip01(math.exp(-((median_notional - 200.0) ** 2) / (2 * 300.0**2)))

        breakdown = ScoreBreakdown(
            profitability=profitability_score,
            stability=stability_score,
            copyability=copyability_score,
            focus=focus_score,
            execution=execution_score,
        )

        total = (
            breakdown.profitability * self.weights.get("profitability", 0.0)
            + breakdown.stability * self.weights.get("stability", 0.0)
            + breakdown.copyability * self.weights.get("copyability", 0.0)
            + breakdown.focus * self.weights.get("focus", 0.0)
            + breakdown.execution * self.weights.get("execution", 0.0)
        ) * 100.0

        name = _extract_first(public_profile, ("username", "handle", "name", "nickname"), "")
        x_name = _extract_first(public_profile, ("xUsername", "twitterUsername", "x", "twitter"), "")
        verified = bool(_extract_first(public_profile, ("verified", "isVerified", "badge"), False))

        return {
            "name": name,
            "x_username": x_name,
            "verified": verified,
            "copyability_score": round(total, 2),
            "score_profitability": round(breakdown.profitability * 100, 2),
            "score_stability": round(breakdown.stability * 100, 2),
            "score_copyability": round(breakdown.copyability * 100, 2),
            "score_focus": round(breakdown.focus * 100, 2),
            "score_execution": round(breakdown.execution * 100, 2),
            "realized_pnl_90d": round(profitability["realized_pnl"], 4),
            "avg_trade_pnl_90d": round(profitability["avg_trade_pnl"], 4),
            "profit_factor_90d": round(profitability["profit_factor"], 4),
            "win_rate_90d": round(profitability["win_rate"], 4),
            "max_drawdown_90d": round(stability["max_drawdown"], 4),
            "max_losing_streak_90d": stability["max_losing_streak"],
            "weekly_vol_90d": round(stability["weekly_pnl_volatility"], 4),
            "avg_trades_per_day_30d": round(copyability["avg_trades_per_day_30d"], 4),
            "avg_hold_hours_90d": round(copyability["avg_hold_hours"], 4),
            "near_settlement_ratio_90d": round(copyability["near_settlement_ratio"], 4),
            "crypto_ratio": round(focus["crypto_ratio"], 4),
            "btc_ratio": round(focus["btc_ratio"], 4),
            "market_concentration_hhi": round(focus["concentration"], 4),
            "median_trade_notional": round(execution["median_notional"], 4),
            "avg_trade_notional": round(execution["avg_notional"], 4),
            "trades_count": len(trades),
            "closed_positions_count": len(closed_positions),
        }
