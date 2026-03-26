from __future__ import annotations

import json
import time
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

from .copybot_models import (
    AggregatedOrder,
    ClosePlanItem,
    LeaderTradeEvent,
    MirroredLot,
    RuntimeStateSnapshot,
    SourceIntent,
    TraderConfig,
    utc_now,
)
from .copybot_storage import BaseRepository


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


class LeaderPollingSource:
    def __init__(
        self,
        client,
        trader_configs: Iterable[TraderConfig],
        state: RuntimeStateSnapshot,
        history_limit: int,
        max_signal_age_sec: int = 8,
    ):
        self.client = client
        self.trader_configs = [item for item in trader_configs if item.enabled]
        self.state = state
        self.history_limit = max(1, history_limit)
        self.max_signal_age_sec = max(0, int(max_signal_age_sec))

    def poll(self) -> List[LeaderTradeEvent]:
        # 这一层的职责不是“尽可能多地抓历史”，而是“尽可能快地拿到最新 leader 成交”。
        # 因为我们是实时跟单，过旧的信号即使还没被处理，也往往已经失去复制价值。
        events: List[LeaderTradeEvent] = []
        now = utc_now()
        cutoff = now - timedelta(seconds=self.max_signal_age_sec) if self.max_signal_age_sec > 0 else None
        for trader in self.trader_configs:
            # 实测里 activity 往往比 trades 更快出现最新成交，所以这里把 activity 放在优先位。
            # 只有 activity 暂时拿不到 TRADE 数据时，才退回到 user_trades。
            activity_rows = self.client.get_user_activity(
                trader.address,
                limit=max(self.history_limit * 3, 20),
            )
            rows = [
                item
                for item in activity_rows
                if str(item.get("type", "")).upper() == "TRADE"
            ]
            if not rows:
                rows = self.client.get_user_trades(trader.address, limit=self.history_limit)
            grouped = self._normalize_rows(rows, trader)
            if not grouped:
                continue

            state_key = trader.address.lower()
            last_seen = self.state.last_seen.get(state_key, {})
            last_ts = _safe_int(last_seen.get("timestamp"), 0)
            last_ids = set(last_seen.get("event_ids", []))

            fresh: List[LeaderTradeEvent] = []
            for event in grouped:
                event_ts = int(event.observed_at.timestamp())
                # 去重的主键不是简单的 tx hash，而是“时间戳 + event_id”双保险。
                # 这样既能挡住同一轮 poll 的重复数据，也能挡住 activity / trades 两个源的重叠回包。
                if event_ts > last_ts or (event_ts == last_ts and event.event_id not in last_ids):
                    fresh.append(event)

            if not fresh:
                continue

            newest_ts = int(fresh[-1].observed_at.timestamp())
            newest_ids = [item.event_id for item in fresh if int(item.observed_at.timestamp()) == newest_ts]
            # 即使后面会把“太旧的信号”过滤掉，也要先推进 last_seen 游标。
            # 否则 bot 重启或下一轮 poll 时，会把这批旧信号反复当成新信号再处理一次。
            self.state.last_seen[state_key] = {"timestamp": newest_ts, "event_ids": newest_ids}
            if cutoff is not None:
                fresh = [item for item in fresh if item.observed_at >= cutoff]
            events.extend(fresh)

        events.sort(key=lambda item: item.observed_at)
        return events

    def _normalize_rows(self, rows: List[Dict[str, Any]], trader: TraderConfig) -> List[LeaderTradeEvent]:
        # 外部接口返回的常常是一笔真实成交拆成多条 fill。
        # 这里先把同一 tx / token / side 的 fill 合并成一个 leader event，
        # 后面的 sizing、聚合、风控都围绕这个更稳定的事件粒度来算。
        grouped: Dict[str, Dict[str, Any]] = {}

        for row in sorted(rows, key=lambda item: _safe_int(item.get("timestamp"), 0)):
            side = str(row.get("side", "")).upper().strip()
            token_id = str(row.get("asset", "")).strip()
            shares = _safe_float(row.get("size"), 0.0)
            price = _safe_float(row.get("price"), 0.0)
            timestamp = _safe_int(row.get("timestamp"), 0)
            if side not in {"BUY", "SELL"} or not token_id or shares <= 0 or price <= 0 or timestamp <= 0:
                continue

            tx_hash = str(row.get("transactionHash", "")).strip().lower()
            fallback_key = f"{timestamp}:{token_id}:{side}:{price:.6f}"
            group_key = f"{trader.address}:{tx_hash or fallback_key}:{token_id}:{side}"
            bucket = grouped.setdefault(
                group_key,
                {
                    "tx_hash": tx_hash,
                    "token_id": token_id,
                    "condition_id": str(row.get("conditionId", "")).strip(),
                    "market_slug": str(row.get("slug", "")).strip(),
                    "market_title": str(row.get("title", "")).strip(),
                    "outcome": str(row.get("outcome", "")).strip(),
                    "side": side,
                    "timestamp": timestamp,
                    "weighted_notional": 0.0,
                    "shares": 0.0,
                    "raw": [],
                },
            )
            bucket["weighted_notional"] += shares * price
            bucket["shares"] += shares
            # 同一组合事件里保留最早时间戳，便于后面计算“从 leader 信号到我成交”这条延迟链。
            bucket["timestamp"] = min(bucket["timestamp"], timestamp)
            bucket["raw"].append(row)

        events: List[LeaderTradeEvent] = []
        for key, bucket in sorted(grouped.items(), key=lambda item: item[1]["timestamp"]):
            shares = bucket["shares"]
            if shares <= 0:
                continue
            avg_price = bucket["weighted_notional"] / shares
            event = LeaderTradeEvent(
                event_id=key,
                trader_address=trader.address.lower(),
                trader_label=trader.display_name(),
                tx_hash=bucket["tx_hash"],
                token_id=bucket["token_id"],
                condition_id=bucket["condition_id"],
                market_slug=bucket["market_slug"],
                market_title=bucket["market_title"],
                outcome=bucket["outcome"],
                side=bucket["side"],
                price=avg_price,
                leader_shares=shares,
                leader_notional_usdc=bucket["weighted_notional"],
                observed_at=datetime.fromtimestamp(bucket["timestamp"], tz=timezone.utc),
                raw={"fills": bucket["raw"]},
            )
            events.append(event)
        return events


class FileSignalSource:
    def __init__(self, path: str, state: RuntimeStateSnapshot):
        self.path = Path(path).expanduser() if path else None
        self.state = state

    def poll(self) -> List[LeaderTradeEvent]:
        if not self.path or not self.path.exists():
            return []

        state_key = f"signal_file::{self.path.resolve()}"
        offset_state = self.state.last_seen.get(state_key, {})
        last_line = _safe_int(offset_state.get("line_number"), 0)

        events: List[LeaderTradeEvent] = []
        current_line = 0
        with self.path.open("r", encoding="utf-8") as handle:
            for line in handle:
                current_line += 1
                if current_line <= last_line:
                    continue
                text = line.strip()
                if not text:
                    continue
                payload = json.loads(text)
                side = str(payload.get("side", "")).upper().strip()
                price = _safe_float(payload.get("price"), 0.0)
                leader_shares = _safe_float(payload.get("leader_shares"), 0.0)
                leader_notional = _safe_float(payload.get("leader_notional_usdc"), 0.0)
                if leader_notional <= 0 and leader_shares > 0 and price > 0:
                    leader_notional = leader_shares * price
                if leader_shares <= 0 and leader_notional > 0 and price > 0:
                    leader_shares = leader_notional / price

                if side not in {"BUY", "SELL"}:
                    continue

                signal_id = str(payload.get("event_id") or payload.get("id") or uuid.uuid4())
                trader_address = str(payload.get("trader_address") or payload.get("strategy_id") or "strategy-signal").lower()
                trader_label = str(payload.get("trader_label") or payload.get("label") or trader_address)
                timestamp = payload.get("timestamp")
                events.append(
                    LeaderTradeEvent(
                        event_id=signal_id,
                        trader_address=trader_address,
                        trader_label=trader_label,
                        tx_hash=str(payload.get("tx_hash", "")),
                        token_id=str(payload.get("token_id", "")),
                        condition_id=str(payload.get("condition_id", "")),
                        market_slug=str(payload.get("market_slug", "")),
                        market_title=str(payload.get("market_title", "")),
                        outcome=str(payload.get("outcome", "")),
                        side=side,
                        price=price,
                        leader_shares=leader_shares,
                        leader_notional_usdc=leader_notional,
                        observed_at=utc_now()
                        if not timestamp
                        else datetime.fromtimestamp(_safe_int(timestamp), tz=timezone.utc),
                        source="strategy_file",
                        raw=payload,
                    )
                )

        self.state.last_seen[state_key] = {"line_number": current_line}
        return events


class LeaderEquityService:
    def __init__(self, client, default_ttl_sec: int = 15):
        self.client = client
        self.default_ttl_sec = max(1, default_ttl_sec)
        self.cache: Dict[str, Dict[str, Any]] = {}

    def get_equity(self, trader: TraderConfig) -> float:
        address = trader.address.lower()
        ttl_sec = max(1, trader.leader_equity_cache_ttl_sec or self.default_ttl_sec)
        cached = self.cache.get(address)
        now = time.monotonic()
        if cached and now - cached["fetched_at"] < ttl_sec:
            return cached["equity"]

        snapshot = self.client.get_accounting_snapshot(address)
        equity = _safe_float(snapshot.get("equity"), 0.0)
        self.cache[address] = {
            "equity": equity,
            "cash_balance": _safe_float(snapshot.get("cashBalance"), 0.0),
            "positions_value": _safe_float(snapshot.get("positionsValue"), 0.0),
            "valuation_time": snapshot.get("valuationTime", ""),
            "fetched_at": now,
        }
        return equity

    def get_cached_snapshot(self, address: str) -> Dict[str, Any]:
        return dict(self.cache.get(address.lower(), {}))


class PositionTracker:
    def __init__(
        self,
        repository: BaseRepository,
        state: RuntimeStateSnapshot,
        starting_balance_usdc: float,
    ):
        self.repository = repository
        self.state = state
        if self.state.cash_balance_usdc == 0:
            self.state.cash_balance_usdc = starting_balance_usdc
        self.lots: Dict[str, MirroredLot] = {
            lot.lot_id: lot for lot in repository.load_open_lots()
        }

    def _roll_daily_realized(self) -> None:
        today = utc_now().date().isoformat()
        if self.state.realized_pnl_day != today:
            self.state.realized_pnl_day = today
            self.state.realized_pnl_today_usdc = 0.0

    def snapshot(self) -> Dict[str, Any]:
        self._roll_daily_realized()
        market_exposure: Dict[str, float] = defaultdict(float)
        trader_exposure: Dict[str, float] = defaultdict(float)
        open_pairs = set()
        total_exposure = 0.0
        total_cost_basis = 0.0
        for lot in self.lots.values():
            if lot.status != "open" or lot.follower_remaining_shares <= 0:
                continue
            exposure = lot.follower_remaining_shares * lot.entry_price
            total_exposure += exposure
            total_cost_basis += exposure
            market_exposure[lot.token_id] += exposure
            trader_exposure[lot.trader_address] += exposure
            open_pairs.add((lot.trader_address, lot.token_id))

        return {
            "cash_balance_usdc": self.state.cash_balance_usdc,
            "realized_pnl_today_usdc": self.state.realized_pnl_today_usdc,
            "total_exposure_usdc": total_exposure,
            "total_cost_basis_usdc": total_cost_basis,
            "tracked_equity_usdc": self.state.cash_balance_usdc + total_cost_basis,
            "market_exposure_usdc": dict(market_exposure),
            "trader_exposure_usdc": dict(trader_exposure),
            "open_positions": len(open_pairs),
        }

    def has_open_position(self, trader_address: str, token_id: str) -> bool:
        return any(
            lot.status == "open"
            and lot.trader_address == trader_address.lower()
            and lot.token_id == token_id
            and lot.follower_remaining_shares > 0
            for lot in self.lots.values()
        )

    def plan_close(self, trader_address: str, token_id: str, leader_shares_to_close: float) -> List[ClosePlanItem]:
        remaining = max(0.0, leader_shares_to_close)
        out: List[ClosePlanItem] = []
        lots = sorted(
            (
                lot
                for lot in self.lots.values()
                if lot.status == "open"
                and lot.trader_address == trader_address.lower()
                and lot.token_id == token_id
                and lot.leader_remaining_shares > 0
                and lot.follower_remaining_shares > 0
            ),
            key=lambda item: item.opened_at,
        )

        for lot in lots:
            if remaining <= 1e-9:
                break
            leader_close = min(remaining, lot.leader_remaining_shares)
            ratio = leader_close / lot.leader_remaining_shares if lot.leader_remaining_shares else 0.0
            follower_close = lot.follower_remaining_shares * ratio
            out.append(
                ClosePlanItem(
                    lot_id=lot.lot_id,
                    leader_shares=leader_close,
                    follower_shares=follower_close,
                )
            )
            remaining -= leader_close

        return out

    def apply_execution(self, order: AggregatedOrder, result) -> None:
        if result.executed_shares <= 0 or result.status not in {"simulated", "executed", "partial"}:
            return

        self._roll_daily_realized()
        total_metric = order.requested_amount_usdc if order.side == "BUY" else order.requested_shares
        if total_metric <= 0:
            return

        for component in order.components:
            component_metric = component.requested_amount_usdc if order.side == "BUY" else component.requested_shares
            if component_metric <= 0:
                continue
            weight = component_metric / total_metric
            component_amount = result.executed_amount_usdc * weight
            component_shares = result.executed_shares * weight
            if order.side == "BUY":
                self._apply_buy_component(component, component_amount, component_shares, result.avg_price)
            else:
                self._apply_sell_component(component, component_amount, component_shares, result.avg_price)

        self.state.updated_at = utc_now()
        self.repository.save_runtime_state(self.state)

    def _apply_buy_component(
        self,
        component: SourceIntent,
        executed_amount_usdc: float,
        executed_shares: float,
        avg_price: float,
    ) -> None:
        if executed_shares <= 0 or executed_amount_usdc <= 0:
            return

        requested = component.requested_shares if component.requested_shares > 0 else executed_shares
        ratio = min(1.0, executed_shares / requested) if requested > 0 else 1.0
        mirrored_leader_shares = component.leader_shares * ratio
        lot = MirroredLot(
            lot_id=str(uuid.uuid4()),
            trader_address=component.trader_address,
            trader_label=component.trader_label,
            token_id=component.token_id,
            condition_id=component.condition_id,
            market_slug=component.market_slug,
            market_title=component.market_title,
            outcome=component.outcome,
            leader_entry_tx_hash=component.leader_tx_hash,
            leader_entry_event_id=component.leader_event_id,
            leader_initial_shares=mirrored_leader_shares,
            leader_remaining_shares=mirrored_leader_shares,
            follower_initial_shares=executed_shares,
            follower_remaining_shares=executed_shares,
            entry_price=avg_price,
            entry_notional_usdc=executed_amount_usdc,
            opened_at=utc_now(),
        )
        self.lots[lot.lot_id] = lot
        self.repository.upsert_lot(lot)
        self.state.cash_balance_usdc -= executed_amount_usdc

    def _apply_sell_component(
        self,
        component: SourceIntent,
        executed_amount_usdc: float,
        executed_shares: float,
        avg_price: float,
    ) -> None:
        if executed_shares <= 0 or executed_amount_usdc < 0:
            return

        requested = component.requested_shares if component.requested_shares > 0 else executed_shares
        ratio = min(1.0, executed_shares / requested) if requested > 0 else 1.0
        self.state.cash_balance_usdc += executed_amount_usdc

        for plan_item in component.close_plan:
            lot = self.lots.get(plan_item.lot_id)
            if lot is None or lot.status != "open":
                continue
            leader_closed = min(lot.leader_remaining_shares, plan_item.leader_shares * ratio)
            follower_closed = min(lot.follower_remaining_shares, plan_item.follower_shares * ratio)
            if follower_closed <= 0:
                continue
            lot.leader_remaining_shares -= leader_closed
            lot.follower_remaining_shares -= follower_closed
            realized = follower_closed * (avg_price - lot.entry_price)
            lot.realized_pnl_usdc += realized
            self.state.realized_pnl_today_usdc += realized
            if lot.leader_remaining_shares <= 1e-9 or lot.follower_remaining_shares <= 1e-9:
                lot.leader_remaining_shares = max(0.0, lot.leader_remaining_shares)
                lot.follower_remaining_shares = max(0.0, lot.follower_remaining_shares)
                lot.status = "closed"
                lot.closed_at = utc_now()
            self.repository.upsert_lot(lot)

    def apply_market_settlement(self, token_payouts: Dict[str, float]) -> List[Dict[str, Any]]:
        self._roll_daily_realized()
        settlements: List[Dict[str, Any]] = []
        for lot in list(self.lots.values()):
            if lot.status != "open" or lot.follower_remaining_shares <= 0:
                continue
            payout_price = _safe_float(token_payouts.get(lot.token_id), -1.0)
            if payout_price < 0:
                continue

            follower_shares = lot.follower_remaining_shares
            payout_amount = follower_shares * payout_price
            cost_basis = follower_shares * lot.entry_price
            realized = payout_amount - cost_basis

            self.state.cash_balance_usdc += payout_amount
            self.state.realized_pnl_today_usdc += realized
            lot.realized_pnl_usdc += realized
            lot.leader_remaining_shares = 0.0
            lot.follower_remaining_shares = 0.0
            lot.status = "closed"
            lot.closed_at = utc_now()
            self.repository.upsert_lot(lot)

            settlements.append(
                {
                    "lot_id": lot.lot_id,
                    "market_slug": lot.market_slug,
                    "market_title": lot.market_title,
                    "outcome": lot.outcome,
                    "token_id": lot.token_id,
                    "payout_price": payout_price,
                    "payout_amount_usdc": payout_amount,
                    "realized_pnl_usdc": realized,
                }
            )

        if settlements:
            self.state.updated_at = utc_now()
            self.repository.save_runtime_state(self.state)

        return settlements

    def list_positions(self) -> List[Dict[str, Any]]:
        grouped: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for lot in self.lots.values():
            if lot.status != "open" or lot.follower_remaining_shares <= 0:
                continue
            key = (lot.trader_address, lot.token_id)
            bucket = grouped.setdefault(
                key,
                {
                    "trader_address": lot.trader_address,
                    "trader_label": lot.trader_label,
                    "token_id": lot.token_id,
                    "market_slug": lot.market_slug,
                    "market_title": lot.market_title,
                    "outcome": lot.outcome,
                    "leader_remaining_shares": 0.0,
                    "follower_remaining_shares": 0.0,
                    "cost_basis_usdc": 0.0,
                    "avg_entry_price": 0.0,
                },
            )
            bucket["leader_remaining_shares"] += lot.leader_remaining_shares
            bucket["follower_remaining_shares"] += lot.follower_remaining_shares
            bucket["cost_basis_usdc"] += lot.follower_remaining_shares * lot.entry_price

        for bucket in grouped.values():
            shares = bucket["follower_remaining_shares"]
            bucket["avg_entry_price"] = bucket["cost_basis_usdc"] / shares if shares > 0 else 0.0

        return sorted(grouped.values(), key=lambda item: (-item["cost_basis_usdc"], item["trader_label"]))


class CopyIntentFactory:
    def __init__(
        self,
        trader_lookup: Dict[str, TraderConfig],
        tracker: PositionTracker,
        leader_equity_service: LeaderEquityService | None = None,
    ):
        self.trader_lookup = trader_lookup
        self.tracker = tracker
        self.leader_equity_service = leader_equity_service

    def build(self, events: Iterable[LeaderTradeEvent]) -> List[SourceIntent]:
        # 这一层把“leader 做了什么”翻译成“我准备怎么跟”。
        # 它不负责风控，也不负责成交，只负责把复制仓位计算清楚。
        intents: List[SourceIntent] = []
        follower_equity = self.tracker.snapshot()["tracked_equity_usdc"]
        for event in events:
            trader = self.trader_lookup.get(event.trader_address.lower())
            if trader is None or not trader.enabled:
                continue

            multiplier = trader.multiplier_for(event.leader_notional_usdc)
            if event.side == "BUY":
                if trader.sizing_mode == "leader_fraction_of_equity":
                    if self.leader_equity_service is None:
                        continue
                    leader_equity = self.leader_equity_service.get_equity(trader)
                    if leader_equity <= 0 or follower_equity <= 0:
                        continue
                    # “仓位比例跟单”的核心公式：
                    # leader 本单占他总权益多少，我就拿自己总权益的同等比例去跟。
                    leader_fraction = event.leader_notional_usdc / leader_equity
                    requested_amount = follower_equity * leader_fraction * trader.copy_ratio * multiplier
                else:
                    # 固定名义金额模式则直接按 leader 成交额乘跟单比例。
                    requested_amount = event.leader_notional_usdc * trader.copy_ratio * multiplier
                if trader.max_order_usdc > 0:
                    requested_amount = min(requested_amount, trader.max_order_usdc)
                requested_shares = requested_amount / event.price if event.price > 0 else 0.0
                close_plan: List[ClosePlanItem] = []
            else:
                # SELL 不是重新决定开多少仓，而是根据 shadow lot 反推出我该平多少。
                # 这样即使 leader 中间加仓、减仓、余额波动，我们也能按持仓映射关系平仓。
                close_plan = self.tracker.plan_close(event.trader_address, event.token_id, event.leader_shares)
                requested_shares = sum(item.follower_shares for item in close_plan)
                requested_amount = requested_shares * event.price
                if requested_shares <= 0:
                    continue

            if requested_amount <= 0 and requested_shares <= 0:
                continue

            intents.append(
                SourceIntent(
                    intent_id=str(uuid.uuid4()),
                    trader_address=event.trader_address,
                    trader_label=event.trader_label,
                    token_id=event.token_id,
                    condition_id=event.condition_id,
                    market_slug=event.market_slug,
                    market_title=event.market_title,
                    outcome=event.outcome,
                    side=event.side,
                    leader_event_id=event.event_id,
                    leader_tx_hash=event.tx_hash,
                    leader_reference_price=event.price,
                    leader_shares=event.leader_shares,
                    leader_notional_usdc=event.leader_notional_usdc,
                    requested_amount_usdc=requested_amount,
                    requested_shares=requested_shares,
                    multiplier=multiplier,
                    copy_ratio=trader.copy_ratio,
                    close_plan=close_plan,
                )
            )
        return intents


class OrderAggregator:
    def __init__(self, window_sec: float):
        self.window_sec = max(0.0, window_sec)
        self.pending: Dict[str, AggregatedOrder] = {}

    def add(self, intents: Iterable[SourceIntent]) -> None:
        for intent in intents:
            # 当前聚合粒度是 token_id + side。
            # 也就是说，同一 outcome 上连续出现的小 BUY 会被并成一张单，以减少碎单。
            key = f"{intent.token_id}:{intent.side}"
            current = self.pending.get(key)
            weight = intent.requested_amount_usdc if intent.side == "BUY" else intent.requested_shares
            if current is None:
                self.pending[key] = AggregatedOrder(
                    order_id=str(uuid.uuid4()),
                    token_id=intent.token_id,
                    side=intent.side,
                    requested_amount_usdc=intent.requested_amount_usdc,
                    requested_shares=intent.requested_shares,
                    reference_price=intent.leader_reference_price,
                    market_slug=intent.market_slug,
                    market_title=intent.market_title,
                    outcome=intent.outcome,
                    components=[intent],
                    created_at=intent.created_at,
                    last_updated_at=intent.created_at,
                )
                continue

            existing_weight = current.requested_amount_usdc if current.side == "BUY" else current.requested_shares
            total_weight = existing_weight + weight
            if total_weight > 0:
                # 聚合单的参考价不是简单取最新价，而是按订单权重做加权均价。
                # 这样 dashboard 里 leader price deviation 的口径更接近真实聚合成本。
                current.reference_price = (
                    current.reference_price * existing_weight + intent.leader_reference_price * weight
                ) / total_weight
            current.requested_amount_usdc += intent.requested_amount_usdc
            current.requested_shares += intent.requested_shares
            current.components.append(intent)
            current.last_updated_at = intent.created_at

    def flush_ready(self, force: bool = False) -> List[AggregatedOrder]:
        if not self.pending:
            return []
        if self.window_sec <= 0:
            force = True

        now = utc_now()
        ready: List[AggregatedOrder] = []
        remove_keys: List[str] = []
        for key, order in self.pending.items():
            age = (now - order.last_updated_at).total_seconds()
            # 聚合窗口的意义是“牺牲一点点时间，换更像真实可执行订单的体量”。
            # 窗口越大，碎单越少，但复制 leader 的时间偏差也会越大。
            if force or age >= self.window_sec:
                ready.append(order)
                remove_keys.append(key)

        for key in remove_keys:
            self.pending.pop(key, None)

        ready.sort(key=lambda item: item.created_at)
        return ready


class RiskManager:
    def __init__(self, risk_config, tracker: PositionTracker, trader_lookup: Dict[str, TraderConfig]):
        self.risk = risk_config
        self.tracker = tracker
        self.trader_lookup = trader_lookup

    def evaluate(self, order: AggregatedOrder) -> Tuple[bool, str]:
        # 风控只回答一个问题：这张聚合单此刻能不能发。
        # 它不修改订单，只给出 pass / reject 及其原因，方便 dashboard 和日志追踪。
        snapshot = self.tracker.snapshot()

        if order.side == "SELL":
            if order.requested_shares <= 0:
                return False, "sell order has no mirrored position to close"
            return True, "ok"

        if order.requested_amount_usdc < self.risk.min_order_usdc:
            return False, f"aggregated order is below min order size {self.risk.min_order_usdc:.2f}"
        if self.risk.max_order_usdc > 0 and order.requested_amount_usdc > self.risk.max_order_usdc:
            return False, f"aggregated order exceeds max order size {self.risk.max_order_usdc:.2f}"
        if snapshot["realized_pnl_today_usdc"] <= -abs(self.risk.max_daily_loss_usdc):
            return False, "daily loss limit reached"
        if snapshot["cash_balance_usdc"] < order.requested_amount_usdc:
            return False, "insufficient tracked cash balance"
        if snapshot["total_exposure_usdc"] + order.requested_amount_usdc > self.risk.max_total_exposure_usdc:
            return False, "total exposure limit exceeded"

        market_exposure = snapshot["market_exposure_usdc"].get(order.token_id, 0.0)
        if market_exposure + order.requested_amount_usdc > self.risk.max_market_exposure_usdc:
            return False, "market exposure limit exceeded"

        new_pair = any(
            not self.tracker.has_open_position(component.trader_address, component.token_id)
            for component in order.components
        )
        if new_pair and snapshot["open_positions"] >= self.risk.max_open_positions:
            return False, "max open positions reached"

        trader_intents: Dict[str, float] = defaultdict(float)
        for component in order.components:
            trader_intents[component.trader_address] += component.requested_amount_usdc

        for trader_address, amount in trader_intents.items():
            trader_config = self.trader_lookup.get(trader_address)
            current = snapshot["trader_exposure_usdc"].get(trader_address, 0.0)
            limit = self.risk.max_trader_exposure_usdc
            if trader_config and trader_config.max_open_allocation_usdc > 0:
                limit = min(limit, trader_config.max_open_allocation_usdc)
            if current + amount > limit:
                return False, f"trader exposure limit exceeded for {trader_config.display_name() if trader_config else trader_address}"

        return True, "ok"
