from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from .client import PolyDataApiClient
from .config import AnalyzerConfig
from .copybot_dashboard import DashboardServer
from .copybot_execution import build_executor
from .copybot_models import RuntimeConfig, to_primitive, utc_now
from .copybot_services import (
    CopyIntentFactory,
    FileSignalSource,
    LeaderEquityService,
    LeaderPollingSource,
    OrderAggregator,
    PositionTracker,
    RiskManager,
)
from .copybot_storage import build_repository


class CopyTradingRuntime:
    def __init__(self, config: RuntimeConfig):
        self.config = config
        self.repository = build_repository(config)
        self.state = self.repository.load_runtime_state(default_cash=config.wallet.starting_balance_usdc)
        self.tracker = PositionTracker(
            repository=self.repository,
            state=self.state,
            starting_balance_usdc=config.wallet.starting_balance_usdc,
        )
        self.trader_lookup = config.trader_lookup()
        self.aggregator = OrderAggregator(config.aggregation_window_sec)
        self.risk = RiskManager(config.risk, self.tracker, self.trader_lookup)
        self.market_cache: Dict[str, Dict[str, Any]] = {}
        proxies = {"http": config.http_proxy, "https": config.https_proxy}
        self.executor = build_executor(
            config.wallet,
            config.risk,
            paper_simulation=config.paper_simulation,
            market_price_lookup=self.lookup_market_price,
            timeout_sec=config.request_timeout_sec,
            proxies=proxies,
        )
        self.dashboard_server: Optional[DashboardServer] = None
        self.leader_position_cache: Dict[str, Dict[str, Any]] = {}

        analyzer_cfg = AnalyzerConfig(
            base_url=config.data_api_base_url,
            gamma_base_url=config.gamma_api_base_url,
            http_proxy=config.http_proxy,
            https_proxy=config.https_proxy,
            request_timeout_sec=config.request_timeout_sec,
            sleep_between_requests_sec=0.0,
        )
        self.data_client = PolyDataApiClient(analyzer_cfg)
        self.leader_equity_service = LeaderEquityService(self.data_client)
        self.intent_factory = CopyIntentFactory(
            self.trader_lookup,
            self.tracker,
            leader_equity_service=self.leader_equity_service,
        )
        self.signal_sources = [
            LeaderPollingSource(
                client=self.data_client,
                trader_configs=config.traders,
                state=self.state,
                history_limit=config.history_limit_per_trader,
            )
        ]
        if config.strategy_signal_file:
            self.signal_sources.append(FileSignalSource(config.strategy_signal_file, self.state))

    @staticmethod
    def _float(value: Any, default: float = 0.0) -> float:
        try:
            if value is None:
                return default
            return float(value)
        except (TypeError, ValueError):
            return default

    def _mark_price_from_book(self, book: Dict[str, Any], fallback: float = 0.0) -> float:
        if not isinstance(book, dict):
            return fallback
        raw_bids = book.get("bids") if isinstance(book.get("bids"), list) else []
        raw_asks = book.get("asks") if isinstance(book.get("asks"), list) else []
        bids = sorted(
            [self._float(item.get("price"), 0.0) for item in raw_bids if isinstance(item, dict)],
            reverse=True,
        )
        asks = sorted(
            [self._float(item.get("price"), 0.0) for item in raw_asks if isinstance(item, dict)]
        )
        best_bid = 0.0
        best_ask = 0.0
        if bids:
            best_bid = bids[0]
        if asks:
            best_ask = asks[0]
        if best_bid > 0 and best_ask > 0:
            return (best_bid + best_ask) / 2.0
        last_trade = self._float(book.get("last_trade_price"), 0.0)
        if last_trade > 0:
            return last_trade
        if best_bid > 0:
            return best_bid
        if best_ask > 0:
            return best_ask
        return fallback

    def _parse_json_list(self, value: Any) -> List[Any]:
        if isinstance(value, list):
            return value
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return []
            try:
                parsed = json.loads(text)
                return parsed if isinstance(parsed, list) else []
            except json.JSONDecodeError:
                return []
        return []

    def lookup_market(self, market_slug: str) -> Dict[str, Any]:
        if not market_slug:
            return {}
        now = time.monotonic()
        cached = self.market_cache.get(market_slug)
        if cached and now - cached["fetched_at"] < 3:
            return cached["market"]
        market = self.data_client.get_market_by_slug(market_slug)
        self.market_cache[market_slug] = {"fetched_at": now, "market": market}
        return market

    def lookup_market_price(self, market_slug: str, outcome: str, token_id: str = "") -> float:
        if not market_slug:
            return 0.0
        market = self.lookup_market(market_slug)

        outcomes = [str(item) for item in self._parse_json_list(market.get("outcomes"))]
        prices = self._parse_json_list(market.get("outcomePrices"))
        token_ids = [str(item) for item in self._parse_json_list(market.get("clobTokenIds"))]

        if outcome and outcomes and prices and len(outcomes) == len(prices):
            for idx, label in enumerate(outcomes):
                if label.strip().lower() == outcome.strip().lower():
                    return self._float(prices[idx], 0.0)

        if token_id and token_ids and prices and len(token_ids) == len(prices):
            for idx, item in enumerate(token_ids):
                if item == token_id:
                    return self._float(prices[idx], 0.0)

        return 0.0

    def _extract_binary_token_payouts(self, market: Dict[str, Any]) -> Dict[str, float]:
        token_ids = [str(item) for item in self._parse_json_list(market.get("clobTokenIds"))]
        prices = [self._float(item, -1.0) for item in self._parse_json_list(market.get("outcomePrices"))]
        if not token_ids or not prices or len(token_ids) != len(prices):
            return {}

        token_payouts: Dict[str, float] = {}
        for token_id, price in zip(token_ids, prices):
            if abs(price - 1.0) <= 1e-9:
                token_payouts[token_id] = 1.0
            elif abs(price - 0.0) <= 1e-9:
                token_payouts[token_id] = 0.0
            else:
                return {}
        return token_payouts

    def _settle_resolved_markets(self) -> None:
        positions = self.tracker.list_positions()
        if not positions:
            return

        open_markets = {str(item.get("market_slug", "")) for item in positions if item.get("market_slug")}
        for market_slug in sorted(open_markets):
            market = self.lookup_market(market_slug)
            if not market:
                continue

            token_payouts = self._extract_binary_token_payouts(market)
            if not token_payouts:
                continue

            is_closed = bool(market.get("closed"))
            accepting_orders = bool(market.get("acceptingOrders", True))
            if not is_closed or accepting_orders:
                continue

            settlements = self.tracker.apply_market_settlement(token_payouts)
            if not settlements:
                continue

            total_payout = sum(item["payout_amount_usdc"] for item in settlements)
            total_realized = sum(item["realized_pnl_usdc"] for item in settlements)
            self.log(
                "INFO",
                "paper positions settled",
                market_slug=market_slug,
                lots=len(settlements),
                payout_amount_usdc=round(total_payout, 6),
                realized_pnl_usdc=round(total_realized, 6),
            )

    def _normalize_leader_positions(self, rows: List[Dict[str, Any]], trader) -> List[Dict[str, Any]]:
        grouped: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            token_id = str(row.get("asset", "")).strip()
            shares = self._float(row.get("size"), 0.0)
            if not token_id or shares <= 0:
                continue

            current_value = self._float(row.get("currentValue"), 0.0)
            current_price = self._float(row.get("curPrice"), 0.0)
            avg_price = self._float(row.get("avgPrice"), 0.0)
            if current_value <= 0 and current_price > 0:
                current_value = shares * current_price

            # Drop stale resolved positions that are only lingering as redeemable dust.
            if current_value <= 0 and current_price <= 0 and bool(row.get("redeemable")):
                continue

            bucket = grouped.setdefault(
                token_id,
                {
                    "trader_address": trader.address.lower(),
                    "trader_label": trader.display_name(),
                    "token_id": token_id,
                    "market_slug": str(row.get("slug", "")).strip(),
                    "market_title": str(row.get("title", "")).strip(),
                    "outcome": str(row.get("outcome", "")).strip(),
                    "leader_shares": 0.0,
                    "leader_avg_price_notional": 0.0,
                    "leader_avg_price": 0.0,
                    "leader_mark_price": 0.0,
                    "leader_market_value_usdc": 0.0,
                    "leader_unrealized_pnl_usdc": 0.0,
                    "leader_realized_pnl_usdc": 0.0,
                },
            )
            bucket["leader_shares"] += shares
            bucket["leader_avg_price_notional"] += shares * avg_price
            bucket["leader_market_value_usdc"] += current_value
            bucket["leader_unrealized_pnl_usdc"] += self._float(row.get("cashPnl"), 0.0)
            bucket["leader_realized_pnl_usdc"] += self._float(row.get("realizedPnl"), 0.0)
            if current_price > 0:
                bucket["leader_mark_price"] = current_price

        normalized: List[Dict[str, Any]] = []
        for item in grouped.values():
            shares = item["leader_shares"]
            item["leader_avg_price"] = item["leader_avg_price_notional"] / shares if shares > 0 else 0.0
            item.pop("leader_avg_price_notional", None)
            if item["leader_mark_price"] <= 0 and shares > 0:
                item["leader_mark_price"] = item["leader_market_value_usdc"] / shares if item["leader_market_value_usdc"] > 0 else item["leader_avg_price"]
            normalized.append(item)

        normalized.sort(key=lambda row: row["leader_market_value_usdc"], reverse=True)
        return normalized

    def _refresh_leader_positions(self) -> None:
        all_rows: List[Dict[str, Any]] = []
        now = time.monotonic()
        for trader in self.config.traders:
            if not trader.enabled:
                continue

            address = trader.address.lower()
            ttl_sec = max(5, trader.leader_equity_cache_ttl_sec or 10)
            cached = self.leader_position_cache.get(address)
            rows: List[Dict[str, Any]]
            if cached and now - cached["fetched_at"] < ttl_sec:
                rows = [dict(item) for item in cached.get("rows", [])]
            else:
                try:
                    current_positions = self.data_client.get_current_positions(address)
                    rows = self._normalize_leader_positions(current_positions, trader)
                    self.leader_position_cache[address] = {"fetched_at": now, "rows": rows}
                except Exception as exc:
                    self.log("WARN", "leader positions refresh failed", trader=trader.display_name(), error=str(exc))
                    rows = [dict(item) for item in cached.get("rows", [])] if cached else []
            all_rows.extend(rows)

        self.state.leader_positions = all_rows

    def _refresh_mark_to_market(self) -> None:
        positions = self.tracker.list_positions()
        if not positions:
            self.state.mark_prices = {}
            self.state.market_value_usdc = 0.0
            self.state.unrealized_pnl_usdc = 0.0
            self.state.tracked_equity_usdc = self.state.cash_balance_usdc
            return

        market_client = getattr(self.executor, "market_client", None)
        previous_marks = dict(self.state.mark_prices)
        mark_prices: Dict[str, float] = {}
        market_value = 0.0
        total_cost_basis = 0.0

        for position in positions:
            token_id = str(position.get("token_id", ""))
            shares = self._float(position.get("follower_remaining_shares"), 0.0)
            avg_entry = self._float(position.get("avg_entry_price"), 0.0)
            cost_basis = self._float(position.get("cost_basis_usdc"), 0.0)
            fallback_mark = previous_marks.get(token_id, avg_entry)
            mark_price = self.lookup_market_price(
                str(position.get("market_slug", "")),
                str(position.get("outcome", "")),
                token_id,
            )
            if mark_price <= 0:
                mark_price = fallback_mark
            if market_client is not None and token_id and mark_price <= 0:
                try:
                    book = market_client.get_order_book(token_id)
                    mark_price = self._mark_price_from_book(book, fallback=fallback_mark)
                except Exception as exc:
                    self.log("WARN", "mark price refresh failed", token_id=token_id, error=str(exc))
            mark_prices[token_id] = mark_price
            total_cost_basis += cost_basis
            market_value += shares * mark_price

        self.state.mark_prices = mark_prices
        self.state.market_value_usdc = market_value
        self.state.unrealized_pnl_usdc = market_value - total_cost_basis
        self.state.tracked_equity_usdc = self.state.cash_balance_usdc + market_value

    def log(self, level: str, message: str, **extra: Any) -> None:
        printable = f"[{level}] {message}"
        if extra:
            printable = f"{printable} | {extra}"
        print(printable)
        self.repository.record_log(level, message, extra or None)

    def start_dashboard(self) -> None:
        if not self.config.dashboard.enabled or self.dashboard_server is not None:
            return
        self.dashboard_server = DashboardServer(
            repository=self.repository,
            host=self.config.dashboard.host,
            port=self.config.dashboard.port,
            refresh_sec=self.config.dashboard.refresh_sec,
        )
        self.dashboard_server.start_in_thread()
        self.log(
            "INFO",
            "dashboard started",
            host=self.config.dashboard.host,
            port=self.config.dashboard.port,
        )

    def _collect_events(self) -> List[Any]:
        events = []
        for source in self.signal_sources:
            try:
                events.extend(source.poll())
            except Exception as exc:
                self.log("ERROR", "signal source failed", source=source.__class__.__name__, error=str(exc))
        return sorted(events, key=lambda item: item.observed_at)

    def _process_orders(self, orders: List[Any]) -> None:
        for order in orders:
            order_doc = to_primitive(order)
            self.repository.record_order(order_doc, status="pending")

            allowed, reason = self.risk.evaluate(order)
            if not allowed:
                self.repository.update_order_status(order.order_id, status="rejected", reason=reason)
                self.log("WARN", "order rejected by risk", order_id=order.order_id, reason=reason)
                continue

            try:
                result = self.executor.execute(order)
            except Exception as exc:
                self.repository.update_order_status(order.order_id, status="rejected", reason=str(exc))
                self.log(
                    "ERROR",
                    "order execution failed",
                    order_id=order.order_id,
                    error=str(exc),
                )
                continue
            self.repository.record_execution(to_primitive(result))
            self.repository.update_order_status(
                order.order_id,
                status=result.status,
                reason=result.reason,
                execution_id=result.execution_id,
            )

            if result.status in {"simulated", "executed", "partial"} and result.executed_shares > 0:
                self.tracker.apply_execution(order, result)
                self.log(
                    "INFO",
                    "order executed",
                    order_id=order.order_id,
                    status=result.status,
                    avg_price=result.avg_price,
                    executed_shares=result.executed_shares,
                )
            else:
                self.log(
                    "WARN",
                    "order not filled",
                    order_id=order.order_id,
                    status=result.status,
                    reason=result.reason,
                )

    def run(self, max_cycles: Optional[int] = None) -> None:
        self.start_dashboard()
        self.log("INFO", "copybot runtime started", mode=self.config.wallet.mode, traders=len(self.config.traders))

        cycles = 0
        try:
            while True:
                cycle_start = time.monotonic()
                cycles += 1
                events = self._collect_events()
                if events:
                    self.repository.record_leader_events([to_primitive(item) for item in events])
                    intents = self.intent_factory.build(events)
                    if intents:
                        self.aggregator.add(intents)
                    self.log(
                        "INFO",
                        "new leader signals captured",
                        events=len(events),
                        intents=len(intents),
                    )

                ready_orders = self.aggregator.flush_ready()
                if ready_orders:
                    self._process_orders(ready_orders)

                self._refresh_leader_positions()
                self._settle_resolved_markets()
                self._refresh_mark_to_market()
                self.state.updated_at = utc_now()
                self.repository.save_runtime_state(self.state)

                if max_cycles is not None and cycles >= max_cycles:
                    break

                elapsed = time.monotonic() - cycle_start
                time.sleep(max(0.0, self.config.poll_interval_sec - elapsed))
        except KeyboardInterrupt:
            self.log("INFO", "copybot runtime interrupted by user")
        finally:
            ready_orders = self.aggregator.flush_ready(force=True)
            if ready_orders:
                self._process_orders(ready_orders)
            self._refresh_leader_positions()
            self._settle_resolved_markets()
            self._refresh_mark_to_market()
            self.state.updated_at = utc_now()
            self.repository.save_runtime_state(self.state)


def load_runtime(path: Path) -> CopyTradingRuntime:
    return CopyTradingRuntime(RuntimeConfig.from_file(path))
