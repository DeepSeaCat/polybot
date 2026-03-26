from __future__ import annotations

import random
import time
import uuid
from typing import Any, Dict, List, Tuple

import requests
import urllib3

from .copybot_models import (
    AggregatedOrder,
    ExecutionResult,
    PaperSimulationConfig,
    RiskConfig,
    WalletConfig,
    utc_now,
)


def _float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _bps_delta(base: float, quote: float) -> float:
    if base <= 0 or quote <= 0:
        return 0.0
    return abs(quote - base) / base * 10000.0


class ClobReadOnlyClient:
    def __init__(
        self,
        host: str,
        timeout_sec: int = 10,
        proxies: Dict[str, str] | None = None,
        ws_client=None,
    ):
        self.host = host.rstrip("/")
        self.timeout_sec = timeout_sec
        self.session = requests.Session()
        self.last_books: Dict[str, Dict[str, Any]] = {}
        self.ws_client = ws_client
        if proxies:
            self.session.proxies.update({k: v for k, v in proxies.items() if v})

    @property
    def has_ws_client(self) -> bool:
        return self.ws_client is not None

    def _get_with_ssl_fallback(self, path: str, params: Dict[str, Any]) -> requests.Response:
        url = f"{self.host}{path}"
        try:
            return self.session.get(
                url,
                params=params,
                timeout=self.timeout_sec,
            )
        except requests.exceptions.SSLError:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            return self.session.get(
                url,
                params=params,
                timeout=self.timeout_sec,
                verify=False,
            )

    def get_order_book(self, token_id: str) -> Dict[str, Any]:
        ws_book = self.get_ws_order_book(token_id)
        if ws_book:
            return ws_book
        try:
            response = self._get_with_ssl_fallback("/book", {"token_id": token_id})
            if response.status_code == 404:
                return {}
            response.raise_for_status()
            payload = response.json()
            if isinstance(payload, dict):
                self.last_books[token_id] = payload
                return payload
        except requests.RequestException:
            pass
        return dict(self.last_books.get(token_id, {}))

    def get_ws_order_book(self, token_id: str) -> Dict[str, Any]:
        if self.ws_client is None:
            return {}
        ws_book = self.ws_client.get_order_book(token_id)
        if ws_book:
            self.last_books[token_id] = ws_book
            return dict(ws_book)
        return {}

    def get_mark_price(self, token_id: str) -> float:
        if self.ws_client is None:
            return 0.0
        return _float(self.ws_client.get_mark_price(token_id), 0.0)


class PaperExecutor:
    def __init__(
        self,
        market_client: ClobReadOnlyClient,
        risk: RiskConfig,
        simulation: PaperSimulationConfig | None = None,
        market_price_lookup=None,
        sleeper=time.sleep,
        monotonic=time.monotonic,
        random_uniform=None,
    ):
        self.market_client = market_client
        self.risk = risk
        self.simulation = simulation or PaperSimulationConfig()
        self.market_price_lookup = market_price_lookup
        self.sleeper = sleeper
        self.monotonic = monotonic
        self.random_uniform = random_uniform or random.uniform

    def execute(self, order: AggregatedOrder) -> ExecutionResult:
        # 先统一模拟“我发现信号 -> 做决策 -> 发出请求”这段链路延迟。
        # 注意这部分延迟发生在真正读取盘口之前，所以会直接改变最终成交价。
        delay_ms = self._execution_delay_ms()
        if delay_ms > 0:
            self.sleeper(delay_ms / 1000.0)

        try:
            ws_enabled = bool(getattr(self.market_client, "has_ws_client", False))
            if ws_enabled:
                # 有 WS 时优先等 WS 盘口。
                # 只有 WS 在超时窗口内完全拿不到可执行深度时，才考虑退回 REST / Gamma。
                simulated = self._simulate_with_timeout(order, ws_only=True)
                if simulated[2] <= 0 and not simulated[6]:
                    live_book = self.market_client.get_order_book(order.token_id)
                    if live_book:
                        simulated = self._simulate_with_timeout(order)
                    else:
                        reference_market_price = self._lookup_market_price(order)
                        if reference_market_price > 0:
                            simulated = self._simulate_from_market_price(order, reference_market_price)
                        else:
                            simulated = self._simulate_with_timeout(order)
            else:
                live_book = self.market_client.get_order_book(order.token_id)
                if live_book:
                    simulated = self._simulate_with_timeout(order)
                else:
                    reference_market_price = self._lookup_market_price(order)
                    if reference_market_price > 0:
                        simulated = self._simulate_from_market_price(order, reference_market_price)
                    else:
                        simulated = self._simulate_with_timeout(order)
        except Exception as exc:
            return ExecutionResult(
                execution_id=str(uuid.uuid4()),
                order_id=order.order_id,
                mode="paper",
                status="rejected",
                token_id=order.token_id,
                side=order.side,
                requested_amount_usdc=order.requested_amount_usdc,
                requested_shares=order.requested_shares,
                executed_amount_usdc=0.0,
                executed_shares=0.0,
                avg_price=0.0,
                best_price=0.0,
                price_source="market-data-error",
                slippage_bps=0.0,
                leader_deviation_bps=0.0,
                reason=f"market data unavailable: {exc}",
            raw_response={"delay_ms": delay_ms, "error": str(exc)},
        )
        (
            best_price,
            executed_amount,
            executed_shares,
            avg_price,
            price_source,
            final_book,
            timed_out,
            poll_count,
        ) = simulated
        # 这里同时保留两条偏差口径：
        # 1. slippage_bps：相对我看到的 best price 偏了多少
        # 2. leader_deviation_bps：相对 leader 当时成交价偏了多少
        slippage_bps = _bps_delta(best_price, avg_price)
        leader_deviation_bps = _bps_delta(order.reference_price, avg_price)

        if self.simulation.exchange_ack_delay_ms > 0:
            self.sleeper(self.simulation.exchange_ack_delay_ms / 1000.0)

        if executed_shares <= 0:
            return ExecutionResult(
                execution_id=str(uuid.uuid4()),
                order_id=order.order_id,
                mode="paper",
                status="rejected",
                token_id=order.token_id,
                side=order.side,
                requested_amount_usdc=order.requested_amount_usdc,
                requested_shares=order.requested_shares,
                executed_amount_usdc=0.0,
                executed_shares=0.0,
                avg_price=0.0,
                best_price=best_price,
                price_source=price_source,
                slippage_bps=0.0,
                leader_deviation_bps=0.0,
                reason="timeout canceled without fill" if timed_out else "no executable liquidity",
                raw_response={
                    "delay_ms": delay_ms,
                    "timed_out": timed_out,
                    "poll_count": poll_count,
                    "book": final_book,
                },
            )

        if slippage_bps > self.risk.max_slippage_bps:
            return ExecutionResult(
                execution_id=str(uuid.uuid4()),
                order_id=order.order_id,
                mode="paper",
                status="rejected",
                token_id=order.token_id,
                side=order.side,
                requested_amount_usdc=order.requested_amount_usdc,
                requested_shares=order.requested_shares,
                executed_amount_usdc=0.0,
                executed_shares=0.0,
                avg_price=avg_price,
                best_price=best_price,
                price_source=price_source,
                slippage_bps=slippage_bps,
                leader_deviation_bps=leader_deviation_bps,
                reason="slippage protection triggered",
                raw_response={
                    "delay_ms": delay_ms,
                    "timed_out": timed_out,
                    "poll_count": poll_count,
                    "book": final_book,
                },
            )

        if self.risk.max_leader_price_deviation_bps > 0 and leader_deviation_bps > self.risk.max_leader_price_deviation_bps:
            return ExecutionResult(
                execution_id=str(uuid.uuid4()),
                order_id=order.order_id,
                mode="paper",
                status="rejected",
                token_id=order.token_id,
                side=order.side,
                requested_amount_usdc=order.requested_amount_usdc,
                requested_shares=order.requested_shares,
                executed_amount_usdc=0.0,
                executed_shares=0.0,
                avg_price=avg_price,
                best_price=best_price,
                price_source=price_source,
                slippage_bps=slippage_bps,
                leader_deviation_bps=leader_deviation_bps,
                reason="leader price deviation protection triggered",
                raw_response={
                    "delay_ms": delay_ms,
                    "timed_out": timed_out,
                    "poll_count": poll_count,
                    "book": final_book,
                },
            )

        status = "partial"
        if order.side == "BUY" and executed_amount >= order.requested_amount_usdc - 1e-9:
            status = "simulated"
        if order.side == "SELL" and executed_shares >= order.requested_shares - 1e-9:
            status = "simulated"

        reason = "paper fill"
        if status == "partial" and timed_out:
            reason = "partial fill; remainder canceled after timeout"

        return ExecutionResult(
            execution_id=str(uuid.uuid4()),
            order_id=order.order_id,
            mode="paper",
            status=status,
            token_id=order.token_id,
            side=order.side,
            requested_amount_usdc=order.requested_amount_usdc,
            requested_shares=order.requested_shares,
            executed_amount_usdc=executed_amount,
            executed_shares=executed_shares,
            avg_price=avg_price,
            best_price=best_price,
            price_source=price_source,
            slippage_bps=slippage_bps,
            leader_deviation_bps=leader_deviation_bps,
            reason=reason,
            raw_response={
                "delay_ms": delay_ms,
                "timed_out": timed_out,
                "poll_count": poll_count,
                "book": final_book,
            },
        )

    def _execution_delay_ms(self) -> int:
        if not self.simulation.enabled:
            return 0
        jitter = 0.0
        if self.simulation.latency_jitter_ms > 0:
            jitter = self.random_uniform(0, self.simulation.latency_jitter_ms)
        # 这里故意把监听、决策、提交三段延迟相加，而不是只模拟“网络耗时”。
        # 因为真实跟单里，最致命的往往不是单纯 RTT，而是整条发现到执行的总链路时间。
        return int(
            self.simulation.signal_detect_delay_ms
            + self.simulation.decision_delay_ms
            + self.simulation.submit_delay_ms
            + jitter
        )

    def _lookup_market_price(self, order: AggregatedOrder) -> float:
        if self.market_price_lookup is None:
            return 0.0
        try:
            value = self.market_price_lookup(order.market_slug, order.outcome, order.token_id)
        except Exception:
            return 0.0
        return _float(value, 0.0)

    def _simulate_from_market_price(
        self,
        order: AggregatedOrder,
        market_price: float,
    ) -> Tuple[float, float, float, float, str, Dict[str, Any], bool, int]:
        if market_price <= 0:
            return 0.0, 0.0, 0.0, 0.0, "gamma_outcome", {}, False, 0
        if order.side == "BUY":
            executed_amount = order.requested_amount_usdc
            executed_shares = executed_amount / market_price if market_price > 0 else 0.0
        else:
            executed_shares = order.requested_shares
            executed_amount = executed_shares * market_price
        return (
            market_price,
            executed_amount,
            executed_shares,
            market_price,
            "gamma_outcome",
            {"market_price": market_price},
            False,
            1,
        )

    def _simulate_with_timeout(
        self,
        order: AggregatedOrder,
        ws_only: bool = False,
    ) -> Tuple[float, float, float, float, str, Dict[str, Any], bool, int]:
        # 这是 paper 执行器的核心：
        # 在 fill_timeout 窗口内反复读取盘口，按买卖方向去吃深度，直到全成、部分成、或超时为止。
        requested_metric = order.requested_amount_usdc if order.side == "BUY" else order.requested_shares
        if requested_metric <= 0:
            return 0.0, 0.0, 0.0, 0.0, "empty-request", {}, False, 0

        executed_amount = 0.0
        executed_shares = 0.0
        best_price = 0.0
        latest_book: Dict[str, Any] = {}
        last_signature = ""
        accepted_price_bound = 0.0
        poll_count = 0
        timed_out = False

        start = self.monotonic()
        timeout_sec = max(0.0, self.simulation.fill_timeout_ms / 1000.0)
        poll_interval_sec = max(0.01, self.simulation.poll_interval_ms / 1000.0)

        while True:
            latest_book = self._get_book(order.token_id, ws_only=ws_only)
            signature = self._book_signature(latest_book)
            book_source = str(latest_book.get("_source", "")).strip()
            if book_source == "ws_market":
                price_source = "ws_orderbook"
            elif ws_only:
                price_source = "ws_orderbook"
            else:
                price_source = "delayed_orderbook" if self.simulation.use_delayed_book_snapshot else "orderbook"

            if signature != last_signature or not signature:
                # 只有盘口真的变了，才重新消费一次快照，避免同一深度被重复吃多次。
                fill = self._consume_snapshot(
                    order=order,
                    book=latest_book,
                    remaining_amount=max(0.0, order.requested_amount_usdc - executed_amount),
                    remaining_shares=max(0.0, order.requested_shares - executed_shares),
                    accepted_price_bound=accepted_price_bound,
                )
                snapshot_best, add_amount, add_shares, snapshot_avg, accepted_price_bound = fill
                if best_price <= 0 and snapshot_best > 0:
                    best_price = snapshot_best
                if add_shares > 0:
                    executed_amount += add_amount
                    executed_shares += add_shares
                last_signature = signature

            poll_count += 1
            if order.side == "BUY" and executed_amount >= order.requested_amount_usdc - 1e-9:
                break
            if order.side == "SELL" and executed_shares >= order.requested_shares - 1e-9:
                break

            now = self.monotonic()
            if timeout_sec <= 0 or now - start >= timeout_sec:
                timed_out = True
                break
            self.sleeper(min(poll_interval_sec, timeout_sec - (now - start)))

        if executed_shares <= 0:
            return best_price, 0.0, 0.0, 0.0, price_source, latest_book, timed_out, poll_count

        if order.side == "BUY" and not self.simulation.allow_partial_fill and executed_amount < order.requested_amount_usdc - 1e-9:
            return best_price, 0.0, 0.0, 0.0, price_source, latest_book, True, poll_count
        if order.side == "SELL" and not self.simulation.allow_partial_fill and executed_shares < order.requested_shares - 1e-9:
            return best_price, 0.0, 0.0, 0.0, price_source, latest_book, True, poll_count

        avg_price = executed_amount / executed_shares if executed_shares > 0 else 0.0
        return best_price, executed_amount, executed_shares, avg_price, price_source, latest_book, timed_out, poll_count

    def _get_book(self, token_id: str, ws_only: bool = False) -> Dict[str, Any]:
        if ws_only:
            getter = getattr(self.market_client, "get_ws_order_book", None)
            if callable(getter):
                return getter(token_id)
        return self.market_client.get_order_book(token_id)

    def _consume_snapshot(
        self,
        order: AggregatedOrder,
        book: Dict[str, Any],
        remaining_amount: float,
        remaining_shares: float,
        accepted_price_bound: float,
    ) -> Tuple[float, float, float, float, float]:
        if order.side == "BUY":
            return self._simulate_buy(book, remaining_amount, order.reference_price, accepted_price_bound)
        return self._simulate_sell(book, remaining_shares, order.reference_price, accepted_price_bound)

    def _simulate_buy(
        self,
        book: Dict[str, Any],
        amount_usdc: float,
        reference_price: float,
        accepted_price_bound: float = 0.0,
    ) -> Tuple[float, float, float, float, float]:
        asks = self._levels(book.get("asks"), reverse=False)
        if not asks:
            return 0.0, 0.0, 0.0, 0.0, accepted_price_bound

        best_ask = asks[0][0]
        max_price = accepted_price_bound
        if max_price <= 0:
            # BUY 的价格上限从当前 best ask 推出来，而不是从 leader 成交价直接推。
            # 这表示我们约束的是“我当下看到的最优卖一被容忍偏离多少”。
            max_price = best_ask * (1 + self.risk.max_slippage_bps / 10000.0)
        remaining = amount_usdc
        spent = 0.0
        shares = 0.0
        for price, level_shares in asks:
            if remaining <= 1e-9:
                break
            if price > max_price:
                break
            level_cost = price * level_shares
            if remaining >= level_cost:
                spent += level_cost
                shares += level_shares
                remaining -= level_cost
            else:
                partial_shares = remaining / price if price > 0 else 0.0
                spent += remaining
                shares += partial_shares
                remaining = 0.0
        avg_price = spent / shares if shares > 0 else 0.0
        return best_ask, spent, shares, avg_price, max_price

    def _simulate_sell(
        self,
        book: Dict[str, Any],
        shares_to_sell: float,
        reference_price: float,
        accepted_price_bound: float = 0.0,
    ) -> Tuple[float, float, float, float, float]:
        bids = self._levels(book.get("bids"), reverse=True)
        if not bids:
            return 0.0, 0.0, 0.0, 0.0, accepted_price_bound

        best_bid = bids[0][0]
        min_price = accepted_price_bound
        if min_price <= 0:
            # SELL 和 BUY 对称：从 best bid 反推一个我能接受的最低卖价。
            min_price = best_bid * (1 - self.risk.max_slippage_bps / 10000.0)
        remaining = shares_to_sell
        notional = 0.0
        shares = 0.0
        for price, level_shares in bids:
            if remaining <= 1e-9:
                break
            if price < min_price:
                break
            filled = min(level_shares, remaining)
            notional += filled * price
            shares += filled
            remaining -= filled
        avg_price = notional / shares if shares > 0 else 0.0
        return best_bid, notional, shares, avg_price, min_price

    def _levels(self, raw_levels: Any, reverse: bool = False) -> List[Tuple[float, float]]:
        levels: List[Tuple[float, float]] = []
        if not isinstance(raw_levels, list):
            return levels
        for item in raw_levels:
            if not isinstance(item, dict):
                continue
            price = _float(item.get("price"), 0.0)
            size = _float(item.get("size"), 0.0)
            if price > 0 and size > 0:
                levels.append((price, size))
        levels.sort(key=lambda item: item[0], reverse=reverse)
        return levels

    def _book_signature(self, book: Dict[str, Any]) -> str:
        if not isinstance(book, dict):
            return ""
        for key in ("hash", "timestamp"):
            value = book.get(key)
            if value:
                return str(value)
        # 某些来源没有显式 hash/timestamp 时，退回到前几档盘口内容做指纹。
        # 这样最少可以避免同一份静态快照被重复消费。
        bids = tuple((str(item.get("price")), str(item.get("size"))) for item in book.get("bids", [])[:8] if isinstance(item, dict))
        asks = tuple((str(item.get("price")), str(item.get("size"))) for item in book.get("asks", [])[:8] if isinstance(item, dict))
        if not bids and not asks:
            return ""
        return str((bids, asks))


class LiveExecutor:
    def __init__(self, market_client: ClobReadOnlyClient, wallet: WalletConfig, risk: RiskConfig):
        self.market_client = market_client
        self.wallet = wallet
        self.risk = risk
        self.client = self._build_client()
        self.paper_preflight = PaperExecutor(market_client, risk)

    def _build_client(self):
        try:
            from py_clob_client.client import ClobClient
            from py_clob_client.clob_types import ApiCreds
        except Exception as exc:  # pragma: no cover - optional dependency
            raise RuntimeError(
                "Live trading requires optional dependency py-clob-client. Install requirements-live.txt first."
            ) from exc

        private_key = self.wallet.private_key()
        funder = self.wallet.funder()
        if not private_key or not funder:
            raise RuntimeError("Missing live trading credentials in environment variables.")

        client = ClobClient(
            self.wallet.clob_host,
            key=private_key,
            chain_id=self.wallet.chain_id,
            signature_type=self.wallet.signature_type,
            funder=funder,
        )
        creds = self.wallet.api_creds()
        if creds["api_key"] and creds["api_secret"] and creds["api_passphrase"]:
            client.set_api_creds(
                ApiCreds(
                    api_key=creds["api_key"],
                    api_secret=creds["api_secret"],
                    api_passphrase=creds["api_passphrase"],
                )
            )
        else:  # pragma: no cover - external service
            client.set_api_creds(client.create_or_derive_api_creds())
        return client

    def execute(self, order: AggregatedOrder) -> ExecutionResult:
        preflight = self.paper_preflight.execute(order)
        if preflight.status == "rejected":
            preflight.mode = "live"
            return preflight

        try:
            from py_clob_client.clob_types import MarketOrderArgs, OrderType
        except Exception as exc:  # pragma: no cover - optional dependency
            raise RuntimeError("py-clob-client is unavailable for live execution.") from exc

        amount = order.requested_amount_usdc if order.side == "BUY" else order.requested_shares
        args = MarketOrderArgs(
            token_id=order.token_id,
            amount=amount,
            side=order.side,
            price=preflight.avg_price,
            order_type=OrderType.FOK,
        )

        try:  # pragma: no cover - external service
            signed = self.client.create_market_order(args)
            response = self.client.post_order(signed, OrderType.FOK)
        except Exception as exc:
            return ExecutionResult(
                execution_id=str(uuid.uuid4()),
                order_id=order.order_id,
                mode="live",
                status="rejected",
                token_id=order.token_id,
                side=order.side,
                requested_amount_usdc=order.requested_amount_usdc,
                requested_shares=order.requested_shares,
                executed_amount_usdc=0.0,
                executed_shares=0.0,
                avg_price=preflight.avg_price,
                best_price=preflight.best_price,
                price_source=preflight.price_source,
                slippage_bps=preflight.slippage_bps,
                leader_deviation_bps=preflight.leader_deviation_bps,
                reason=str(exc),
            )

        parsed = self._parse_live_response(response, preflight)
        parsed.order_id = order.order_id
        parsed.mode = "live"
        return parsed

    def _parse_live_response(self, payload: Dict[str, Any], fallback: ExecutionResult) -> ExecutionResult:
        status = "submitted"
        if payload.get("error"):
            status = "rejected"
        executed_shares = _float(
            payload.get("executed_size")
            or payload.get("size_matched")
            or payload.get("sizeMatched")
            or payload.get("filled_size"),
            0.0,
        )
        executed_amount = _float(
            payload.get("executed_amount")
            or payload.get("usdc")
            or payload.get("amount_matched")
            or payload.get("amountMatched"),
            0.0,
        )
        avg_price = _float(payload.get("avg_price") or payload.get("price"), fallback.avg_price)

        if executed_shares > 0:
            status = "executed"
            if executed_amount <= 0:
                executed_amount = executed_shares * avg_price

        return ExecutionResult(
            execution_id=str(uuid.uuid4()),
            order_id="",
            mode="live",
            status=status,
            token_id=str(payload.get("asset_id") or payload.get("tokenId") or fallback.token_id),
            side=str(payload.get("side") or fallback.side),
            requested_amount_usdc=fallback.requested_amount_usdc,
            requested_shares=fallback.requested_shares,
            executed_amount_usdc=executed_amount,
            executed_shares=executed_shares,
            avg_price=avg_price,
            best_price=fallback.best_price,
            price_source=fallback.price_source,
            slippage_bps=fallback.slippage_bps,
            leader_deviation_bps=fallback.leader_deviation_bps,
            reason="live order submitted" if status == "submitted" else "",
            raw_response=payload if isinstance(payload, dict) else {"payload": payload},
            executed_at=utc_now(),
        )


def build_executor(
    wallet: WalletConfig,
    risk: RiskConfig,
    paper_simulation: PaperSimulationConfig | None = None,
    market_price_lookup=None,
    timeout_sec: int = 10,
    proxies: Dict[str, str] | None = None,
    ws_client=None,
):
    market_client = ClobReadOnlyClient(wallet.clob_host, timeout_sec=timeout_sec, proxies=proxies, ws_client=ws_client)
    if wallet.mode == "live":
        return LiveExecutor(market_client, wallet, risk)
    return PaperExecutor(
        market_client,
        risk,
        simulation=paper_simulation,
        market_price_lookup=market_price_lookup,
    )
