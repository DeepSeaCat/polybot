from __future__ import annotations

from datetime import timedelta

from src.polybot.copybot_models import (
    AggregatedOrder,
    ExecutionResult,
    LeaderTradeEvent,
    PaperSimulationConfig,
    RuntimeStateSnapshot,
    TraderConfig,
    to_primitive,
    utc_now,
)
from src.polybot.copybot_dashboard import build_dashboard_payload
from src.polybot.copybot_execution import PaperExecutor
from src.polybot.copybot_market_ws import MarketWebSocketClient
from src.polybot.copybot_services import CopyIntentFactory, LeaderPollingSource, OrderAggregator, PositionTracker, RiskManager
from src.polybot.copybot_storage import InMemoryRepository
from src.polybot.copybot_models import MultiplierTier, RiskConfig, SourceIntent


class DummyLeaderEquityService:
    def __init__(self, equities):
        self.equities = equities

    def get_equity(self, trader_cfg):
        return self.equities.get(trader_cfg.address, 0.0)


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def sleep(self, seconds: float):
        self.now += seconds

    def monotonic(self) -> float:
        return self.now


class DummyMarketClient:
    def __init__(self, books):
        self.books = list(books)
        self.index = 0

    def get_order_book(self, token_id: str):
        if self.index < len(self.books):
            book = self.books[self.index]
            self.index += 1
            return book
        return self.books[-1] if self.books else {}


class DummyWsMarketClient:
    def __init__(self, book):
        self.book = book
        self.has_ws_client = True

    def get_order_book(self, token_id: str):
        return dict(self.book)

    def get_ws_order_book(self, token_id: str):
        return dict(self.book)

    def get_mark_price(self, token_id: str):
        best_bid = float(self.book.get("best_bid", 0.0) or 0.0)
        best_ask = float(self.book.get("best_ask", 0.0) or 0.0)
        if best_bid > 0 and best_ask > 0:
            return (best_bid + best_ask) / 2.0
        return 0.0


class DummyDelayedWsMarketClient:
    has_ws_client = True

    def __init__(self, ws_books, rest_book=None):
        self.ws_books = list(ws_books)
        self.rest_book = dict(rest_book or {})
        self.index = 0

    def get_ws_order_book(self, token_id: str):
        if self.index < len(self.ws_books):
            book = self.ws_books[self.index]
            self.index += 1
            return dict(book)
        return dict(self.ws_books[-1]) if self.ws_books else {}

    def get_order_book(self, token_id: str):
        book = self.get_ws_order_book(token_id)
        if book:
            return book
        return dict(self.rest_book)


class ExplodingMarketClient:
    def get_order_book(self, token_id: str):
        raise RuntimeError("simulated order book failure")


class FakeSignalClient:
    def __init__(self, activity_rows=None, trade_rows=None):
        self.activity_rows = list(activity_rows or [])
        self.trade_rows = list(trade_rows or [])
        self.calls = []

    def get_user_activity(self, address: str, limit: int = 500):
        self.calls.append(("activity", address, limit))
        return list(self.activity_rows)

    def get_user_trades(self, address: str, limit: int = 2000):
        self.calls.append(("trades", address, limit))
        return list(self.trade_rows)


def trader(address: str, ratio: float = 0.1, max_alloc: float = 100.0) -> TraderConfig:
    return TraderConfig(
        address=address,
        label=address[-4:],
        copy_ratio=ratio,
        max_open_allocation_usdc=max_alloc,
        multiplier_tiers=[MultiplierTier(min_leader_notional_usdc=0, multiplier=1.0)],
    )


def test_buy_intent_uses_ratio_and_multiplier():
    trader_cfg = TraderConfig(
        address="0xabc",
        label="alpha",
        copy_ratio=0.1,
        max_open_allocation_usdc=200,
        multiplier_tiers=[
            MultiplierTier(min_leader_notional_usdc=0, multiplier=1.0),
            MultiplierTier(min_leader_notional_usdc=100, multiplier=1.5),
        ],
    )
    tracker = PositionTracker(InMemoryRepository(), RuntimeStateSnapshot(cash_balance_usdc=500), 500)
    factory = CopyIntentFactory({"0xabc": trader_cfg}, tracker)
    event = LeaderTradeEvent(
        event_id="evt1",
        trader_address="0xabc",
        trader_label="alpha",
        tx_hash="tx1",
        token_id="token1",
        condition_id="cond1",
        market_slug="market-1",
        market_title="Market 1",
        outcome="Yes",
        side="BUY",
        price=0.5,
        leader_shares=400,
        leader_notional_usdc=200,
        observed_at=utc_now(),
    )
    intents = factory.build([event])
    assert len(intents) == 1
    assert round(intents[0].requested_amount_usdc, 6) == 30.0
    assert round(intents[0].requested_shares, 6) == 60.0


def test_buy_intent_can_follow_leader_fraction_of_equity():
    trader_cfg = TraderConfig(
        address="0xabc",
        label="alpha",
        sizing_mode="leader_fraction_of_equity",
        copy_ratio=1.0,
        max_open_allocation_usdc=200,
        multiplier_tiers=[MultiplierTier(min_leader_notional_usdc=0, multiplier=1.0)],
    )
    tracker = PositionTracker(InMemoryRepository(), RuntimeStateSnapshot(cash_balance_usdc=500), 500)
    factory = CopyIntentFactory(
        {"0xabc": trader_cfg},
        tracker,
        leader_equity_service=DummyLeaderEquityService({"0xabc": 1000.0}),
    )
    event = LeaderTradeEvent(
        event_id="evt2",
        trader_address="0xabc",
        trader_label="alpha",
        tx_hash="tx2",
        token_id="token1",
        condition_id="cond1",
        market_slug="market-1",
        market_title="Market 1",
        outcome="Yes",
        side="BUY",
        price=0.5,
        leader_shares=400,
        leader_notional_usdc=200,
        observed_at=utc_now(),
    )
    intents = factory.build([event])
    assert len(intents) == 1
    assert round(intents[0].requested_amount_usdc, 6) == 100.0
    assert round(intents[0].requested_shares, 6) == 200.0


def test_sell_plan_tracks_shadow_position_proportionally():
    repository = InMemoryRepository()
    tracker = PositionTracker(repository, RuntimeStateSnapshot(cash_balance_usdc=1000), 1000)

    buy_component = SourceIntent(
        intent_id="buy1",
        trader_address="0xabc",
        trader_label="alpha",
        token_id="token1",
        condition_id="cond1",
        market_slug="market-1",
        market_title="Market 1",
        outcome="Yes",
        side="BUY",
        leader_event_id="evt-buy",
        leader_tx_hash="tx-buy",
        leader_reference_price=0.5,
        leader_shares=100.0,
        leader_notional_usdc=50.0,
        requested_amount_usdc=10.0,
        requested_shares=20.0,
        multiplier=1.0,
        copy_ratio=0.2,
    )
    order = AggregatedOrder(
        order_id="order-buy",
        token_id="token1",
        side="BUY",
        requested_amount_usdc=10.0,
        requested_shares=20.0,
        reference_price=0.5,
        market_slug="market-1",
        market_title="Market 1",
        outcome="Yes",
        components=[buy_component],
    )
    execution = ExecutionResult(
        execution_id="exec-buy",
        order_id="order-buy",
        mode="paper",
        status="simulated",
        token_id="token1",
        side="BUY",
        requested_amount_usdc=10.0,
        requested_shares=20.0,
        executed_amount_usdc=10.0,
        executed_shares=20.0,
        avg_price=0.5,
        best_price=0.5,
        price_source="test",
        slippage_bps=0.0,
        leader_deviation_bps=0.0,
    )
    tracker.apply_execution(order, execution)

    close_plan = tracker.plan_close("0xabc", "token1", leader_shares_to_close=25.0)
    assert len(close_plan) == 1
    assert round(close_plan[0].follower_shares, 6) == 5.0


def test_aggregator_merges_same_token_and_side():
    aggregator = OrderAggregator(window_sec=0)
    now = utc_now()
    intents = [
        SourceIntent(
            intent_id="i1",
            trader_address="0x1",
            trader_label="one",
            token_id="token1",
            condition_id="cond1",
            market_slug="market-1",
            market_title="Market 1",
            outcome="Yes",
            side="BUY",
            leader_event_id="e1",
            leader_tx_hash="tx1",
            leader_reference_price=0.5,
            leader_shares=50,
            leader_notional_usdc=25,
            requested_amount_usdc=10,
            requested_shares=20,
            multiplier=1,
            copy_ratio=0.2,
            created_at=now,
        ),
        SourceIntent(
            intent_id="i2",
            trader_address="0x2",
            trader_label="two",
            token_id="token1",
            condition_id="cond1",
            market_slug="market-1",
            market_title="Market 1",
            outcome="Yes",
            side="BUY",
            leader_event_id="e2",
            leader_tx_hash="tx2",
            leader_reference_price=0.55,
            leader_shares=30,
            leader_notional_usdc=16.5,
            requested_amount_usdc=6,
            requested_shares=10.9090909,
            multiplier=1,
            copy_ratio=0.2,
            created_at=now + timedelta(seconds=1),
        ),
    ]
    aggregator.add(intents)
    ready = aggregator.flush_ready(force=True)
    assert len(ready) == 1
    assert round(ready[0].requested_amount_usdc, 6) == 16.0
    assert len(ready[0].components) == 2


def test_dashboard_builds_per_order_difference_rows():
    repository = InMemoryRepository()
    observed_at = utc_now()
    leader_event = LeaderTradeEvent(
        event_id="evt-diff",
        trader_address="0xabc",
        trader_label="alpha",
        tx_hash="tx-diff",
        token_id="token1",
        condition_id="cond1",
        market_slug="market-1",
        market_title="Market 1",
        outcome="Yes",
        side="BUY",
        price=0.4,
        leader_shares=10.0,
        leader_notional_usdc=4.0,
        observed_at=observed_at,
    )
    repository.record_leader_events([to_primitive(leader_event)])

    component = SourceIntent(
        intent_id="intent-diff",
        trader_address="0xabc",
        trader_label="alpha",
        token_id="token1",
        condition_id="cond1",
        market_slug="market-1",
        market_title="Market 1",
        outcome="Yes",
        side="BUY",
        leader_event_id="evt-diff",
        leader_tx_hash="tx-diff",
        leader_reference_price=0.4,
        leader_shares=10.0,
        leader_notional_usdc=4.0,
        requested_amount_usdc=1.0,
        requested_shares=2.5,
        multiplier=1.0,
        copy_ratio=1.0,
        created_at=observed_at + timedelta(seconds=1),
    )
    order = AggregatedOrder(
        order_id="order-diff",
        token_id="token1",
        side="BUY",
        requested_amount_usdc=1.0,
        requested_shares=2.5,
        reference_price=0.4,
        market_slug="market-1",
        market_title="Market 1",
        outcome="Yes",
        components=[component],
        created_at=observed_at + timedelta(seconds=1),
        last_updated_at=observed_at + timedelta(seconds=1),
    )
    execution = ExecutionResult(
        execution_id="exec-diff",
        order_id="order-diff",
        mode="paper",
        status="simulated",
        token_id="token1",
        side="BUY",
        requested_amount_usdc=1.0,
        requested_shares=2.5,
        executed_amount_usdc=1.0,
        executed_shares=2.0,
        avg_price=0.5,
        best_price=0.5,
        price_source="gamma_outcome",
        slippage_bps=0.0,
        leader_deviation_bps=2500.0,
        reason="paper fill",
        raw_response={"delay_ms": 123},
        executed_at=observed_at + timedelta(seconds=2),
    )

    repository.record_order(to_primitive(order), status="pending")
    repository.update_order_status("order-diff", status="simulated", reason="paper fill", execution_id="exec-diff")
    repository.record_execution(to_primitive(execution))

    payload = build_dashboard_payload(repository)
    row = payload["order_differences"][0]
    assert row["leader_price"] == 0.4
    assert row["follower_fill_price"] == 0.5
    assert round(row["execution_gap_bps"], 6) == 2500.0
    assert row["signal_to_fill_ms"] == 2000
    assert row["status"] == "simulated"


def test_market_ws_client_updates_book_and_mark_price():
    client = MarketWebSocketClient("wss://example.com/ws/market")
    client.apply_payload(
        {
            "event_type": "book",
            "asset_id": "token-1",
            "market": "market-1",
            "bids": [{"price": "0.44", "size": "10"}],
            "asks": [{"price": "0.46", "size": "8"}],
            "timestamp": "2026-03-26T00:00:00Z",
            "hash": "hash-1",
        }
    )
    client.apply_payload(
        {
            "event_type": "last_trade_price",
            "asset_id": "token-1",
            "price": "0.45",
            "timestamp": "2026-03-26T00:00:01Z",
        }
    )
    book = client.get_order_book("token-1")
    assert book["asset_id"] == "token-1"
    assert float(book["bids"][0]["price"]) == 0.44
    assert float(book["asks"][0]["price"]) == 0.46
    assert book["_source"] == "ws_market"
    assert round(client.get_mark_price("token-1"), 6) == 0.45


def test_market_ws_client_caps_corrected_best_price_to_visible_top_size():
    client = MarketWebSocketClient("wss://example.com/ws/market")
    client.apply_payload(
        {
            "asset_id": "token-1",
            "market": "market-1",
            "bids": [{"price": "0.20", "size": "15"}],
            "asks": [{"price": "0.99", "size": "7"}],
            "timestamp": "2026-03-26T00:00:00Z",
            "hash": "hash-1",
        }
    )
    client.apply_payload(
        {
            "event_type": "best_bid_ask",
            "asset_id": "token-1",
            "best_bid": "0.20",
            "best_ask": "0.01",
            "timestamp": "2026-03-26T00:00:01Z",
        }
    )
    book = client.get_order_book("token-1")
    assert float(book["asks"][0]["price"]) == 0.01
    assert float(book["asks"][0]["size"]) == 7.0


def test_paper_executor_prefers_ws_orderbook_when_available():
    clock = FakeClock()
    market = DummyWsMarketClient(
        {
            "_source": "ws_market",
            "hash": "ws-book-1",
            "best_bid": 0.44,
            "best_ask": 0.46,
            "asks": [{"price": "0.46", "size": "100"}],
            "bids": [{"price": "0.44", "size": "100"}],
        }
    )
    executor = PaperExecutor(
        market,
        RiskConfig(max_slippage_bps=10000),
        simulation=PaperSimulationConfig(
            enabled=True,
            signal_detect_delay_ms=0,
            decision_delay_ms=0,
            submit_delay_ms=0,
            exchange_ack_delay_ms=0,
            latency_jitter_ms=0,
            fill_timeout_ms=50,
            poll_interval_ms=25,
            allow_partial_fill=True,
            use_delayed_book_snapshot=True,
        ),
        sleeper=clock.sleep,
        monotonic=clock.monotonic,
    )
    order = AggregatedOrder(
        order_id="paper-ws-book",
        token_id="token-1",
        side="BUY",
        requested_amount_usdc=4.6,
        requested_shares=10.0,
        reference_price=0.46,
        market_slug="market-1",
        market_title="Market 1",
        outcome="Yes",
        components=[],
    )
    result = executor.execute(order)
    assert result.status == "simulated"
    assert round(result.avg_price, 6) == 0.46
    assert result.price_source == "ws_orderbook"


def test_paper_executor_waits_for_ws_before_falling_back_to_market_price():
    clock = FakeClock()
    market = DummyDelayedWsMarketClient(
        [
            {},
            {},
            {
                "_source": "ws_market",
                "hash": "ws-book-2",
                "best_bid": 0.44,
                "best_ask": 0.46,
                "asks": [{"price": "0.46", "size": "100"}],
                "bids": [{"price": "0.44", "size": "100"}],
            },
        ]
    )
    executor = PaperExecutor(
        market,
        RiskConfig(max_slippage_bps=10000),
        simulation=PaperSimulationConfig(
            enabled=True,
            signal_detect_delay_ms=0,
            decision_delay_ms=0,
            submit_delay_ms=0,
            exchange_ack_delay_ms=0,
            latency_jitter_ms=0,
            fill_timeout_ms=100,
            poll_interval_ms=25,
            allow_partial_fill=True,
            use_delayed_book_snapshot=True,
        ),
        market_price_lookup=lambda slug, outcome, token_id: 0.315,
        sleeper=clock.sleep,
        monotonic=clock.monotonic,
    )
    order = AggregatedOrder(
        order_id="paper-ws-late-book",
        token_id="token-1",
        side="BUY",
        requested_amount_usdc=4.6,
        requested_shares=10.0,
        reference_price=0.46,
        market_slug="market-1",
        market_title="Market 1",
        outcome="Yes",
        components=[],
    )
    result = executor.execute(order)
    assert result.status == "simulated"
    assert round(result.avg_price, 6) == 0.46
    assert result.price_source == "ws_orderbook"


def test_risk_rejects_trader_exposure_breach():
    repository = InMemoryRepository()
    state = RuntimeStateSnapshot(cash_balance_usdc=200)
    tracker = PositionTracker(repository, state, 200)
    risk = RiskManager(
        RiskConfig(
            min_order_usdc=5,
            max_order_usdc=200,
            max_total_exposure_usdc=500,
            max_market_exposure_usdc=500,
            max_trader_exposure_usdc=80,
            max_daily_loss_usdc=100,
            max_open_positions=5,
        ),
        tracker,
        {"0xabc": trader("0xabc", ratio=0.1, max_alloc=80)},
    )
    component = SourceIntent(
        intent_id="risk1",
        trader_address="0xabc",
        trader_label="alpha",
        token_id="token1",
        condition_id="cond1",
        market_slug="market-1",
        market_title="Market 1",
        outcome="Yes",
        side="BUY",
        leader_event_id="evt1",
        leader_tx_hash="tx1",
        leader_reference_price=0.5,
        leader_shares=500,
        leader_notional_usdc=250,
        requested_amount_usdc=90,
        requested_shares=180,
        multiplier=1,
        copy_ratio=0.1,
    )
    order = AggregatedOrder(
        order_id="order-risk",
        token_id="token1",
        side="BUY",
        requested_amount_usdc=90,
        requested_shares=180,
        reference_price=0.5,
        market_slug="market-1",
        market_title="Market 1",
        outcome="Yes",
        components=[component],
    )
    allowed, reason = risk.evaluate(order)
    assert not allowed
    assert "trader exposure limit" in reason


def test_paper_executor_supports_partial_fill_and_timeout_cancel():
    clock = FakeClock()
    market = DummyMarketClient(
        [
            {
                "hash": "book-1",
                "asks": [{"price": "1.00", "size": "3"}],
                "bids": [],
            },
            {
                "hash": "book-1",
                "asks": [{"price": "1.00", "size": "3"}],
                "bids": [],
            },
            {
                "hash": "book-2",
                "asks": [{"price": "1.00", "size": "2"}],
                "bids": [],
            },
        ]
    )
    executor = PaperExecutor(
        market,
        RiskConfig(max_slippage_bps=200),
        simulation=PaperSimulationConfig(
            enabled=True,
            signal_detect_delay_ms=100,
            decision_delay_ms=50,
            submit_delay_ms=50,
            exchange_ack_delay_ms=100,
            latency_jitter_ms=0,
            fill_timeout_ms=250,
            poll_interval_ms=100,
            allow_partial_fill=True,
            use_delayed_book_snapshot=True,
        ),
        sleeper=clock.sleep,
        monotonic=clock.monotonic,
    )
    order = AggregatedOrder(
        order_id="paper-partial",
        token_id="token-1",
        side="BUY",
        requested_amount_usdc=10.0,
        requested_shares=10.0,
        reference_price=1.0,
        market_slug="market-1",
        market_title="Market 1",
        outcome="Yes",
        components=[],
    )
    result = executor.execute(order)
    assert result.status == "partial"
    assert round(result.executed_amount_usdc, 6) == 5.0
    assert round(result.executed_shares, 6) == 5.0
    assert "remainder canceled" in result.reason
    assert result.raw_response["timed_out"] is True
    assert result.raw_response["delay_ms"] == 200


def test_paper_executor_can_cancel_entire_order_when_partial_fills_disabled():
    clock = FakeClock()
    market = DummyMarketClient(
        [
            {
                "hash": "book-1",
                "asks": [{"price": "1.00", "size": "2"}],
                "bids": [],
            }
        ]
    )
    executor = PaperExecutor(
        market,
        RiskConfig(max_slippage_bps=200),
        simulation=PaperSimulationConfig(
            enabled=True,
            signal_detect_delay_ms=0,
            decision_delay_ms=0,
            submit_delay_ms=0,
            exchange_ack_delay_ms=0,
            latency_jitter_ms=0,
            fill_timeout_ms=0,
            poll_interval_ms=100,
            allow_partial_fill=False,
            use_delayed_book_snapshot=True,
        ),
        sleeper=clock.sleep,
        monotonic=clock.monotonic,
    )
    order = AggregatedOrder(
        order_id="paper-fok",
        token_id="token-1",
        side="BUY",
        requested_amount_usdc=5.0,
        requested_shares=5.0,
        reference_price=1.0,
        market_slug="market-1",
        market_title="Market 1",
        outcome="Yes",
        components=[],
    )
    result = executor.execute(order)
    assert result.status == "rejected"
    assert result.executed_amount_usdc == 0.0
    assert result.executed_shares == 0.0
    assert "timeout canceled" in result.reason


def test_paper_executor_rejects_gracefully_when_market_data_errors():
    clock = FakeClock()
    executor = PaperExecutor(
        ExplodingMarketClient(),
        RiskConfig(max_slippage_bps=200),
        simulation=PaperSimulationConfig(
            enabled=True,
            signal_detect_delay_ms=50,
            decision_delay_ms=0,
            submit_delay_ms=0,
            exchange_ack_delay_ms=0,
            latency_jitter_ms=0,
            fill_timeout_ms=200,
            poll_interval_ms=100,
            allow_partial_fill=True,
            use_delayed_book_snapshot=True,
        ),
        sleeper=clock.sleep,
        monotonic=clock.monotonic,
    )
    order = AggregatedOrder(
        order_id="paper-error",
        token_id="token-1",
        side="BUY",
        requested_amount_usdc=5.0,
        requested_shares=5.0,
        reference_price=1.0,
        market_slug="market-1",
        market_title="Market 1",
        outcome="Yes",
        components=[],
    )
    result = executor.execute(order)
    assert result.status == "rejected"
    assert result.price_source == "market-data-error"
    assert "market data unavailable" in result.reason


def test_paper_executor_uses_best_ask_even_when_book_is_descending():
    clock = FakeClock()
    market = DummyMarketClient(
        [
            {
                "hash": "book-desc-ask",
                "asks": [
                    {"price": "0.99", "size": "100"},
                    {"price": "0.70", "size": "100"},
                    {"price": "0.35", "size": "100"},
                ],
                "bids": [],
            }
        ]
    )
    executor = PaperExecutor(
        market,
        RiskConfig(max_slippage_bps=10000),
        simulation=PaperSimulationConfig(
            enabled=True,
            signal_detect_delay_ms=0,
            decision_delay_ms=0,
            submit_delay_ms=0,
            exchange_ack_delay_ms=0,
            latency_jitter_ms=0,
            fill_timeout_ms=50,
            poll_interval_ms=25,
            allow_partial_fill=True,
            use_delayed_book_snapshot=True,
        ),
        sleeper=clock.sleep,
        monotonic=clock.monotonic,
    )
    order = AggregatedOrder(
        order_id="paper-best-ask",
        token_id="token-1",
        side="BUY",
        requested_amount_usdc=3.5,
        requested_shares=10.0,
        reference_price=0.35,
        market_slug="market-1",
        market_title="Market 1",
        outcome="Yes",
        components=[],
    )
    result = executor.execute(order)
    assert result.status == "simulated"
    assert round(result.avg_price, 6) == 0.35
    assert round(result.executed_shares, 6) == 10.0


def test_paper_executor_uses_best_bid_even_when_book_is_ascending():
    clock = FakeClock()
    market = DummyMarketClient(
        [
            {
                "hash": "book-asc-bid",
                "asks": [],
                "bids": [
                    {"price": "0.01", "size": "100"},
                    {"price": "0.40", "size": "100"},
                    {"price": "0.85", "size": "100"},
                ],
            }
        ]
    )
    executor = PaperExecutor(
        market,
        RiskConfig(max_slippage_bps=10000),
        simulation=PaperSimulationConfig(
            enabled=True,
            signal_detect_delay_ms=0,
            decision_delay_ms=0,
            submit_delay_ms=0,
            exchange_ack_delay_ms=0,
            latency_jitter_ms=0,
            fill_timeout_ms=50,
            poll_interval_ms=25,
            allow_partial_fill=True,
            use_delayed_book_snapshot=True,
        ),
        sleeper=clock.sleep,
        monotonic=clock.monotonic,
    )
    order = AggregatedOrder(
        order_id="paper-best-bid",
        token_id="token-1",
        side="SELL",
        requested_amount_usdc=8.5,
        requested_shares=10.0,
        reference_price=0.85,
        market_slug="market-1",
        market_title="Market 1",
        outcome="Yes",
        components=[],
    )
    result = executor.execute(order)
    assert result.status == "simulated"
    assert round(result.avg_price, 6) == 0.85
    assert round(result.executed_amount_usdc, 6) == 8.5


def test_paper_executor_prefers_market_price_lookup_when_available():
    clock = FakeClock()
    market = DummyMarketClient([])
    executor = PaperExecutor(
        market,
        RiskConfig(max_slippage_bps=200),
        simulation=PaperSimulationConfig(
            enabled=True,
            signal_detect_delay_ms=0,
            decision_delay_ms=0,
            submit_delay_ms=0,
            exchange_ack_delay_ms=0,
            latency_jitter_ms=0,
            fill_timeout_ms=50,
            poll_interval_ms=25,
            allow_partial_fill=True,
            use_delayed_book_snapshot=True,
        ),
        market_price_lookup=lambda slug, outcome, token_id: 0.315,
        sleeper=clock.sleep,
        monotonic=clock.monotonic,
    )
    order = AggregatedOrder(
        order_id="paper-gamma-price",
        token_id="token-1",
        side="BUY",
        requested_amount_usdc=3.15,
        requested_shares=10.0,
        reference_price=0.32,
        market_slug="market-1",
        market_title="Market 1",
        outcome="Down",
        components=[],
    )
    result = executor.execute(order)
    assert result.status == "simulated"
    assert round(result.avg_price, 6) == 0.315
    assert round(result.executed_shares, 6) == 10.0
    assert result.price_source == "gamma_outcome"


def test_leader_polling_source_prefers_activity_feed():
    now = utc_now()
    ts = int(now.timestamp())
    client = FakeSignalClient(
        activity_rows=[
            {
                "timestamp": ts,
                "type": "TRADE",
                "side": "BUY",
                "asset": "token-1",
                "conditionId": "cond-1",
                "slug": "market-1",
                "title": "Market 1",
                "outcome": "Yes",
                "price": 0.42,
                "size": 10,
                "transactionHash": "0xabc",
            }
        ],
        trade_rows=[
            {
                "timestamp": ts,
                "type": "TRADE",
                "side": "BUY",
                "asset": "token-1",
                "conditionId": "cond-1",
                "slug": "market-1",
                "title": "Market 1",
                "outcome": "Yes",
                "price": 0.99,
                "size": 10,
                "transactionHash": "0xdef",
            }
        ],
    )
    state = RuntimeStateSnapshot()
    source = LeaderPollingSource(client, [trader("0xabc")], state, history_limit=20, max_signal_age_sec=8)

    events = source.poll()

    assert len(events) == 1
    assert round(events[0].price, 6) == 0.42
    assert [call[0] for call in client.calls] == ["activity"]


def test_leader_polling_source_drops_stale_events_but_advances_cursor():
    stale_ts = int((utc_now() - timedelta(seconds=30)).timestamp())
    client = FakeSignalClient(
        activity_rows=[
            {
                "timestamp": stale_ts,
                "type": "TRADE",
                "side": "BUY",
                "asset": "token-1",
                "conditionId": "cond-1",
                "slug": "market-1",
                "title": "Market 1",
                "outcome": "Yes",
                "price": 0.42,
                "size": 10,
                "transactionHash": "0xabc",
            }
        ]
    )
    state = RuntimeStateSnapshot()
    source = LeaderPollingSource(client, [trader("0xabc")], state, history_limit=20, max_signal_age_sec=8)

    first = source.poll()
    second = source.poll()

    assert first == []
    assert second == []
    assert state.last_seen["0xabc"]["timestamp"] == stale_ts


def test_position_tracker_can_settle_resolved_market():
    repository = InMemoryRepository()
    tracker = PositionTracker(repository, RuntimeStateSnapshot(cash_balance_usdc=100), 100)

    buy_component = SourceIntent(
        intent_id="settle-buy",
        trader_address="0xabc",
        trader_label="alpha",
        token_id="token-win",
        condition_id="cond1",
        market_slug="market-1",
        market_title="Market 1",
        outcome="Yes",
        side="BUY",
        leader_event_id="evt-buy",
        leader_tx_hash="tx-buy",
        leader_reference_price=0.4,
        leader_shares=10.0,
        leader_notional_usdc=4.0,
        requested_amount_usdc=4.0,
        requested_shares=10.0,
        multiplier=1.0,
        copy_ratio=1.0,
    )
    order = AggregatedOrder(
        order_id="settle-order",
        token_id="token-win",
        side="BUY",
        requested_amount_usdc=4.0,
        requested_shares=10.0,
        reference_price=0.4,
        market_slug="market-1",
        market_title="Market 1",
        outcome="Yes",
        components=[buy_component],
    )
    execution = ExecutionResult(
        execution_id="settle-exec",
        order_id="settle-order",
        mode="paper",
        status="simulated",
        token_id="token-win",
        side="BUY",
        requested_amount_usdc=4.0,
        requested_shares=10.0,
        executed_amount_usdc=4.0,
        executed_shares=10.0,
        avg_price=0.4,
        best_price=0.4,
        price_source="test",
        slippage_bps=0.0,
        leader_deviation_bps=0.0,
    )
    tracker.apply_execution(order, execution)

    settlements = tracker.apply_market_settlement({"token-win": 1.0})
    assert len(settlements) == 1
    assert round(tracker.state.cash_balance_usdc, 6) == 106.0
    assert round(tracker.state.realized_pnl_today_usdc, 6) == 6.0
    assert tracker.list_positions() == []


def test_position_tracker_can_settle_losing_market_to_zero():
    repository = InMemoryRepository()
    tracker = PositionTracker(repository, RuntimeStateSnapshot(cash_balance_usdc=100), 100)

    buy_component = SourceIntent(
        intent_id="settle-lose",
        trader_address="0xabc",
        trader_label="alpha",
        token_id="token-lose",
        condition_id="cond1",
        market_slug="market-2",
        market_title="Market 2",
        outcome="No",
        side="BUY",
        leader_event_id="evt-lose",
        leader_tx_hash="tx-lose",
        leader_reference_price=0.25,
        leader_shares=8.0,
        leader_notional_usdc=2.0,
        requested_amount_usdc=2.0,
        requested_shares=8.0,
        multiplier=1.0,
        copy_ratio=1.0,
    )
    order = AggregatedOrder(
        order_id="settle-order-lose",
        token_id="token-lose",
        side="BUY",
        requested_amount_usdc=2.0,
        requested_shares=8.0,
        reference_price=0.25,
        market_slug="market-2",
        market_title="Market 2",
        outcome="No",
        components=[buy_component],
    )
    execution = ExecutionResult(
        execution_id="settle-exec-lose",
        order_id="settle-order-lose",
        mode="paper",
        status="simulated",
        token_id="token-lose",
        side="BUY",
        requested_amount_usdc=2.0,
        requested_shares=8.0,
        executed_amount_usdc=2.0,
        executed_shares=8.0,
        avg_price=0.25,
        best_price=0.25,
        price_source="test",
        slippage_bps=0.0,
        leader_deviation_bps=0.0,
    )
    tracker.apply_execution(order, execution)

    settlements = tracker.apply_market_settlement({"token-lose": 0.0})
    assert len(settlements) == 1
    assert round(tracker.state.cash_balance_usdc, 6) == 98.0
    assert round(tracker.state.realized_pnl_today_usdc, 6) == -2.0
    assert tracker.list_positions() == []
