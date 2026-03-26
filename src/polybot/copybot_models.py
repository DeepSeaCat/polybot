from __future__ import annotations

import json
import os
from dataclasses import dataclass, field, fields, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, (int, float)):
        if value > 10**12:
            return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc)
        return datetime.fromtimestamp(value, tz=timezone.utc)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return utc_now()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    return utc_now()


def to_primitive(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, Path):
        return str(value)
    if is_dataclass(value):
        return {item.name: to_primitive(getattr(value, item.name)) for item in fields(value)}
    if isinstance(value, dict):
        return {str(k): to_primitive(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [to_primitive(v) for v in value]
    return value


def short_address(address: str) -> str:
    text = (address or "").strip()
    if len(text) <= 12:
        return text
    return f"{text[:6]}...{text[-4:]}"


@dataclass
class MultiplierTier:
    min_leader_notional_usdc: float
    multiplier: float


@dataclass
class TraderConfig:
    address: str
    label: str = ""
    enabled: bool = True
    sizing_mode: str = "fixed_notional"
    copy_ratio: float = 0.10
    max_open_allocation_usdc: float = 0.0
    max_order_usdc: float = 0.0
    leader_equity_cache_ttl_sec: int = 15
    multiplier_tiers: List[MultiplierTier] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "TraderConfig":
        tiers = [
            MultiplierTier(
                min_leader_notional_usdc=float(item.get("min_leader_notional_usdc", 0.0)),
                multiplier=float(item.get("multiplier", 1.0)),
            )
            for item in payload.get("multiplier_tiers", [])
        ]
        return cls(
            address=str(payload.get("address", "")).lower(),
            label=str(payload.get("label", "")).strip(),
            enabled=bool(payload.get("enabled", True)),
            sizing_mode=str(payload.get("sizing_mode", "fixed_notional")).strip() or "fixed_notional",
            copy_ratio=float(payload.get("copy_ratio", 0.10)),
            max_open_allocation_usdc=float(payload.get("max_open_allocation_usdc", 0.0)),
            max_order_usdc=float(payload.get("max_order_usdc", 0.0)),
            leader_equity_cache_ttl_sec=int(payload.get("leader_equity_cache_ttl_sec", 15)),
            multiplier_tiers=tiers,
            tags=[str(item) for item in payload.get("tags", [])],
        )

    def display_name(self) -> str:
        return self.label or short_address(self.address)

    def multiplier_for(self, leader_notional_usdc: float) -> float:
        multiplier = 1.0
        for tier in sorted(self.multiplier_tiers, key=lambda item: item.min_leader_notional_usdc):
            if leader_notional_usdc >= tier.min_leader_notional_usdc:
                multiplier = tier.multiplier
        return multiplier


@dataclass
class RiskConfig:
    min_order_usdc: float = 5.0
    max_order_usdc: float = 250.0
    max_total_exposure_usdc: float = 1000.0
    max_market_exposure_usdc: float = 300.0
    max_trader_exposure_usdc: float = 250.0
    max_daily_loss_usdc: float = 100.0
    max_open_positions: int = 20
    max_slippage_bps: float = 150.0
    max_leader_price_deviation_bps: float = 250.0

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "RiskConfig":
        return cls(
            min_order_usdc=float(payload.get("min_order_usdc", 5.0)),
            max_order_usdc=float(payload.get("max_order_usdc", 250.0)),
            max_total_exposure_usdc=float(payload.get("max_total_exposure_usdc", 1000.0)),
            max_market_exposure_usdc=float(payload.get("max_market_exposure_usdc", 300.0)),
            max_trader_exposure_usdc=float(payload.get("max_trader_exposure_usdc", 250.0)),
            max_daily_loss_usdc=float(payload.get("max_daily_loss_usdc", 100.0)),
            max_open_positions=int(payload.get("max_open_positions", 20)),
            max_slippage_bps=float(payload.get("max_slippage_bps", 150.0)),
            max_leader_price_deviation_bps=float(payload.get("max_leader_price_deviation_bps", 250.0)),
        )


@dataclass
class MongoConfig:
    enabled: bool = True
    uri: str = "mongodb://localhost:27017"
    database: str = "polybot"

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "MongoConfig":
        return cls(
            enabled=bool(payload.get("enabled", True)),
            uri=str(payload.get("uri", "mongodb://localhost:27017")),
            database=str(payload.get("database", "polybot")),
        )


@dataclass
class DashboardConfig:
    enabled: bool = True
    host: str = "127.0.0.1"
    port: int = 8088
    refresh_sec: int = 3

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "DashboardConfig":
        return cls(
            enabled=bool(payload.get("enabled", True)),
            host=str(payload.get("host", "127.0.0.1")),
            port=int(payload.get("port", 8088)),
            refresh_sec=int(payload.get("refresh_sec", 3)),
        )


@dataclass
class WalletConfig:
    mode: str = "paper"
    starting_balance_usdc: float = 1000.0
    clob_host: str = "https://clob.polymarket.com"
    chain_id: int = 137
    signature_type: int = 1
    private_key_env: str = "POLYMARKET_PRIVATE_KEY"
    funder_env: str = "POLYMARKET_FUNDER"
    api_key_env: str = "POLYMARKET_API_KEY"
    api_secret_env: str = "POLYMARKET_API_SECRET"
    api_passphrase_env: str = "POLYMARKET_API_PASSPHRASE"

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "WalletConfig":
        return cls(
            mode=str(payload.get("mode", "paper")).lower(),
            starting_balance_usdc=float(payload.get("starting_balance_usdc", 1000.0)),
            clob_host=str(payload.get("clob_host", "https://clob.polymarket.com")),
            chain_id=int(payload.get("chain_id", 137)),
            signature_type=int(payload.get("signature_type", 1)),
            private_key_env=str(payload.get("private_key_env", "POLYMARKET_PRIVATE_KEY")),
            funder_env=str(payload.get("funder_env", "POLYMARKET_FUNDER")),
            api_key_env=str(payload.get("api_key_env", "POLYMARKET_API_KEY")),
            api_secret_env=str(payload.get("api_secret_env", "POLYMARKET_API_SECRET")),
            api_passphrase_env=str(payload.get("api_passphrase_env", "POLYMARKET_API_PASSPHRASE")),
        )

    def private_key(self) -> str:
        return os.getenv(self.private_key_env, "")

    def funder(self) -> str:
        return os.getenv(self.funder_env, "")

    def api_creds(self) -> Dict[str, str]:
        return {
            "api_key": os.getenv(self.api_key_env, ""),
            "api_secret": os.getenv(self.api_secret_env, ""),
            "api_passphrase": os.getenv(self.api_passphrase_env, ""),
        }


@dataclass
class PaperSimulationConfig:
    enabled: bool = True
    signal_detect_delay_ms: int = 600
    decision_delay_ms: int = 120
    submit_delay_ms: int = 180
    exchange_ack_delay_ms: int = 100
    latency_jitter_ms: int = 200
    fill_timeout_ms: int = 1200
    poll_interval_ms: int = 250
    allow_partial_fill: bool = True
    use_delayed_book_snapshot: bool = True

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "PaperSimulationConfig":
        return cls(
            enabled=bool(payload.get("enabled", True)),
            signal_detect_delay_ms=int(payload.get("signal_detect_delay_ms", 600)),
            decision_delay_ms=int(payload.get("decision_delay_ms", 120)),
            submit_delay_ms=int(payload.get("submit_delay_ms", 180)),
            exchange_ack_delay_ms=int(payload.get("exchange_ack_delay_ms", 100)),
            latency_jitter_ms=int(payload.get("latency_jitter_ms", 200)),
            fill_timeout_ms=int(payload.get("fill_timeout_ms", 1200)),
            poll_interval_ms=int(payload.get("poll_interval_ms", 250)),
            allow_partial_fill=bool(payload.get("allow_partial_fill", True)),
            use_delayed_book_snapshot=bool(payload.get("use_delayed_book_snapshot", True)),
        )


@dataclass
class MarketWebsocketConfig:
    enabled: bool = False
    url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    ping_interval_sec: int = 10
    reconnect_delay_sec: float = 2.0
    stale_after_sec: int = 5
    connect_timeout_sec: int = 10
    custom_feature_enabled: bool = True

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "MarketWebsocketConfig":
        return cls(
            enabled=bool(payload.get("enabled", False)),
            url=str(payload.get("url", "wss://ws-subscriptions-clob.polymarket.com/ws/market")),
            ping_interval_sec=int(payload.get("ping_interval_sec", 10)),
            reconnect_delay_sec=float(payload.get("reconnect_delay_sec", 2.0)),
            stale_after_sec=int(payload.get("stale_after_sec", 5)),
            connect_timeout_sec=int(payload.get("connect_timeout_sec", 10)),
            custom_feature_enabled=bool(payload.get("custom_feature_enabled", True)),
        )


@dataclass
class RuntimeConfig:
    poll_interval_sec: float = 1.0
    aggregation_window_sec: float = 2.0
    history_limit_per_trader: int = 50
    leader_signal_max_age_sec: int = 8
    data_api_base_url: str = "https://data-api.polymarket.com"
    gamma_api_base_url: str = "https://gamma-api.polymarket.com"
    http_proxy: str = ""
    https_proxy: str = ""
    request_timeout_sec: int = 20
    strategy_signal_file: str = ""
    market_ws: MarketWebsocketConfig = field(default_factory=MarketWebsocketConfig)
    mongo: MongoConfig = field(default_factory=MongoConfig)
    dashboard: DashboardConfig = field(default_factory=DashboardConfig)
    wallet: WalletConfig = field(default_factory=WalletConfig)
    paper_simulation: PaperSimulationConfig = field(default_factory=PaperSimulationConfig)
    risk: RiskConfig = field(default_factory=RiskConfig)
    traders: List[TraderConfig] = field(default_factory=list)

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "RuntimeConfig":
        return cls(
            poll_interval_sec=float(payload.get("poll_interval_sec", 1.0)),
            aggregation_window_sec=float(payload.get("aggregation_window_sec", 2.0)),
            history_limit_per_trader=int(payload.get("history_limit_per_trader", 50)),
            leader_signal_max_age_sec=int(payload.get("leader_signal_max_age_sec", 8)),
            data_api_base_url=str(payload.get("data_api_base_url", "https://data-api.polymarket.com")),
            gamma_api_base_url=str(payload.get("gamma_api_base_url", "https://gamma-api.polymarket.com")),
            http_proxy=str(payload.get("http_proxy", "")),
            https_proxy=str(payload.get("https_proxy", "")),
            request_timeout_sec=int(payload.get("request_timeout_sec", 20)),
            strategy_signal_file=str(payload.get("strategy_signal_file", "")),
            market_ws=MarketWebsocketConfig.from_dict(payload.get("market_ws", {})),
            mongo=MongoConfig.from_dict(payload.get("mongo", {})),
            dashboard=DashboardConfig.from_dict(payload.get("dashboard", {})),
            wallet=WalletConfig.from_dict(payload.get("wallet", {})),
            paper_simulation=PaperSimulationConfig.from_dict(payload.get("paper_simulation", {})),
            risk=RiskConfig.from_dict(payload.get("risk", {})),
            traders=[TraderConfig.from_dict(item) for item in payload.get("traders", [])],
        )

    @classmethod
    def from_file(cls, path: Path) -> "RuntimeConfig":
        payload = json.loads(path.read_text(encoding="utf-8"))
        return cls.from_dict(payload)

    def trader_lookup(self) -> Dict[str, TraderConfig]:
        return {item.address.lower(): item for item in self.traders}


@dataclass
class ClosePlanItem:
    lot_id: str
    leader_shares: float
    follower_shares: float


@dataclass
class LeaderTradeEvent:
    event_id: str
    trader_address: str
    trader_label: str
    tx_hash: str
    token_id: str
    condition_id: str
    market_slug: str
    market_title: str
    outcome: str
    side: str
    price: float
    leader_shares: float
    leader_notional_usdc: float
    observed_at: datetime
    source: str = "leader_poll"
    raw: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SourceIntent:
    intent_id: str
    trader_address: str
    trader_label: str
    token_id: str
    condition_id: str
    market_slug: str
    market_title: str
    outcome: str
    side: str
    leader_event_id: str
    leader_tx_hash: str
    leader_reference_price: float
    leader_shares: float
    leader_notional_usdc: float
    requested_amount_usdc: float
    requested_shares: float
    multiplier: float
    copy_ratio: float
    close_plan: List[ClosePlanItem] = field(default_factory=list)
    created_at: datetime = field(default_factory=utc_now)


@dataclass
class AggregatedOrder:
    order_id: str
    token_id: str
    side: str
    requested_amount_usdc: float
    requested_shares: float
    reference_price: float
    market_slug: str
    market_title: str
    outcome: str
    components: List[SourceIntent] = field(default_factory=list)
    created_at: datetime = field(default_factory=utc_now)
    last_updated_at: datetime = field(default_factory=utc_now)


@dataclass
class ExecutionResult:
    execution_id: str
    order_id: str
    mode: str
    status: str
    token_id: str
    side: str
    requested_amount_usdc: float
    requested_shares: float
    executed_amount_usdc: float
    executed_shares: float
    avg_price: float
    best_price: float
    price_source: str
    slippage_bps: float
    leader_deviation_bps: float
    reason: str = ""
    raw_response: Dict[str, Any] = field(default_factory=dict)
    executed_at: datetime = field(default_factory=utc_now)


@dataclass
class MirroredLot:
    lot_id: str
    trader_address: str
    trader_label: str
    token_id: str
    condition_id: str
    market_slug: str
    market_title: str
    outcome: str
    leader_entry_tx_hash: str
    leader_entry_event_id: str
    leader_initial_shares: float
    leader_remaining_shares: float
    follower_initial_shares: float
    follower_remaining_shares: float
    entry_price: float
    entry_notional_usdc: float
    realized_pnl_usdc: float = 0.0
    status: str = "open"
    opened_at: datetime = field(default_factory=utc_now)
    closed_at: Optional[datetime] = None

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "MirroredLot":
        closed_at = payload.get("closed_at")
        return cls(
            lot_id=str(payload.get("lot_id", "")),
            trader_address=str(payload.get("trader_address", "")).lower(),
            trader_label=str(payload.get("trader_label", "")),
            token_id=str(payload.get("token_id", "")),
            condition_id=str(payload.get("condition_id", "")),
            market_slug=str(payload.get("market_slug", "")),
            market_title=str(payload.get("market_title", "")),
            outcome=str(payload.get("outcome", "")),
            leader_entry_tx_hash=str(payload.get("leader_entry_tx_hash", "")),
            leader_entry_event_id=str(payload.get("leader_entry_event_id", "")),
            leader_initial_shares=float(payload.get("leader_initial_shares", 0.0)),
            leader_remaining_shares=float(payload.get("leader_remaining_shares", 0.0)),
            follower_initial_shares=float(payload.get("follower_initial_shares", 0.0)),
            follower_remaining_shares=float(payload.get("follower_remaining_shares", 0.0)),
            entry_price=float(payload.get("entry_price", 0.0)),
            entry_notional_usdc=float(payload.get("entry_notional_usdc", 0.0)),
            realized_pnl_usdc=float(payload.get("realized_pnl_usdc", 0.0)),
            status=str(payload.get("status", "open")),
            opened_at=parse_datetime(payload.get("opened_at")),
            closed_at=parse_datetime(closed_at) if closed_at else None,
        )


@dataclass
class RuntimeStateSnapshot:
    last_seen: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    cash_balance_usdc: float = 0.0
    realized_pnl_day: str = ""
    realized_pnl_today_usdc: float = 0.0
    unrealized_pnl_usdc: float = 0.0
    market_value_usdc: float = 0.0
    tracked_equity_usdc: float = 0.0
    mark_prices: Dict[str, float] = field(default_factory=dict)
    leader_positions: List[Dict[str, Any]] = field(default_factory=list)
    updated_at: datetime = field(default_factory=utc_now)

    @classmethod
    def from_dict(cls, payload: Dict[str, Any], default_cash: float) -> "RuntimeStateSnapshot":
        return cls(
            last_seen=dict(payload.get("last_seen", {})),
            cash_balance_usdc=float(payload.get("cash_balance_usdc", default_cash)),
            realized_pnl_day=str(payload.get("realized_pnl_day", "")),
            realized_pnl_today_usdc=float(payload.get("realized_pnl_today_usdc", 0.0)),
            unrealized_pnl_usdc=float(payload.get("unrealized_pnl_usdc", 0.0)),
            market_value_usdc=float(payload.get("market_value_usdc", 0.0)),
            tracked_equity_usdc=float(payload.get("tracked_equity_usdc", default_cash)),
            mark_prices={str(k): float(v) for k, v in dict(payload.get("mark_prices", {})).items()},
            leader_positions=[
                dict(item) for item in payload.get("leader_positions", []) if isinstance(item, dict)
            ],
            updated_at=parse_datetime(payload.get("updated_at")),
        )
