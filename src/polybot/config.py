from dataclasses import dataclass, field
from typing import Dict, List


@dataclass
class ApiPaths:
    # Put multiple candidates for each endpoint. The client will try in order.
    leaderboard: List[str] = field(
        default_factory=lambda: [
            "/v1/leaderboard",
            "/leaderboard",
            "/traders/leaderboard",
            "/users/leaderboard",
        ]
    )
    public_profile: List[str] = field(
        default_factory=lambda: [
            "https://gamma-api.polymarket.com/public-profile",
            "/profile/{address}",
            "/users/{address}/profile",
            "/users/{address}",
        ]
    )
    current_positions: List[str] = field(
        default_factory=lambda: [
            "/positions",
            "/users/{address}/positions",
        ]
    )
    closed_positions: List[str] = field(
        default_factory=lambda: [
            "/closed-positions",
            "/positions/closed",
            "/users/{address}/positions/closed",
        ]
    )
    user_activity: List[str] = field(
        default_factory=lambda: [
            "/activity",
            "/users/{address}/activity",
        ]
    )
    user_trades: List[str] = field(
        default_factory=lambda: [
            "/trades",
            "/users/{address}/trades",
        ]
    )
    accounting_snapshot: List[str] = field(
        default_factory=lambda: [
            "/v1/accounting/snapshot",
            "/accounting/{address}/snapshot",
            "/accounting/snapshot/{address}",
        ]
    )


@dataclass
class AnalyzerConfig:
    base_url: str = "https://data-api.polymarket.com"
    gamma_base_url: str = "https://gamma-api.polymarket.com"
    request_timeout_sec: int = 20
    sleep_between_requests_sec: float = 0.05
    max_traders: int = 50
    max_market_lookups_per_trader: int = 0
    leaderboard_category: str = "CRYPTO"
    leaderboard_interval: str = "MONTH"
    leaderboard_order_by: str = "PNL"
    analysis_windows_days: List[int] = field(default_factory=lambda: [7, 30, 90])
    copyability_weights: Dict[str, float] = field(
        default_factory=lambda: {
            "profitability": 0.28,
            "stability": 0.24,
            "copyability": 0.24,
            "focus": 0.14,
            "execution": 0.10,
        }
    )
    api_paths: ApiPaths = field(default_factory=ApiPaths)
