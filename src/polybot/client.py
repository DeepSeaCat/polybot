import time
from typing import Any, Dict, List, Optional

import requests

from .config import AnalyzerConfig


class ApiEndpointError(RuntimeError):
    pass


class PolyDataApiClient:
    def __init__(self, config: AnalyzerConfig):
        self.config = config
        self.session = requests.Session()

    def _request_candidates(
        self,
        path_candidates: List[str],
        params: Optional[Dict[str, Any]] = None,
        path_kwargs: Optional[Dict[str, Any]] = None,
    ) -> Any:
        path_kwargs = path_kwargs or {}
        params = params or {}
        last_error: Optional[str] = None

        for path_template in path_candidates:
            path = path_template.format(**path_kwargs)
            if path.startswith("http://") or path.startswith("https://"):
                url = path
            else:
                url = f"{self.config.base_url.rstrip('/')}{path}"
            try:
                response = self.session.get(url, params=params, timeout=self.config.request_timeout_sec)
                if response.status_code in (400, 404, 422):
                    last_error = f"{url} -> {response.status_code}"
                    continue
                response.raise_for_status()
                time.sleep(self.config.sleep_between_requests_sec)
                return response.json()
            except requests.RequestException as exc:
                last_error = f"{url} -> {exc}"
                continue

        raise ApiEndpointError(
            f"All endpoint candidates failed. Last error: {last_error}. "
            "Please adjust paths in src/polybot/config.py"
        )

    def get_leaderboard(
        self,
        category: str,
        interval: str,
        order_by: str,
        limit: int,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        payload = self._request_candidates(
            self.config.api_paths.leaderboard,
            params={
                "category": category,
                "timePeriod": interval,
                "interval": interval,
                "orderBy": order_by,
                "limit": limit,
                "offset": offset,
            },
        )
        if isinstance(payload, dict):
            for key in ("data", "results", "items", "leaderboard"):
                if key in payload and isinstance(payload[key], list):
                    return payload[key]
        if isinstance(payload, list):
            return payload
        return []

    def get_public_profile(self, address: str) -> Dict[str, Any]:
        payload = self._request_candidates(
            self.config.api_paths.public_profile,
            path_kwargs={"address": address},
            params={"address": address, "user": address},
        )
        return payload if isinstance(payload, dict) else {}

    def get_current_positions(self, address: str) -> List[Dict[str, Any]]:
        payload = self._request_candidates(
            self.config.api_paths.current_positions,
            path_kwargs={"address": address},
            params={"address": address, "user": address, "status": "open", "limit": 500},
        )
        if isinstance(payload, dict):
            for key in ("data", "results", "items", "positions"):
                if key in payload and isinstance(payload[key], list):
                    return payload[key]
        return payload if isinstance(payload, list) else []

    def get_closed_positions(self, address: str) -> List[Dict[str, Any]]:
        payload = self._request_candidates(
            self.config.api_paths.closed_positions,
            path_kwargs={"address": address},
            params={
                "address": address,
                "user": address,
                "status": "closed",
                "limit": 50,
                "sortBy": "TIMESTAMP",
                "sortDirection": "DESC",
            },
        )
        if isinstance(payload, dict):
            for key in ("data", "results", "items", "positions"):
                if key in payload and isinstance(payload[key], list):
                    return payload[key]
        return payload if isinstance(payload, list) else []

    def get_user_activity(self, address: str, limit: int = 500) -> List[Dict[str, Any]]:
        payload = self._request_candidates(
            self.config.api_paths.user_activity,
            path_kwargs={"address": address},
            params={"address": address, "user": address, "limit": limit},
        )
        if isinstance(payload, dict):
            for key in ("data", "results", "items", "activity"):
                if key in payload and isinstance(payload[key], list):
                    return payload[key]
        return payload if isinstance(payload, list) else []

    def get_user_trades(self, address: str, limit: int = 2000) -> List[Dict[str, Any]]:
        payload = self._request_candidates(
            self.config.api_paths.user_trades,
            path_kwargs={"address": address},
            params={"address": address, "user": address, "limit": limit},
        )
        if isinstance(payload, dict):
            for key in ("data", "results", "items", "trades"):
                if key in payload and isinstance(payload[key], list):
                    return payload[key]
        return payload if isinstance(payload, list) else []

    def get_accounting_snapshot_url(self, address: str) -> Optional[str]:
        # The OpenAPI spec returns a ZIP file directly; expose a stable URL.
        if self.config.api_paths.accounting_snapshot:
            path = self.config.api_paths.accounting_snapshot[0]
            if path.startswith("http://") or path.startswith("https://"):
                base = path
            else:
                base = f"{self.config.base_url.rstrip('/')}{path}"
            return f"{base}?user={address}"
        return None

    def get_market_by_slug(self, slug: str) -> Dict[str, Any]:
        url = f"{self.config.gamma_base_url.rstrip('/')}/markets"
        try:
            timeout = min(self.config.request_timeout_sec, 8)
            response = self.session.get(url, params={"slug": slug, "limit": 1}, timeout=timeout)
            response.raise_for_status()
            payload = response.json()
            if isinstance(payload, list) and payload:
                return payload[0]
            if isinstance(payload, dict):
                return payload
        except requests.RequestException:
            return {}
        return {}
