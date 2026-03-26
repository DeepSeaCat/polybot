from __future__ import annotations

import json
import threading
import time
from typing import Any, Dict, Iterable, List
from urllib.parse import urlparse


def _float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


class MarketWebSocketClient:
    def __init__(
        self,
        url: str,
        ping_interval_sec: int = 10,
        reconnect_delay_sec: float = 2.0,
        stale_after_sec: int = 5,
        connect_timeout_sec: int = 10,
        custom_feature_enabled: bool = True,
        http_proxy: str = "",
        https_proxy: str = "",
    ):
        self.url = url
        self.ping_interval_sec = max(2, int(ping_interval_sec))
        self.reconnect_delay_sec = max(0.5, float(reconnect_delay_sec))
        self.stale_after_sec = max(1, int(stale_after_sec))
        self.connect_timeout_sec = max(2, int(connect_timeout_sec))
        self.custom_feature_enabled = bool(custom_feature_enabled)
        self.http_proxy = http_proxy
        self.https_proxy = https_proxy

        self._lock = threading.RLock()
        self._books: Dict[str, Dict[str, Any]] = {}
        self._desired_assets: set[str] = set()
        self._subscribed_assets: set[str] = set()
        self._stop_event = threading.Event()
        self._connected_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._ws = None
        self.last_error: str = ""
        self.message_count: int = 0
        self.last_payload_preview: str = ""

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_forever, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._connected_event.clear()
        ws = self._ws
        self._ws = None
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)

    def is_connected(self) -> bool:
        return self._connected_event.is_set()

    def ensure_assets(self, asset_ids: Iterable[str]) -> None:
        normalized = {str(item).strip() for item in asset_ids if str(item).strip()}
        if not normalized:
            return
        with self._lock:
            new_assets = normalized.difference(self._desired_assets)
            self._desired_assets.update(normalized)
        if not new_assets:
            return
        ws = self._ws
        if ws is not None and self.is_connected():
            with self._lock:
                asset_ids = sorted(self._desired_assets)
            self._send(
                {
                    "assets_ids": asset_ids,
                    "type": "market",
                    "custom_feature_enabled": self.custom_feature_enabled,
                }
            )
            with self._lock:
                self._subscribed_assets = set(asset_ids)

    def get_order_book(self, token_id: str) -> Dict[str, Any]:
        if token_id:
            self.ensure_assets([token_id])
        with self._lock:
            book = dict(self._books.get(token_id, {}))
        if not book:
            return {}
        if self._is_stale(book):
            return {}
        return self._materialize_book(book)

    def get_mark_price(self, token_id: str) -> float:
        book = self.get_order_book(token_id)
        if not book:
            return 0.0
        best_bid = _float(book.get("best_bid"), 0.0)
        best_ask = _float(book.get("best_ask"), 0.0)
        if best_bid > 0 and best_ask > 0:
            return (best_bid + best_ask) / 2.0
        last_trade = _float(book.get("last_trade_price"), 0.0)
        if last_trade > 0:
            return last_trade
        if best_bid > 0:
            return best_bid
        if best_ask > 0:
            return best_ask
        return 0.0

    def _is_stale(self, book: Dict[str, Any]) -> bool:
        updated_at = _float(book.get("_updated_at_monotonic"), 0.0)
        if updated_at <= 0:
            return True
        return time.monotonic() - updated_at > self.stale_after_sec

    def _run_forever(self) -> None:
        try:
            import websocket
        except Exception:
            return

        while not self._stop_event.is_set():
            ws = None
            try:
                connect_kwargs = self._connect_kwargs()
                ws = websocket.create_connection(
                    self.url,
                    timeout=self.connect_timeout_sec,
                    **connect_kwargs,
                )
                ws.settimeout(1)
                self._ws = ws
                self._connected_event.set()
                self._subscribe_initial()
                last_ping = time.monotonic()
                while not self._stop_event.is_set():
                    now = time.monotonic()
                    if now - last_ping >= self.ping_interval_sec:
                        try:
                            ws.send("PING")
                        except Exception:
                            break
                        last_ping = now

                    try:
                        raw = ws.recv()
                    except websocket.WebSocketTimeoutException:
                        continue
                    except Exception:
                        break

                    if raw in {"PONG", "PING", b"PONG", b"PING"}:
                        if raw in {"PING", b"PING"}:
                            try:
                                ws.send("PONG")
                            except Exception:
                                break
                        continue

                    self._handle_message(raw)
            except Exception as exc:
                self.last_error = f"{type(exc).__name__}: {exc}"
            finally:
                self._connected_event.clear()
                self._ws = None
                if ws is not None:
                    try:
                        ws.close()
                    except Exception:
                        pass
            if not self._stop_event.is_set():
                time.sleep(self.reconnect_delay_sec)

    def _connect_kwargs(self) -> Dict[str, Any]:
        proxy_url = self.https_proxy or self.http_proxy
        if not proxy_url:
            return {}
        parsed = urlparse(proxy_url)
        if not parsed.hostname:
            return {}
        kwargs: Dict[str, Any] = {
            "http_proxy_host": parsed.hostname,
            "http_proxy_port": parsed.port,
        }
        if parsed.username and parsed.password:
            kwargs["http_proxy_auth"] = (parsed.username, parsed.password)
        if parsed.scheme in {"socks5", "socks5h"}:
            kwargs["proxy_type"] = "socks5"
        else:
            kwargs["proxy_type"] = "http"
        return kwargs

    def _subscribe_initial(self) -> None:
        with self._lock:
            asset_ids = sorted(self._desired_assets)
        if not asset_ids:
            return
        self._send(
            {
                "assets_ids": asset_ids,
                "type": "market",
                "custom_feature_enabled": self.custom_feature_enabled,
            }
        )
        with self._lock:
            self._subscribed_assets = set(asset_ids)

    def _send(self, payload: Dict[str, Any]) -> None:
        ws = self._ws
        if ws is None:
            return
        ws.send(json.dumps(payload))

    def _handle_message(self, raw: Any) -> None:
        text = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw)
        self.last_payload_preview = text[:500]
        payload = json.loads(text)
        self.message_count += 1
        self.apply_payload(payload)

    def apply_payload(self, payload: Any) -> None:
        if isinstance(payload, list):
            for item in payload:
                self.apply_payload(item)
            return
        if not isinstance(payload, dict):
            return

        event_type = str(payload.get("event_type", "")).strip().lower()
        if event_type == "book" or (
            not event_type and "asset_id" in payload and ("bids" in payload or "asks" in payload)
        ):
            self._apply_book_snapshot(payload)
        elif event_type == "price_change" or (not event_type and "price_changes" in payload):
            self._apply_price_change(payload)
        elif event_type == "best_bid_ask" or (
            not event_type and "asset_id" in payload and ("best_bid" in payload or "best_ask" in payload)
        ):
            self._apply_best_bid_ask(payload)
        elif event_type == "last_trade_price":
            self._apply_last_trade_price(payload)

    def _apply_book_snapshot(self, payload: Dict[str, Any]) -> None:
        token_id = str(payload.get("asset_id", "")).strip()
        if not token_id:
            return
        book = self._get_or_create_book(token_id)
        book["market"] = str(payload.get("market", ""))
        book["bids"] = self._normalize_levels(payload.get("bids"), reverse=True)
        book["asks"] = self._normalize_levels(payload.get("asks"), reverse=False)
        book["hash"] = str(payload.get("hash", ""))
        book["timestamp"] = str(payload.get("timestamp", ""))
        if book["bids"]:
            book["best_bid"] = _float(book["bids"][0]["price"], 0.0)
        if book["asks"]:
            book["best_ask"] = _float(book["asks"][0]["price"], 0.0)
        self._mark_book_updated(book)

    def _apply_price_change(self, payload: Dict[str, Any]) -> None:
        changes = payload.get("price_changes")
        if not isinstance(changes, list):
            return
        timestamp = str(payload.get("timestamp", ""))
        for item in changes:
            if not isinstance(item, dict):
                continue
            token_id = str(item.get("asset_id", "")).strip()
            if not token_id:
                continue
            side = str(item.get("side", "")).strip().upper()
            price = _float(item.get("price"), 0.0)
            size = _float(item.get("size"), 0.0)
            book = self._get_or_create_book(token_id)
            levels_key = "bids" if side == "BUY" else "asks"
            reverse = side == "BUY"
            levels = self._normalize_levels(book.get(levels_key), reverse=reverse)
            levels = [lvl for lvl in levels if _float(lvl.get("price"), 0.0) != price]
            if price > 0 and size > 0:
                levels.append({"price": f"{price}", "size": f"{size}"})
            book[levels_key] = self._normalize_levels(levels, reverse=reverse)
            best_bid = _float(item.get("best_bid"), 0.0)
            best_ask = _float(item.get("best_ask"), 0.0)
            if best_bid > 0:
                book["best_bid"] = best_bid
            elif book.get("bids"):
                book["best_bid"] = _float(book["bids"][0]["price"], 0.0)
            if best_ask > 0:
                book["best_ask"] = best_ask
            elif book.get("asks"):
                book["best_ask"] = _float(book["asks"][0]["price"], 0.0)
            book["timestamp"] = timestamp
            book["hash"] = str(item.get("hash", "")) or str(book.get("hash", ""))
            self._mark_book_updated(book)

    def _apply_best_bid_ask(self, payload: Dict[str, Any]) -> None:
        token_id = str(payload.get("asset_id", "")).strip()
        if not token_id:
            return
        book = self._get_or_create_book(token_id)
        book["market"] = str(payload.get("market", "")) or str(book.get("market", ""))
        best_bid = _float(payload.get("best_bid"), 0.0)
        best_ask = _float(payload.get("best_ask"), 0.0)
        if best_bid > 0:
            book["best_bid"] = best_bid
        if best_ask > 0:
            book["best_ask"] = best_ask
        book["timestamp"] = str(payload.get("timestamp", "")) or str(book.get("timestamp", ""))
        self._mark_book_updated(book)

    def _apply_last_trade_price(self, payload: Dict[str, Any]) -> None:
        token_id = str(payload.get("asset_id", "")).strip()
        if not token_id:
            return
        book = self._get_or_create_book(token_id)
        price = _float(payload.get("price"), 0.0)
        if price > 0:
            book["last_trade_price"] = price
        book["timestamp"] = str(payload.get("timestamp", "")) or str(book.get("timestamp", ""))
        self._mark_book_updated(book)

    def _get_or_create_book(self, token_id: str) -> Dict[str, Any]:
        with self._lock:
            book = self._books.setdefault(
                token_id,
                {
                    "asset_id": token_id,
                    "market": "",
                    "bids": [],
                    "asks": [],
                    "best_bid": 0.0,
                    "best_ask": 0.0,
                    "last_trade_price": 0.0,
                    "timestamp": "",
                    "hash": "",
                    "_source": "ws_market",
                    "_updated_at_monotonic": 0.0,
                },
            )
            return book

    def _mark_book_updated(self, book: Dict[str, Any]) -> None:
        book["_source"] = "ws_market"
        book["_updated_at_monotonic"] = time.monotonic()

    def _materialize_book(self, book: Dict[str, Any]) -> Dict[str, Any]:
        # WS 流里会同时出现整本盘口、best bid/ask、last trade 等多种消息。
        # 这里的目标不是“原样返回收到的字段”，而是尽量拼出一个对执行器可用的统一盘口视图。
        rendered = dict(book)
        raw_bids = self._normalize_levels(book.get("bids"), reverse=True)
        raw_asks = self._normalize_levels(book.get("asks"), reverse=False)
        best_bid = _float(book.get("best_bid"), 0.0)
        best_ask = _float(book.get("best_ask"), 0.0)

        rendered["bids"] = self._render_levels(raw_bids, best_bid, reverse=True)
        rendered["asks"] = self._render_levels(raw_asks, best_ask, reverse=False)
        if best_bid <= 0 and rendered["bids"]:
            rendered["best_bid"] = _float(rendered["bids"][0]["price"], 0.0)
        if best_ask <= 0 and rendered["asks"]:
            rendered["best_ask"] = _float(rendered["asks"][0]["price"], 0.0)
        return rendered

    def _render_levels(self, raw_levels: List[Dict[str, str]], best_price: float, reverse: bool) -> List[Dict[str, str]]:
        _ = reverse
        if best_price > 0 and raw_levels:
            top_price = _float(raw_levels[0].get("price"), 0.0)
            if abs(top_price - best_price) <= 0.02:
                return raw_levels
        if best_price <= 0:
            return raw_levels

        # 某些时刻 best bid/ask 会先于整本盘口更新，导致“top of book”和“best_price”短暂不一致。
        # 这里选择信 best_price，但成交量只借用当前可见顶层 size，避免凭空造出无限深度。
        matching_size = 0.0
        for level in raw_levels:
            if abs(_float(level.get("price"), 0.0) - best_price) <= 1e-6:
                matching_size = _float(level.get("size"), 0.0)
                break
        if matching_size <= 0 and raw_levels:
            matching_size = _float(raw_levels[0].get("size"), 0.0)
        if matching_size <= 0:
            return []
        return [{"price": f"{best_price}", "size": f"{matching_size}"}]

    def _normalize_levels(self, raw_levels: Any, reverse: bool) -> List[Dict[str, str]]:
        levels: List[Dict[str, str]] = []
        if not isinstance(raw_levels, list):
            return levels
        for item in raw_levels:
            if not isinstance(item, dict):
                continue
            price = _float(item.get("price"), 0.0)
            size = _float(item.get("size"), 0.0)
            if price <= 0 or size <= 0:
                continue
            levels.append({"price": f"{price}", "size": f"{size}"})
        levels.sort(key=lambda row: _float(row.get("price"), 0.0), reverse=reverse)
        return levels
