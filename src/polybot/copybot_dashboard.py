from __future__ import annotations

import json
import threading
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List

from .copybot_storage import BaseRepository


def _float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _time_delta_ms(start: Any, end: Any) -> int:
    left = _parse_dt(start)
    right = _parse_dt(end)
    if left is None or right is None:
        return 0
    return max(0, int((right - left).total_seconds() * 1000))


def _execution_gap_bps(side: str, leader_price: float, follower_price: float) -> float:
    if leader_price <= 0 or follower_price <= 0:
        return 0.0
    raw_bps = (follower_price - leader_price) / leader_price * 10000.0
    return -raw_bps if str(side).upper() == "SELL" else raw_bps


def _aggregate_open_positions(lots: List[Dict[str, Any]], mark_prices: Dict[str, float] | None = None) -> List[Dict[str, Any]]:
    mark_prices = mark_prices or {}
    grouped: Dict[tuple[str, str], Dict[str, Any]] = {}
    for lot in lots:
        if lot.get("status") != "open":
            continue
        key = (str(lot.get("trader_address", "")), str(lot.get("token_id", "")))
        bucket = grouped.setdefault(
            key,
            {
                "trader_address": str(lot.get("trader_address", "")),
                "trader_label": str(lot.get("trader_label", "")),
                "token_id": str(lot.get("token_id", "")),
                "market_slug": str(lot.get("market_slug", "")),
                "market_title": str(lot.get("market_title", "")),
                "outcome": str(lot.get("outcome", "")),
                "follower_remaining_shares": 0.0,
                "leader_remaining_shares": 0.0,
                "cost_basis_usdc": 0.0,
                "avg_entry_price": 0.0,
                "mark_price": 0.0,
                "market_value_usdc": 0.0,
                "unrealized_pnl_usdc": 0.0,
            },
        )
        follower_shares = float(lot.get("follower_remaining_shares", 0.0))
        leader_shares = float(lot.get("leader_remaining_shares", 0.0))
        price = float(lot.get("entry_price", 0.0))
        bucket["follower_remaining_shares"] += follower_shares
        bucket["leader_remaining_shares"] += leader_shares
        bucket["cost_basis_usdc"] += follower_shares * price

    rows = list(grouped.values())
    for item in rows:
        shares = float(item["follower_remaining_shares"])
        item["avg_entry_price"] = item["cost_basis_usdc"] / shares if shares > 0 else 0.0
        item["mark_price"] = float(mark_prices.get(item["token_id"], item["avg_entry_price"]))
        item["market_value_usdc"] = shares * item["mark_price"]
        item["unrealized_pnl_usdc"] = item["market_value_usdc"] - item["cost_basis_usdc"]
    rows.sort(key=lambda item: item["cost_basis_usdc"], reverse=True)
    return rows


def _build_position_comparison(
    follower_positions: List[Dict[str, Any]],
    leader_positions: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    grouped: Dict[tuple[str, str], Dict[str, Any]] = {}

    for row in leader_positions:
        key = (str(row.get("trader_address", "")), str(row.get("token_id", "")))
        grouped[key] = {
            "trader_address": str(row.get("trader_address", "")),
            "trader_label": str(row.get("trader_label", "")),
            "token_id": str(row.get("token_id", "")),
            "market_slug": str(row.get("market_slug", "")),
            "market_title": str(row.get("market_title", "")),
            "outcome": str(row.get("outcome", "")),
            "leader_shares": float(row.get("leader_shares", 0.0)),
            "leader_avg_price": float(row.get("leader_avg_price", 0.0)),
            "leader_mark_price": float(row.get("leader_mark_price", 0.0)),
            "leader_market_value_usdc": float(row.get("leader_market_value_usdc", 0.0)),
            "leader_unrealized_pnl_usdc": float(row.get("leader_unrealized_pnl_usdc", 0.0)),
            "follower_shares": 0.0,
            "follower_avg_entry_price": 0.0,
            "follower_mark_price": 0.0,
            "follower_market_value_usdc": 0.0,
            "follower_unrealized_pnl_usdc": 0.0,
            "follower_cost_basis_usdc": 0.0,
            "sync_status": "leader-only",
            "follower_vs_leader_pct": 0.0,
        }

    for row in follower_positions:
        key = (str(row.get("trader_address", "")), str(row.get("token_id", "")))
        bucket = grouped.setdefault(
            key,
            {
                "trader_address": str(row.get("trader_address", "")),
                "trader_label": str(row.get("trader_label", "")),
                "token_id": str(row.get("token_id", "")),
                "market_slug": str(row.get("market_slug", "")),
                "market_title": str(row.get("market_title", "")),
                "outcome": str(row.get("outcome", "")),
                "leader_shares": 0.0,
                "leader_avg_price": 0.0,
                "leader_mark_price": 0.0,
                "leader_market_value_usdc": 0.0,
                "leader_unrealized_pnl_usdc": 0.0,
                "follower_shares": 0.0,
                "follower_avg_entry_price": 0.0,
                "follower_mark_price": 0.0,
                "follower_market_value_usdc": 0.0,
                "follower_unrealized_pnl_usdc": 0.0,
                "follower_cost_basis_usdc": 0.0,
                "sync_status": "follower-only",
                "follower_vs_leader_pct": 0.0,
            },
        )
        bucket["follower_shares"] = float(row.get("follower_remaining_shares", 0.0))
        bucket["follower_avg_entry_price"] = float(row.get("avg_entry_price", 0.0))
        bucket["follower_mark_price"] = float(row.get("mark_price", 0.0))
        bucket["follower_market_value_usdc"] = float(row.get("market_value_usdc", 0.0))
        bucket["follower_unrealized_pnl_usdc"] = float(row.get("unrealized_pnl_usdc", 0.0))
        bucket["follower_cost_basis_usdc"] = float(row.get("cost_basis_usdc", 0.0))
        if bucket["leader_shares"] > 0:
            bucket["sync_status"] = "mirrored"

    rows = list(grouped.values())
    for row in rows:
        leader_value = float(row["leader_market_value_usdc"])
        if leader_value > 0:
            row["follower_vs_leader_pct"] = float(row["follower_market_value_usdc"]) / leader_value * 100.0
        elif float(row["leader_shares"]) > 0:
            row["follower_vs_leader_pct"] = float(row["follower_shares"]) / float(row["leader_shares"]) * 100.0
        else:
            row["follower_vs_leader_pct"] = 0.0

    rows.sort(key=lambda item: abs(item["follower_unrealized_pnl_usdc"]), reverse=True)
    return rows


def _build_order_differences(
    orders: List[Dict[str, Any]],
    executions: List[Dict[str, Any]],
    leader_events: List[Dict[str, Any]],
    limit: int = 80,
) -> List[Dict[str, Any]]:
    execution_by_order_id = {
        str(item.get("order_id", "")): item for item in executions if str(item.get("order_id", "")).strip()
    }
    leader_event_by_id = {
        str(item.get("event_id", "")): item for item in leader_events if str(item.get("event_id", "")).strip()
    }

    rows: List[Dict[str, Any]] = []
    for order in orders:
        order_id = str(order.get("order_id", ""))
        side = str(order.get("side", "")).upper()
        execution = execution_by_order_id.get(order_id, {})
        components = order.get("components", [])
        if not isinstance(components, list) or not components:
            components = [{}]

        total_metric = _float(
            order.get("requested_amount_usdc") if side == "BUY" else order.get("requested_shares"),
            0.0,
        )
        component_count = len(components)
        for index, component in enumerate(components, start=1):
            component_side = str(component.get("side") or side).upper()
            component_metric = _float(
                component.get("requested_amount_usdc") if component_side == "BUY" else component.get("requested_shares"),
                0.0,
            )
            weight = component_metric / total_metric if total_metric > 0 else 1.0 / component_count
            leader_event = leader_event_by_id.get(str(component.get("leader_event_id", "")), {})
            leader_price = _float(component.get("leader_reference_price"), _float(leader_event.get("price"), 0.0))
            leader_notional = _float(
                component.get("leader_notional_usdc"),
                _float(leader_event.get("leader_notional_usdc"), 0.0),
            )
            leader_shares = _float(component.get("leader_shares"), _float(leader_event.get("leader_shares"), 0.0))
            requested_amount = _float(component.get("requested_amount_usdc"), 0.0)
            requested_shares = _float(component.get("requested_shares"), 0.0)
            executed_amount = _float(execution.get("executed_amount_usdc"), 0.0) * weight
            executed_shares = _float(execution.get("executed_shares"), 0.0) * weight
            executed_price = _float(execution.get("avg_price"), 0.0)

            rows.append(
                {
                    "order_id": order_id,
                    "execution_id": str(execution.get("execution_id", "")),
                    "component_index": index,
                    "component_count": component_count,
                    "trader_label": str(component.get("trader_label") or leader_event.get("trader_label", "")),
                    "market_title": str(component.get("market_title") or order.get("market_title", "")),
                    "market_slug": str(component.get("market_slug") or order.get("market_slug", "")),
                    "outcome": str(component.get("outcome") or order.get("outcome", "")),
                    "side": component_side,
                    "leader_event_id": str(component.get("leader_event_id", "")),
                    "leader_tx_hash": str(component.get("leader_tx_hash", "")),
                    "leader_observed_at": str(leader_event.get("observed_at", "")),
                    "order_created_at": str(order.get("created_at", "")),
                    "executed_at": str(execution.get("executed_at", "")),
                    "leader_price": leader_price,
                    "leader_notional_usdc": leader_notional,
                    "leader_shares": leader_shares,
                    "follower_requested_amount_usdc": requested_amount,
                    "follower_requested_shares": requested_shares,
                    "follower_executed_amount_usdc": executed_amount,
                    "follower_executed_shares": executed_shares,
                    "follower_fill_price": executed_price,
                    "request_vs_leader_notional_pct": (requested_amount / leader_notional * 100.0)
                    if leader_notional > 0
                    else 0.0,
                    "executed_vs_leader_shares_pct": (executed_shares / leader_shares * 100.0)
                    if leader_shares > 0
                    else 0.0,
                    "execution_gap_bps": _execution_gap_bps(component_side, leader_price, executed_price)
                    if executed_price > 0
                    else 0.0,
                    "delay_ms": int(_float(execution.get("raw_response", {}).get("delay_ms"), 0.0)),
                    "signal_to_order_ms": _time_delta_ms(leader_event.get("observed_at"), order.get("created_at")),
                    "signal_to_fill_ms": _time_delta_ms(leader_event.get("observed_at"), execution.get("executed_at")),
                    "status": str(execution.get("status") or order.get("status", "")),
                    "reason": str(execution.get("reason") or order.get("reason", "")),
                    "price_source": str(execution.get("price_source", "")),
                }
            )

    def sort_key(item: Dict[str, Any]) -> tuple[datetime, str, int]:
        timestamp = (
            _parse_dt(item.get("executed_at"))
            or _parse_dt(item.get("order_created_at"))
            or _parse_dt(item.get("leader_observed_at"))
            or datetime.min
        )
        return (timestamp, item.get("order_id", ""), int(item.get("component_index", 0)))

    rows.sort(key=sort_key, reverse=True)
    return rows[:limit]


def build_dashboard_payload(repository: BaseRepository) -> Dict[str, Any]:
    state = repository.load_runtime_state(default_cash=0.0)
    positions = _aggregate_open_positions(repository.get_open_lot_documents(), state.mark_prices)
    position_comparison = _build_position_comparison(positions, state.leader_positions)
    leader_events = repository.get_recent_leader_events(limit=200)
    orders = repository.get_recent_orders(limit=100)
    executions = repository.get_recent_executions(limit=100)
    order_differences = _build_order_differences(orders, executions, leader_events)
    total_cost = sum(item["cost_basis_usdc"] for item in positions)
    return {
        "runtime": {
            "cash_balance_usdc": state.cash_balance_usdc,
            "realized_pnl_today_usdc": state.realized_pnl_today_usdc,
            "unrealized_pnl_usdc": state.unrealized_pnl_usdc,
            "market_value_usdc": state.market_value_usdc,
            "tracked_equity_usdc": state.tracked_equity_usdc,
            "open_positions": len(positions),
            "total_cost_basis_usdc": total_cost,
            "updated_at": state.updated_at.isoformat(),
        },
        "positions": positions,
        "position_comparison": position_comparison,
        "order_differences": order_differences,
        "leader_events": leader_events[:25],
        "orders": orders[:25],
        "executions": executions[:25],
        "logs": repository.get_recent_logs(limit=50),
    }


def build_dashboard_html(refresh_sec: int) -> str:
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Polybot Copy Dashboard</title>
  <style>
    :root {{
      --bg: #09131f;
      --panel: rgba(14, 29, 44, 0.92);
      --panel-soft: rgba(22, 42, 61, 0.9);
      --line: rgba(128, 176, 214, 0.22);
      --ink: #edf6ff;
      --muted: #96abc2;
      --good: #8de0b2;
      --warn: #f1c66d;
      --bad: #ff8a8a;
      --accent: #66d9ef;
    }}
    body {{
      margin: 0;
      font-family: "IBM Plex Sans", "Avenir Next", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(68, 192, 255, 0.2), transparent 28%),
        radial-gradient(circle at 90% 10%, rgba(119, 255, 194, 0.18), transparent 22%),
        linear-gradient(180deg, #050b14 0%, #0a1624 100%);
    }}
    .wrap {{ max-width: 1480px; margin: 0 auto; padding: 24px 18px 48px; }}
    .hero {{
      display: grid;
      grid-template-columns: 1.6fr 1fr;
      gap: 16px;
      margin-bottom: 18px;
    }}
    .card {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 20px;
      box-shadow: 0 24px 60px rgba(0, 0, 0, 0.35);
      backdrop-filter: blur(14px);
    }}
    .hero-main {{ padding: 24px; }}
    .hero-main h1 {{ margin: 0 0 8px; font-size: 30px; }}
    .hero-main p {{ margin: 0; color: var(--muted); line-height: 1.5; }}
    .stats {{
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 12px;
      padding: 16px;
    }}
    .stat {{
      background: var(--panel-soft);
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 16px;
    }}
    .stat-label {{ color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: 0.08em; }}
    .stat-value {{ font-size: 28px; font-weight: 700; margin-top: 8px; }}
    .grid {{
      display: grid;
      grid-template-columns: 1.15fr 0.85fr;
      gap: 16px;
    }}
    .section {{ padding: 18px; }}
    .section h2 {{ margin: 0 0 12px; font-size: 17px; }}
    table {{ width: 100%; border-collapse: collapse; }}
    th, td {{
      padding: 10px 8px;
      border-bottom: 1px solid var(--line);
      text-align: left;
      font-size: 13px;
      vertical-align: top;
    }}
    th {{ color: var(--muted); font-weight: 600; }}
    .scroll {{ max-height: 520px; overflow: auto; }}
    .mono {{ font-family: "SFMono-Regular", ui-monospace, monospace; }}
    .good {{ color: var(--good); }}
    .warn {{ color: var(--warn); }}
    .bad {{ color: var(--bad); }}
    .log {{ padding: 10px 0; border-bottom: 1px solid var(--line); }}
    .log small {{ color: var(--muted); display: block; margin-top: 4px; }}
    .tiny {{ color: var(--muted); font-size: 11px; display: block; margin-top: 4px; line-height: 1.4; }}
    @media (max-width: 1080px) {{
      .hero, .grid {{ grid-template-columns: 1fr; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <div class="card hero-main">
        <h1>Polybot 自动跟单控制台</h1>
        <p>实时查看资金、持仓、执行与日志。页面每 {refresh_sec} 秒自动刷新一次，适合盯盘与排查风控拦截。</p>
      </div>
      <div class="card stats">
        <div class="stat"><div class="stat-label">Cash</div><div id="cash" class="stat-value">-</div></div>
        <div class="stat"><div class="stat-label">Realized Today</div><div id="realizedPnl" class="stat-value">-</div></div>
        <div class="stat"><div class="stat-label">Unrealized</div><div id="unrealizedPnl" class="stat-value">-</div></div>
        <div class="stat"><div class="stat-label">Equity</div><div id="equity" class="stat-value">-</div></div>
        <div class="stat"><div class="stat-label">Open Positions</div><div id="openPositions" class="stat-value">-</div></div>
        <div class="stat"><div class="stat-label">Cost Basis</div><div id="costBasis" class="stat-value">-</div></div>
      </div>
    </section>

    <section class="grid">
      <div class="card section">
        <h2>当前持仓</h2>
        <div class="scroll">
          <table>
            <thead>
              <tr>
                <th>交易员</th>
                <th>市场</th>
                <th>方向</th>
                <th>交易员持仓</th>
                <th>交易员浮盈亏</th>
                <th>我的持仓</th>
                <th>我的浮盈亏</th>
                <th>跟随占比</th>
              </tr>
            </thead>
            <tbody id="positionsBody"></tbody>
          </table>
        </div>
      </div>

      <div class="card section">
        <h2>交易员信号</h2>
        <div class="scroll">
          <table>
            <thead>
              <tr>
                <th>交易员</th>
                <th>方向</th>
                <th>市场</th>
                <th>价格</th>
                <th>金额</th>
              </tr>
            </thead>
            <tbody id="leaderEventsBody"></tbody>
          </table>
        </div>
      </div>
    </section>

    <section style="margin-top: 16px;">
      <div class="card section">
        <h2>逐笔跟单差异</h2>
        <div class="scroll">
          <table>
            <thead>
              <tr>
                <th>时间</th>
                <th>市场</th>
                <th>交易员订单</th>
                <th>我的请求</th>
                <th>我的成交</th>
                <th>价格偏差</th>
                <th>延迟</th>
                <th>结果</th>
              </tr>
            </thead>
            <tbody id="orderDiffBody"></tbody>
          </table>
        </div>
      </div>
    </section>

    <section class="grid" style="margin-top: 16px;">
      <div class="card section">
        <h2>我的订单</h2>
        <div class="scroll">
          <table>
            <thead>
              <tr>
                <th>状态</th>
                <th>资产</th>
                <th>方向</th>
                <th>请求金额</th>
                <th>组件数</th>
                <th>原因</th>
              </tr>
            </thead>
            <tbody id="ordersBody"></tbody>
          </table>
        </div>
      </div>

      <div class="card section">
        <h2>我的执行</h2>
        <div class="scroll">
          <table>
            <thead>
              <tr>
                <th>状态</th>
                <th>资产</th>
                <th>均价</th>
                <th>数量</th>
                <th>延迟</th>
                <th>结果</th>
              </tr>
            </thead>
            <tbody id="executionsBody"></tbody>
          </table>
        </div>
      </div>
    </section>

    <section class="grid" style="margin-top: 16px;">
      <div class="card section">
        <h2>运行日志</h2>
        <div class="scroll" id="logsBox"></div>
      </div>
      <div class="card section">
        <h2>状态说明</h2>
        <div class="scroll">
          <div class="log"><div>`交易员信号` 是 leader 被监听到的成交。</div></div>
          <div class="log"><div>`逐笔跟单差异` 会把 leader 单笔、你的请求和你的成交放到同一行。`价格偏差` 为正代表你比 leader 更差，为负代表你更好。</div></div>
          <div class="log"><div>`我的订单` 是 bot 聚合并提交风控检查后的订单。</div></div>
          <div class="log"><div>`我的执行` 是 paper/live 执行结果。</div></div>
          <div class="log"><div>`Realized Today` 只在平仓时变化，`Unrealized` 是当前持仓按最新标记价格估算的浮盈浮亏。</div></div>
          <div class="log"><div>`mirrored` 表示这笔仓位 leader 和你都还持有，`leader-only` 表示 leader 还有仓但你没有，`follower-only` 表示你这边还有遗留仓。</div></div>
          <div class="log"><div>`partial` 表示部分成交，超时后剩余部分已取消。</div></div>
          <div class="log"><div>`rejected + timeout canceled` 表示延迟后反复看盘口，直到超时也没拿到可执行流动性。</div></div>
          <div class="log"><div>如果 `交易员信号` 有数据但 `我的订单` 没有，通常是被最小下单额或风控拦住。</div></div>
        </div>
      </div>
    </section>
  </div>

  <script>
    function fmt(v, digits=2) {{
      const n = Number(v);
      return Number.isFinite(n) ? n.toFixed(digits) : "-";
    }}
    function esc(v) {{
      return String(v ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;");
    }}
    function tone(status) {{
      const s = String(status || "").toLowerCase();
      if (s.includes("reject")) return "bad";
      if (s.includes("partial")) return "warn";
      return "good";
    }}
    function positionTone(status) {{
      const s = String(status || "").toLowerCase();
      if (s.includes("leader-only")) return "warn";
      if (s.includes("follower-only")) return "bad";
      return "good";
    }}
    function gapTone(value) {{
      const n = Number(value);
      if (!Number.isFinite(n) || Math.abs(n) < 1) return "";
      return n > 0 ? "bad" : "good";
    }}
    function fmtTime(v) {{
      if (!v) return "-";
      return esc(String(v).replace("T", " ").replace("+00:00", " UTC"));
    }}
    async function refresh() {{
      const res = await fetch("/api/summary");
      const data = await res.json();
      document.getElementById("cash").textContent = fmt(data.runtime.cash_balance_usdc);
      const realizedEl = document.getElementById("realizedPnl");
      realizedEl.textContent = fmt(data.runtime.realized_pnl_today_usdc);
      realizedEl.className = "stat-value " + (Number(data.runtime.realized_pnl_today_usdc) >= 0 ? "good" : "bad");
      const unrealizedEl = document.getElementById("unrealizedPnl");
      unrealizedEl.textContent = fmt(data.runtime.unrealized_pnl_usdc);
      unrealizedEl.className = "stat-value " + (Number(data.runtime.unrealized_pnl_usdc) >= 0 ? "good" : "bad");
      document.getElementById("equity").textContent = fmt(data.runtime.tracked_equity_usdc);
      document.getElementById("openPositions").textContent = data.runtime.open_positions;
      document.getElementById("costBasis").textContent = fmt(data.runtime.total_cost_basis_usdc);

      document.getElementById("positionsBody").innerHTML = data.position_comparison.map(row => `
        <tr>
          <td>${{esc(row.trader_label || row.trader_address)}}</td>
          <td>${{esc(row.market_title || row.market_slug)}}</td>
          <td>${{esc(row.outcome)}}</td>
          <td>
            <div>${{fmt(row.leader_shares, 4)}} sh</div>
            <small>${{fmt(row.leader_mark_price, 4)}} / $${{fmt(row.leader_market_value_usdc)}}</small>
          </td>
          <td class="${{Number(row.leader_unrealized_pnl_usdc) >= 0 ? "good" : "bad"}}">${{fmt(row.leader_unrealized_pnl_usdc)}}</td>
          <td>
            <div>${{fmt(row.follower_shares, 4)}} sh</div>
            <small>${{fmt(row.follower_mark_price, 4)}} / $${{fmt(row.follower_market_value_usdc)}}</small>
          </td>
          <td class="${{Number(row.follower_unrealized_pnl_usdc) >= 0 ? "good" : "bad"}}">${{fmt(row.follower_unrealized_pnl_usdc)}}</td>
          <td class="${{positionTone(row.sync_status)}}">${{fmt(row.follower_vs_leader_pct, 1)}}% · ${{esc(row.sync_status)}}</td>
        </tr>
      `).join("");

      document.getElementById("leaderEventsBody").innerHTML = data.leader_events.map(row => `
        <tr>
          <td>${{esc(row.trader_label || row.trader_address)}}</td>
          <td class="${{tone(row.side)}}">${{esc(row.side)}}</td>
          <td>${{esc(row.market_title || row.market_slug)}}</td>
          <td>${{fmt(row.price, 4)}}</td>
          <td>${{fmt(row.leader_notional_usdc)}}</td>
        </tr>
      `).join("");

      document.getElementById("orderDiffBody").innerHTML = data.order_differences.map(row => `
        <tr>
          <td>
            <div>${{fmtTime(row.leader_observed_at || row.order_created_at)}}</div>
            <small>${{esc(row.trader_label || "")}} · ${{esc(row.side)}} · ${{esc(row.outcome)}}</small>
          </td>
          <td>
            <div>${{esc(row.market_title || row.market_slug)}}</div>
            <small>聚合 ${{row.component_index}} / ${{row.component_count}}</small>
          </td>
          <td>
            <div>${{fmt(row.leader_price, 4)}} · $${{fmt(row.leader_notional_usdc)}}</div>
            <small>${{fmt(row.leader_shares, 4)}} sh</small>
          </td>
          <td>
            <div>$${{fmt(row.follower_requested_amount_usdc)}} · ${{fmt(row.follower_requested_shares, 4)}} sh</div>
            <small>金额比 ${{fmt(row.request_vs_leader_notional_pct, 2)}}%</small>
          </td>
          <td>
            <div>${{row.follower_fill_price > 0 ? fmt(row.follower_fill_price, 4) : "-"}} · $${{fmt(row.follower_executed_amount_usdc)}}</div>
            <small>${{fmt(row.follower_executed_shares, 4)}} sh · ${{esc(row.price_source || "-")}}</small>
          </td>
          <td class="${{gapTone(row.execution_gap_bps)}}">
            <div>${{fmt(row.execution_gap_bps, 1)}} bps</div>
            <small>${{row.execution_gap_bps > 0 ? "比交易员更差" : (row.execution_gap_bps < 0 ? "比交易员更好" : "接近一致")}}</small>
          </td>
          <td>
            <div>paper ${{fmt(row.delay_ms, 0)}} ms</div>
            <small>信号到成交 ${{fmt(row.signal_to_fill_ms, 0)}} ms</small>
          </td>
          <td class="${{tone(row.status)}}">
            <div>${{esc(row.status)}}</div>
            <small>${{esc(row.reason || "")}}</small>
          </td>
        </tr>
      `).join("");

      document.getElementById("executionsBody").innerHTML = data.executions.map(row => `
        <tr>
          <td class="${{tone(row.status)}}">${{esc(row.status)}}</td>
          <td class="mono">${{esc(row.token_id)}}</td>
          <td>${{fmt(row.avg_price, 4)}}</td>
          <td>${{fmt(row.executed_shares, 4)}}</td>
          <td>${{fmt(row.raw_response?.delay_ms, 0)}} ms / ${{Number(row.raw_response?.poll_count || 0)}}x</td>
          <td>${{esc(row.reason || "")}}${{row.raw_response?.timed_out ? " · timed out" : ""}}</td>
        </tr>
      `).join("");

      document.getElementById("ordersBody").innerHTML = data.orders.map(row => `
        <tr>
          <td class="${{tone(row.status)}}">${{esc(row.status)}}</td>
          <td class="mono">${{esc(row.token_id)}}</td>
          <td>${{esc(row.side)}}</td>
          <td>${{fmt(row.requested_amount_usdc)}}</td>
          <td>${{Array.isArray(row.components) ? row.components.length : 0}}</td>
          <td>${{esc(row.reason || "")}}</td>
        </tr>
      `).join("");

      document.getElementById("logsBox").innerHTML = data.logs.map(row => `
        <div class="log">
          <div class="${{tone(row.level)}}">${{esc(row.level)}} · ${{esc(row.message)}}</div>
          <small>${{esc(row.created_at)}}${{row.extra ? " · " + esc(JSON.stringify(row.extra)) : ""}}</small>
        </div>
      `).join("");
    }}
    refresh();
    setInterval(refresh, {refresh_sec * 1000});
  </script>
</body>
</html>"""


class DashboardServer:
    def __init__(self, repository: BaseRepository, host: str, port: int, refresh_sec: int):
        self.repository = repository
        self.host = host
        self.port = port
        self.refresh_sec = refresh_sec
        self.httpd = ThreadingHTTPServer((host, port), self._handler())
        self.thread: threading.Thread | None = None

    def _handler(self):
        repository = self.repository
        html = build_dashboard_html(self.refresh_sec).encode("utf-8")

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802
                if self.path == "/":
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.end_headers()
                    self.wfile.write(html)
                    return
                if self.path == "/api/summary":
                    payload = json.dumps(build_dashboard_payload(repository), ensure_ascii=False).encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.end_headers()
                    self.wfile.write(payload)
                    return
                self.send_response(404)
                self.end_headers()

            def log_message(self, format, *args):  # noqa: A003
                return

        return Handler

    def serve_forever(self) -> None:
        self.httpd.serve_forever()

    def start_in_thread(self) -> None:
        self.thread = threading.Thread(target=self.serve_forever, daemon=True)
        self.thread.start()
