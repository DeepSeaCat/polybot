from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


def build_html_report(df: pd.DataFrame, output_path: Path) -> None:
    rows = df.to_dict(orient="records")
    payload = json.dumps(rows, ensure_ascii=False)

    html = """<!doctype html>
<html lang=\"zh-CN\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>Polybot 分析看板</title>
  <style>
    :root {
      --bg: #f6f8fb;
      --card: #ffffff;
      --ink: #1b2430;
      --muted: #5f6b7a;
      --line: #dce3ed;
      --accent: #0b7285;
      --good: #1b9e59;
      --warn: #e67700;
    }
    body { margin: 0; font-family: ui-sans-serif, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; background: radial-gradient(1200px 500px at 90% -10%, #d6f0ff, transparent), var(--bg); color: var(--ink); }
    .wrap { max-width: 1600px; margin: 24px auto; padding: 0 16px; }
    .head { background: var(--card); border: 1px solid var(--line); border-radius: 14px; padding: 16px; margin-bottom: 14px; }
    .title { font-size: 24px; font-weight: 800; margin: 0 0 6px; }
    .sub { color: var(--muted); margin: 0; }
    .filters { display: grid; grid-template-columns: repeat(8, minmax(140px, 1fr)); gap: 10px; margin-top: 14px; }
    .filters .box { background: #f9fbfd; border: 1px solid var(--line); border-radius: 10px; padding: 10px; }
    label { display: block; font-size: 12px; color: var(--muted); margin-bottom: 6px; }
    input { width: 100%; box-sizing: border-box; border: 1px solid var(--line); border-radius: 8px; padding: 8px; }
    button { border: 0; background: var(--accent); color: #fff; border-radius: 8px; padding: 9px 12px; font-weight: 700; cursor: pointer; }
    .toolbar { display: flex; gap: 8px; align-items: center; margin-top: 10px; }
    .table-wrap { background: var(--card); border: 1px solid var(--line); border-radius: 14px; overflow: auto; }
    table { width: 100%; border-collapse: collapse; min-width: 1450px; }
    th, td { border-bottom: 1px solid var(--line); padding: 8px 10px; text-align: left; white-space: nowrap; }
    th { position: sticky; top: 0; background: #f1f6fa; font-size: 12px; color: #304154; cursor: pointer; }
    tr:hover { background: #f7fbff; }
    .tag { display: inline-block; padding: 2px 8px; border-radius: 999px; font-size: 11px; font-weight: 700; }
    .tag.good { background: #e6f8ef; color: var(--good); }
    .tag.warn { background: #fff3e0; color: var(--warn); }
    .meta { margin: 8px 0 0; color: var(--muted); font-size: 13px; }
  </style>
</head>
<body>
  <div class=\"wrap\">
    <section class=\"head\">
      <h1 class=\"title\">Polybot 交易员筛选看板</h1>
      <p class=\"sub\">优先筛选: 胜率高 + 买入接近开盘时间 + 5分钟首分钟单决策纪律。</p>
      <div class=\"filters\">
        <div class=\"box\"><label>最小胜率 win_rate_90d</label><input id=\"minWinRate\" type=\"number\" step=\"0.01\" value=\"0.70\"></div>
        <div class=\"box\"><label>最小开盘附近买入比(10m)</label><input id=\"minNearOpen10\" type=\"number\" step=\"0.01\" value=\"0.35\"></div>
        <div class=\"box\"><label>最小买入样本数</label><input id=\"minBuyCount\" type=\"number\" step=\"1\" value=\"10\"></div>
        <div class=\"box\"><label>最小5m首分钟单决策比</label><input id=\"minCycle5Ratio\" type=\"number\" step=\"0.01\" value=\"0.70\"></div>
        <div class=\"box\"><label>最小5m窗口样本</label><input id=\"minCycle5Windows\" type=\"number\" step=\"1\" value=\"20\"></div>
        <div class=\"box\"><label>最小可复制分</label><input id=\"minCopy\" type=\"number\" step=\"1\" value=\"0\"></div>
        <div class=\"box\"><label>最大日均交易数(30d)</label><input id=\"maxTradesPerDay\" type=\"number\" step=\"0.1\" value=\"20\"></div>
        <div class=\"box\"><label>关键词(地址/昵称/X)</label><input id=\"q\" type=\"text\" placeholder=\"可留空\"></div>
      </div>
      <div class=\"toolbar\">
        <button id=\"applyBtn\">应用筛选</button>
        <button id=\"resetBtn\">重置</button>
      </div>
      <p id=\"meta\" class=\"meta\"></p>
    </section>

    <section class=\"table-wrap\">
      <table id=\"tbl\">
        <thead>
          <tr>
            <th data-k=\"copyability_score\">CopyScore</th>
            <th data-k=\"win_rate_90d\">胜率</th>
            <th data-k=\"near_open_buy_ratio_10m\">开盘买入比10m</th>
            <th data-k=\"near_open_buy_ratio_30m\">开盘买入比30m</th>
            <th data-k=\"cycle5_first_minute_single_ratio\">5m首分钟单决策比</th>
            <th data-k=\"cycle5_window_count\">5m窗口数</th>
            <th data-k=\"median_open_delay_min\">开盘延迟中位(分)</th>
            <th data-k=\"buy_count_with_open_time\">买入样本数</th>
            <th data-k=\"avg_trades_per_day_30d\">日均交易30d</th>
            <th data-k=\"realized_pnl_90d\">PnL 90d</th>
            <th>交易员</th>
            <th data-k=\"address\">地址</th>
            <th data-k=\"name\">昵称</th>
            <th data-k=\"x_username\">X</th>
          </tr>
        </thead>
        <tbody></tbody>
      </table>
    </section>
  </div>

<script>
const raw = __PAYLOAD__;
let rows = raw.slice();
let sortKey = 'copyability_score';
let sortDesc = true;

const els = {
  minWinRate: document.getElementById('minWinRate'),
  minNearOpen10: document.getElementById('minNearOpen10'),
  minBuyCount: document.getElementById('minBuyCount'),
  minCycle5Ratio: document.getElementById('minCycle5Ratio'),
  minCycle5Windows: document.getElementById('minCycle5Windows'),
  minCopy: document.getElementById('minCopy'),
  maxTradesPerDay: document.getElementById('maxTradesPerDay'),
  q: document.getElementById('q'),
  meta: document.getElementById('meta'),
  tbody: document.querySelector('#tbl tbody')
};

function num(v, d=0) {
  const n = Number(v);
  return Number.isFinite(n) ? n : d;
}
function f3(v) { return num(v).toFixed(3); }
function f2(v) { return num(v).toFixed(2); }
function f0(v) { return Math.round(num(v)); }
function esc(v) {
  return String(v ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}
function shortAddr(addr) {
  const s = String(addr || '');
  if (s.length <= 14) return s;
  return `${s.slice(0, 8)}...${s.slice(-6)}`;
}

function hitKeyword(r, q) {
  if (!q) return true;
  const t = [r.address, r.name, r.x_username].join(' ').toLowerCase();
  return t.includes(q.toLowerCase());
}

function applyFilters() {
  const minWinRate = num(els.minWinRate.value, 0);
  const minNearOpen10 = num(els.minNearOpen10.value, 0);
  const minBuyCount = num(els.minBuyCount.value, 0);
  const minCycle5Ratio = num(els.minCycle5Ratio.value, 0);
  const minCycle5Windows = num(els.minCycle5Windows.value, 0);
  const minCopy = num(els.minCopy.value, 0);
  const maxTradesPerDay = num(els.maxTradesPerDay.value, 9999);
  const q = (els.q.value || '').trim();

  rows = raw.filter(r =>
    num(r.win_rate_90d) >= minWinRate &&
    num(r.near_open_buy_ratio_10m) >= minNearOpen10 &&
    num(r.buy_count_with_open_time) >= minBuyCount &&
    num(r.cycle5_first_minute_single_ratio) >= minCycle5Ratio &&
    num(r.cycle5_window_count) >= minCycle5Windows &&
    num(r.copyability_score) >= minCopy &&
    num(r.avg_trades_per_day_30d) <= maxTradesPerDay &&
    hitKeyword(r, q)
  );

  render();
}

function sortRows() {
  rows.sort((a,b) => {
    const av = a[sortKey];
    const bv = b[sortKey];
    if (typeof av === 'number' && typeof bv === 'number') return sortDesc ? (bv-av) : (av-bv);
    return sortDesc ? String(bv).localeCompare(String(av)) : String(av).localeCompare(String(bv));
  });
}

function render() {
  sortRows();
  els.meta.textContent = `命中 ${rows.length} / ${raw.length} 个交易员`;
  els.tbody.innerHTML = rows.map(r => {
    const flag = (num(r.win_rate_90d) >= 0.8 && num(r.near_open_buy_ratio_10m) >= 0.7)
      ? '<span class="tag good">高胜率+近开盘</span>'
      : '<span class="tag warn">待观察</span>';

    const addr = esc(r.address || '');
    const profileUrl = addr ? `https://polymarket.com/profile/${encodeURIComponent(addr)}` : '';
    const traderLink = profileUrl ? `<a href="${profileUrl}" target="_blank" rel="noopener">打开主页</a>` : '';

    return `<tr>
      <td>${f2(r.copyability_score)} ${flag}</td>
      <td>${f3(r.win_rate_90d)}</td>
      <td>${f3(r.near_open_buy_ratio_10m)}</td>
      <td>${f3(r.near_open_buy_ratio_30m)}</td>
      <td>${f3(r.cycle5_first_minute_single_ratio)}</td>
      <td>${f0(r.cycle5_window_count)}</td>
      <td>${f2(r.median_open_delay_min)}</td>
      <td>${f0(r.buy_count_with_open_time)}</td>
      <td>${f2(r.avg_trades_per_day_30d)}</td>
      <td>${f2(r.realized_pnl_90d)}</td>
      <td>${traderLink}</td>
      <td>${profileUrl ? `<a href="${profileUrl}" target="_blank" rel="noopener">${esc(shortAddr(addr))}</a>` : addr}</td>
      <td>${esc(r.name || '')}</td>
      <td>${esc(r.x_username || '')}</td>
    </tr>`;
  }).join('');
}

document.querySelectorAll('#tbl th').forEach(th => {
  th.addEventListener('click', () => {
    const k = th.getAttribute('data-k');
    if (k === sortKey) sortDesc = !sortDesc;
    else { sortKey = k; sortDesc = true; }
    render();
  });
});

document.getElementById('applyBtn').addEventListener('click', applyFilters);
document.getElementById('resetBtn').addEventListener('click', () => {
  els.minWinRate.value = '0.70';
  els.minNearOpen10.value = '0.35';
  els.minBuyCount.value = '10';
  els.minCycle5Ratio.value = '0.70';
  els.minCycle5Windows.value = '20';
  els.minCopy.value = '0';
  els.maxTradesPerDay.value = '20';
  els.q.value = '';
  applyFilters();
});

applyFilters();
</script>
</body>
</html>
"""

    html = html.replace("__PAYLOAD__", payload)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")
