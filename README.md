# Polybot - Polymarket 分析 + 自动跟单机器人

这个仓库现在包含两部分：

1. `交易员分析器`：从 leaderboard 拉高手，计算 Copyability Score，并生成网页报告。
2. `自动跟单机器人`：实时监听指定地址或策略信号，按比例复制交易，带风控、仓位追踪、聚合执行、MongoDB 持久化和可视化监控。

分析器按你给的流程实现了一个可落地的分析流水线：

1. 从 leaderboard 拉候选交易员（支持 `category/interval/orderBy/limit`）。
2. 对每个地址拉取：
	 - public profile
	 - current positions
	 - closed positions
	 - user trades
	 - user activity
	 - accounting snapshot URL（如果接口返回）
3. 计算 `Copyability Score`（可复制分），并导出排名与原始数据。

## 目录结构

```
poly_analyzer.py
requirements.txt
src/polybot/
	config.py
	client.py
	scoring.py
	pipeline.py
outputs/
```

## 快速开始

### 1) 安装依赖

```bash
pip install -r requirements.txt
```

如需本地开发测试：

```bash
pip install -r requirements-dev.txt
```

如需真实下单：

```bash
pip install -r requirements-live.txt
```

### 2) 运行分析

```bash
python poly_analyzer.py \
	--category CRYPTO \
	--interval MONTH \
	--order-by PNL \
	--limit 100 \
	--output outputs/monthly_crypto
```

常用参数：

- `--category`: `CRYPTO` 等分类
- `--interval`: `DAY` / `WEEK` / `MONTH` / `ALL`
- `--order-by`: `PNL` 或 `VOL`
- `--limit`: 候选人数
- `--top`: 控制终端打印前 N 名

## 输出结果

运行后会生成：

- `outputs/.../trader_scores.csv`：总表，按 `copyability_score` 排序
- `outputs/.../trader_scores.json`：同内容 JSON
- `outputs/.../raw/<address>.json`：每个地址的原始数据和评分拆解
- `outputs/.../report.html`：可交互网页看板（可直接本地打开）

## 网页筛选（你要的高胜率 + 开盘买入）

网页里默认就按下面思路筛选：

- `win_rate_90d >= 0.70`
- `near_open_buy_ratio_10m >= 0.60`
- `buy_count_with_open_time >= 30`

其中新增字段含义：

- `near_open_buy_ratio_10m`：买入发生在开盘后 10 分钟内的比例
- `near_open_buy_ratio_30m`：买入发生在开盘后 30 分钟内的比例
- `avg_open_delay_min` / `median_open_delay_min`：买入时间相对开盘时间的延迟（分钟）
- `high_win_near_open`：是否命中“高胜率 + 近开盘买入”

开盘时间来自 `gamma-api` 的市场元数据（优先 `startDate`）。

## 评分框架（默认）

总分 `Copyability Score` 范围 `0-100`，由 5 个维度加权：

- `profitability`（收益能力）
	- 90d realized PnL
	- 平均单笔收益
	- 盈亏比（profit factor）
- `stability`（稳定性）
	- 最大回撤
	- 最大连续亏损
	- 周收益波动
- `copyability`（可复制性）
	- 30d 日均交易频次（中低频更优）
	- 平均持仓时长
	- 临近结算抢跑比例
- `focus`（专注度）
	- crypto 占比
	- BTC 占比
	- 市场集中度（HHI）
- `execution`（执行难度）
	- 中位成交规模（普通账户可复制性）

你可以在 `src/polybot/config.py` 调整权重。

## API 端点说明

Polymarket 文档可能迭代，工具里把端点做成了候选路径自动回退。

- 默认 base URL: `https://data-api.polymarket.com`
- 端点候选定义位置：`src/polybot/config.py` 的 `ApiPaths`

如果你运行时报 endpoint 404 或参数不匹配，直接改 `ApiPaths` 即可，不需要改业务逻辑。

## 注意

- 该工具优先做“非高频可复制”筛选，不是单纯追求排行榜第一。
- 胜率只是参考项之一，核心是频率、持仓时长、回撤、专注度、执行难度。
- 若需更精细的复盘（比如盘口深度、滑点），建议再接 CLOB 深度数据。

## 自动跟单机器人

### 能力概览

- 实时监听多个高手地址
- 支持本地 JSONL 策略信号输入
- 智能仓位管理：按 leader 成交金额 * `copy_ratio` 复制
- 分级乘数：大单可按 tier 放大
- 影子持仓追踪：leader 卖出时按 shadow lots 比例平仓
- 订单聚合：多个小额同向信号聚合成一个可执行订单
- 风控：总仓位、单市场、单交易员、单笔、日损、滑点、leader 偏离保护
- `paper` 模拟盘与 `live` 实盘模式
- MongoDB 持久化
- Dashboard 可视化监控与日志追踪

### 目录补充

```
poly_copybot.py
configs/
  copybot.example.json
src/polybot/
  copybot_models.py
  copybot_storage.py
  copybot_services.py
  copybot_execution.py
  copybot_dashboard.py
  copybot_runtime.py
tests/
  test_copybot_core.py
```

### 配置

复制 [configs/copybot.example.json](configs/copybot.example.json) 作为你的运行配置，核心字段：

- `traders`: 要跟踪的高手地址列表
- `sizing_mode`: `fixed_notional` 或 `leader_fraction_of_equity`
- `copy_ratio`: 跟单比例
- `multiplier_tiers`: 分级乘数
- `wallet.mode`: `paper` 或 `live`
- `risk`: 风控上限
- `mongo`: MongoDB 连接信息
- `dashboard`: 监控页面地址
- `strategy_signal_file`: 可选，本地 JSONL 信号源

两种 sizing 的区别：

- `fixed_notional`：按 leader 成交金额跟。例：他下 `100 USDC`，`copy_ratio=0.2`，你下 `20 USDC`。
- `leader_fraction_of_equity`：按 leader 仓位占比跟。例：他总资金 `1000 USDC`，某单下 `200 USDC`，占 `20%`；如果你当前资金是 `500 USDC` 且 `copy_ratio=1.0`，你下 `100 USDC`。

### 运行机器人

先校验配置：

```bash
python poly_copybot.py validate-config --config configs/copybot.example.json
```

以模拟盘启动：

```bash
python poly_copybot.py run --config configs/copybot.example.json
```

只跑有限轮轮询做 dry-run：

```bash
python poly_copybot.py run --config configs/copybot.example.json --max-cycles 5
```

只开监控页：

```bash
python poly_copybot.py serve-dashboard --config configs/copybot.example.json
```

默认 dashboard 地址：

```text
http://127.0.0.1:8088
```

### 部署到服务器

仓库现在带了一个最小可用的容器部署入口：

- [Dockerfile](/Users/dida/PycharmProjects/polybot/Dockerfile)
- [docker-compose.server.example.yml](/Users/dida/PycharmProjects/polybot/docker-compose.server.example.yml)
- [.dockerignore](/Users/dida/PycharmProjects/polybot/.dockerignore)

部署前，建议先准备一份服务器专用配置，例如 `configs/server.copybot.json`，重点注意这几项：

- `dashboard.host` 改成 `0.0.0.0`
- `mongo.enabled` 如果希望重启不丢 paper 状态，改成 `true`
- `http_proxy` / `https_proxy` 只在服务器确实需要代理时再填
- 如果只是看延迟，先继续用 `wallet.mode = "paper"`

最简单的 Docker 方式：

```bash
cp docker-compose.server.example.yml docker-compose.yml
# 准备你的 configs/server.copybot.json
docker compose up -d --build
```

启动后：

- Dashboard: `http://<你的服务器IP>:8088`
- 查看日志：`docker compose logs -f polybot`

如果你不用 Docker，也可以直接在服务器上跑：

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python poly_copybot.py run --config configs/server.copybot.json
```

### 上服务器后怎么看真实延迟

Dashboard 里的 `逐笔跟单差异` 表已经把延迟拆开了：

- `paper xxx ms`
  这是你配置里的模拟执行延迟
- `信号到成交 xxx ms`
  这是从 leader 成交被我们观察到，到你的 paper 成交完成的总耗时

你部署到服务器后，重点看这个表的两列：

- `价格偏差`
- `信号到成交`

如果服务器部署后 `信号到成交` 从本地的十几秒/几十秒明显降下来，就说明瓶颈主要在你本地网络或代理。
如果依然很高，说明瓶颈更可能在当前 REST 轮询方案本身，而不是你的机器。

### 策略信号格式

`strategy_signal_file` 支持每行一个 JSON：

```json
{"strategy_id":"strategy:manual-alpha","side":"BUY","token_id":"123","market_slug":"btc-above-100k","market_title":"BTC > 100k?","outcome":"Yes","price":0.42,"leader_notional_usdc":25}
```

注意：

- `strategy_id` 需要在 `traders` 中有同名配置项，便于复用同一套资金/风控参数。
- `BUY` 优先按 `leader_notional_usdc` 复制。
- `SELL` 建议同时给 `leader_shares`，方便精确按 shadow position 比例减仓。

### 实盘说明

`live` 模式通过官方 `py-clob-client` 下单，但本地环境要先满足其依赖编译条件，并配置：

- `POLYMARKET_PRIVATE_KEY`
- `POLYMARKET_FUNDER`
- 可选：`POLYMARKET_API_KEY`
- 可选：`POLYMARKET_API_SECRET`
- 可选：`POLYMARKET_API_PASSPHRASE`

如果你只是先验证策略逻辑，建议先用 `paper` 模式把监听、风控、聚合和仓位账本跑顺。
