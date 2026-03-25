# Polybot - Polymarket 交易员分析工具

这个工具按你给的流程实现了一个可落地的分析流水线：

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
