# 预测市场微观结构分析 — 项目总览

> 论文：Becker, J. (2026). *The Microstructure of Wealth Transfer in Prediction Markets*
> https://jbecker.dev/research/prediction-market-microstructure

## 这个项目是什么？

这是一个**预测市场研究框架**，用于分析两大预测市场平台（Kalshi、Polymarket）的交易数据，研究市场微观结构问题：

- 市场定价是否准确（calibration 校准分析）
- Maker 和 Taker 谁赚谁亏（财富转移方向）
- 长尾合约（低概率事件）是否存在系统性定价偏差
- 交易行为随时间的演变趋势

项目包含三个部分：
1. **数据采集工具**（Indexers）：从 API 和区块链爬取数据
2. **预下载数据集**：约 50GB 的历史交易数据（已下载到 `data/`）
3. **分析脚本**：23 个自动化分析，生成图表、CSV 和统计数据

---

## 两个数据源

### Kalshi（美国合规预测交易所）

- **获取方式**：REST API（带分页 cursor）
- **数据内容**：
  - `data/kalshi/markets/` — 市场元数据（ticker、标题、状态、结果等）
  - `data/kalshi/trades/` — 每笔交易记录（价格、数量、taker 方向）
- **价格格式**：美分制，`yes_price` 范围 1-99，`no_price = 100 - yes_price`
- **关键字段**：`taker_side`（标明是 taker 买 YES 还是 NO，这是区分 maker/taker 的关键）

### Polymarket（去中心化预测市场）

- **获取方式**：Polygon 区块链 RPC（Web3）+ REST API
- **数据内容**：
  - `data/polymarket/markets/` — 市场元数据（问题、结果价格、流动性）
  - `data/polymarket/trades/` — CTF Exchange 的 `OrderFilled` 链上事件（2023 至今）
  - `data/polymarket/legacy_trades/` — Legacy FPMM 交易（2020-2022，旧的 AMM 模式）
  - `data/polymarket/blocks/` — 区块号 → 时间戳映射表
  - `data/polymarket/fpmm_collateral_lookup.json` — FPMM 合约地址 → 抵押品代币映射
- **价格格式**：小数制（0-1），通过 `maker_amount / taker_amount` 计算

### 数据规模

| 数据集 | 大小 | 文件数 |
|--------|------|--------|
| Kalshi Markets | 570 MB | 769 个 parquet |
| Kalshi Trades | 3.3 GB | 7,214 个 parquet |
| Polymarket Markets | 102 MB | 41 个 parquet |
| Polymarket Trades (CTF) | 45 GB | 40,454 个 parquet |
| Polymarket Blocks | 843 MB | 785 个 parquet |
| Polymarket Legacy Trades | 211 MB | 221 个 parquet |
| **总计** | **~50 GB** | |

---

## 数据管道架构

```
Indexers（数据采集）
    │
    │  Kalshi: REST API → cursor 分页 → 并发10线程
    │  Polymarket: Polygon RPC → 区块范围扫描 → 断点续传
    ▼
Parquet 分片存储（ParquetStorage）
    │
    │  每 10,000 条记录一个 parquet 文件
    │  自动去重（按 trade_id / ticker）
    ▼
DuckDB SQL 查询引擎
    │
    │  直接查询 parquet 文件，无需导入数据库
    │  典型模式：trades JOIN markets ON ticker → 关联结果
    ▼
分析脚本（Analysis）
    │
    │  计算指标 → 生成 matplotlib 图表 → 导出 CSV/JSON
    ▼
output/ 目录
    PNG, PDF, CSV, JSON, GIF
```

### 核心设计

- **存储层**（`src/common/storage.py`）：`ParquetStorage` 类管理分片写入和去重
- **查询层**：DuckDB 原生支持 glob 查询多个 parquet 文件，例如 `SELECT * FROM 'data/kalshi/trades/*.parquet'`
- **分析基类**（`src/common/analysis.py`）：所有分析继承 `Analysis`，实现 `run()` 方法返回图表+数据
- **自动发现**：`Analysis.load()` 扫描 `src/analysis/` 目录自动发现所有分析类

---

## 全部 23 个分析脚本

### 一、市场概览

| # | 脚本名 | 说明 |
|---|--------|------|
| 1 | `meta_stats` | 数据集基础统计量（总交易数、总交易额、市场数等） |
| 2 | `market_types` | 市场类型分布树状图（按 notional volume 展示各类别占比） |

### 二、市场校准（Calibration）

核心问题：**合约价格是否等于事件实际发生的概率？**

例如价格 60 美分的合约，对应的事件是否确实有 60% 的概率发生？

| # | 脚本名 | 说明 |
|---|--------|------|
| 3 | `win_rate_by_price` | Kalshi 胜率 vs 价格校准图（最基础的校准分析） |
| 4 | `kalshi_calibration_deviation_over_time` | 校准偏差（MAD）随时间的变化趋势 |
| 5 | `mispricing_by_price` | 按价格段的定价偏差百分比（taker/maker/总体） |
| 6 | `polymarket_win_rate_by_price` | Polymarket 校准分析（含 Brier Score、ECE、Log Loss） |
| 7 | `win_rate_by_price_animated` | **跨平台对比**：Kalshi vs Polymarket 校准曲线的动画 GIF |

### 三、Maker vs Taker 动态

核心问题：**谁在赚钱？做市商（Maker）还是主动交易者（Taker）？**

- **Taker**：主动下单方（吃单者），按市场价成交
- **Maker**：挂单方（做市者），提供流动性

| # | 脚本名 | 说明 |
|---|--------|------|
| 8 | `maker_vs_taker_returns` | Maker 和 Taker 在各价格段的超额收益对比 |
| 9 | `maker_taker_gap_over_time` | Maker-Taker 收益差距随季度变化趋势 |
| 10 | `maker_taker_returns_by_category` | 按市场类别（体育、政治、金融等）的 Maker/Taker 收益 |
| 11 | `maker_returns_by_direction` | Maker 在 YES vs NO 方向上的超额收益 |
| 12 | `maker_win_rate_by_direction` | Maker 胜率按方向（YES/NO）的分解 |
| 13 | `trade_size_by_role` | Maker 和 Taker 的平均/中位交易规模对比 |
| 14 | `win_rate_by_trade_size` | 胜率是否与交易规模相关 |

### 四、交易行为分析

| # | 脚本名 | 说明 |
|---|--------|------|
| 15 | `yes_vs_no_by_price` | 各价格段 YES/NO 成交量占比（按 taker/maker 分解） |
| 16 | `ev_yes_vs_no` | YES vs NO 的期望价值（EV）比较 |
| 17 | `longshot_volume_share_over_time` | 低概率合约（1-20 美分）的交易量占比随时间变化 |

### 五、时间模式

| # | 脚本名 | 说明 |
|---|--------|------|
| 18 | `volume_over_time` | Kalshi 季度交易量（对数坐标） |
| 19 | `returns_by_hour` | 一天内不同小时的超额收益分布（东部时间） |
| 20 | `vwap_by_hour` | 一天内不同小时的 VWAP 分布 |

### 六、统计检验

| # | 脚本名 | 说明 |
|---|--------|------|
| 21 | `statistical_tests` | 5 项严格统计检验（Mann-Whitney U、z-test、t-test、相关性等） |

### 七、Polymarket 专属

| # | 脚本名 | 说明 |
|---|--------|------|
| 22 | `polymarket_volume_over_time` | Polymarket 季度交易量（合并 CTF + Legacy） |
| 23 | `polymarket_trades_over_time` | Polymarket 逐区块的交易数量 |

---

## 分析的典型 SQL 模式

### Kalshi：交易 + 市场结果关联

```sql
SELECT
    t.yes_price AS price,
    CASE WHEN m.result = t.taker_side THEN 1 ELSE 0 END AS won
FROM 'data/kalshi/trades/*.parquet' t
JOIN 'data/kalshi/markets/*.parquet' m ON t.ticker = m.ticker
WHERE m.result IN ('yes', 'no')
```

### Polymarket：通过 token_id 关联结果

```sql
-- 先从 markets 构建 token_id → won 映射
-- 再 JOIN trades 计算胜率
-- 价格 = maker_amount / taker_amount（当 taker 用 USDC 买 token 时）
```

### 价格理解

- **Kalshi**：`yes_price = 65` 意味着花 $0.65 买 YES 合约，赢了得 $1.00（隐含概率 65%）
- **Polymarket**：`price = 0.65` 同理，但用小数表示
- **超额收益** = `win_rate - implied_probability`（正数说明该策略盈利）

---

## 如何运行

### 环境准备

```bash
# 安装依赖（需要 Python 3.9+ 和 uv）
uv sync
```

### 下载数据（如果还没有）

```bash
make setup    # 下载 36GB 压缩包并解压
```

### 运行分析

```bash
# 交互式菜单选择
make analyze

# 运行全部分析
uv run main.py analyze all

# 运行指定分析
uv run main.py analyze win_rate_by_price
uv run main.py analyze polymarket_win_rate_by_price
```

### 输出位置

所有输出保存在 `output/` 目录：
- `.png` / `.pdf` — 图表
- `.csv` — 数据表格
- `.json` — 网页可用的图表配置
- `.gif` — 动画（仅 `win_rate_by_price_animated`）

---

## 项目结构

```
prediction-market-analysis/
├── main.py                           # CLI 入口：analyze / index / package
├── pyproject.toml                    # 依赖配置
├── Makefile                          # 快捷命令
│
├── src/
│   ├── common/
│   │   ├── analysis.py               # Analysis 基类（run/save/load）
│   │   ├── indexer.py                # Indexer 基类
│   │   ├── storage.py                # ParquetStorage（分片写入+去重）
│   │   ├── client.py                 # HTTP 重试装饰器
│   │   └── interfaces/chart.py       # 图表配置类型定义
│   │
│   ├── indexers/
│   │   ├── kalshi/                   # Kalshi 数据采集
│   │   │   ├── client.py             #   API 客户端
│   │   │   ├── markets.py            #   市场索引器
│   │   │   └── trades.py             #   交易索引器
│   │   └── polymarket/               # Polymarket 数据采集
│   │       ├── blockchain.py          #   Polygon Web3 客户端
│   │       ├── blocks.py             #   区块时间戳索引器
│   │       ├── markets.py            #   市场索引器
│   │       ├── trades.py             #   CTF 交易索引器
│   │       └── fpmm_trades.py        #   Legacy FPMM 索引器
│   │
│   └── analysis/
│       ├── kalshi/                   # 20 个 Kalshi 分析
│       │   ├── win_rate_by_price.py
│       │   ├── maker_vs_taker_returns.py
│       │   ├── ... (共 20 个)
│       │   └── util/categories.py    # 市场分类工具（570+ 行正则模式）
│       ├── polymarket/               # 3 个 Polymarket 分析
│       └── comparison/               # 1 个跨平台对比分析
│
├── data/                             # 数据目录（~50GB）
│   ├── kalshi/{markets,trades}/
│   └── polymarket/{markets,trades,blocks,legacy_trades}/
│
├── output/                           # 分析输出目录
├── docs/
│   ├── SCHEMAS.md                    # 数据 schema 文档
│   └── ANALYSIS.md                   # 如何编写新分析
└── scripts/
    ├── download.sh                   # 数据集下载脚本
    └── install-tools.sh              # 工具安装脚本
```

---

## 技术栈

| 组件 | 技术 |
|------|------|
| 包管理 | uv (Python 3.9+) |
| 数据存储 | Apache Parquet (PyArrow) |
| 查询引擎 | DuckDB（SQL 直接查询 Parquet） |
| 数据处理 | pandas, numpy |
| 可视化 | matplotlib, squarify |
| 统计 | scipy |
| 区块链 | web3.py (Polygon RPC) |
| HTTP | httpx + tenacity (重试) |
| 代码质量 | ruff (lint + format) |
