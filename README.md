# Binance-Connector

Binance 现货行情 Connector，为 KlineChartQuant 提供 K 线、原始逐笔成交、聚合成交、
实时盘口以及历史成交归档。目标是让 Footprint（足迹图）和订单流类功能拿到**真实成交**
和**主动买卖方向**，而不是把报价或盘口变化当成成交量。

- 语言：Go 1.24（单 module）
- 默认监听：`127.0.0.1:8091`
- 市场：仅 `spot`（合约未实现，见「未完成」）
- 协议：`/api/v1/market-data`（V1 风格 envelope），另有 `/health`

## 快速开始

```bash
# 依赖（gorilla/websocket）
go mod download

# 运行（默认代理 127.0.0.1:6666）
go run ./cmd/binance-connector

# 构建
go build -o bin/binance-connector.exe ./cmd/binance-connector

# 测试（不依赖外网）
go test -race ./...

# 真实 Binance 连通性验证（需代理可用）
$env:BINANCE_LIVE_TEST='1'; go test ./internal/binance -run '^TestLive' -v
```

## 配置

全部通过环境变量读取，业务代码不直接访问 `os.environ`。

| 变量 | 默认 | 说明 |
|---|---|---|
| `BINANCE_LISTEN` | `127.0.0.1:8091` | HTTP 监听地址 |
| `BINANCE_REST_URL` | `https://data-api.binance.vision` | 公共行情 REST（免 Key） |
| `BINANCE_HISTORY_URL` | `https://api.binance.com` | `historicalTrades` 专用域名 |
| `BINANCE_WS_URL` | `wss://data-stream.binance.vision` | 公共行情 WebSocket |
| `BINANCE_ARCHIVE_URL` | `https://data.binance.vision` | 历史成交归档 |
| `BINANCE_CACHE_DIR` | `data/archives` | 归档缓存目录 |
| `BINANCE_API_KEY` | 空 | 仅 `historicalTrades` 可能用到 |
| `BINANCE_PROXY` | `HTTP(S)_PROXY` → `http://127.0.0.1:6666` | HTTP 与 WS 共用；设为 `direct` 表示直连 |
| `BINANCE_HTTP_TIMEOUT` | `20s` | REST 超时 |
| `BINANCE_ARCHIVE_TIMEOUT` | `5m` | 归档下载超时 |
| `BINANCE_WEIGHT_PER_MINUTE` | `1200` | 出口请求权重预算 |
| `BINANCE_CATCHUP_PAGES` | `20` | 断线回补最大分页 |
| `BINANCE_MAX_BOOK_LEVELS` | `50000` | 已知盘口档位上限 |

**代理说明**：`data-api.binance.vision` 通常可直连；`api.binance.com`（`historicalTrades`）
在部分网络需要代理。默认走 `127.0.0.1:6666`，如需直连设 `BINANCE_PROXY=direct`。

## 接口

| 方法 | 路径 | 用途 |
|---|---|---|
| GET | `/health` | 健康检查 |
| GET | `/api/v1/market-data/sources/binance/probe` | 上游探测与能力清单 |
| POST | `/api/v1/market-data/instruments/search` | 现货品种搜索 |
| POST | `/api/v1/market-data/bars` | 历史 K 线（UTC、exclusive 游标） |
| POST | `/api/v1/market-data/trades` | 成交分页（`raw` / `aggregated`） |
| POST | `/api/v1/market-data/trades/range` | 聚合成交时间范围（左闭右开，完整覆盖） |
| GET | `/api/v1/market-data/sources/binance/depth` | 盘口 REST 基线 |
| GET | `/api/v1/market-data/sources/binance/stream` | 实时 K 线 SSE |
| GET | `/api/v1/market-data/sources/binance/trades/stream` | 实时成交 SSE |
| GET | `/api/v1/market-data/sources/binance/depth/stream` | 实时盘口 SSE |
| GET | `/api/v1/market-data/sources/binance/trades/archive` | 历史成交 NDJSON |

成功返回 `{"data":...,"requestId":...}`，失败返回 `{"error":{"code","message"},"requestId"}`。

## 关键语义

- **成交方向**：`isBuyerMaker=true` → 主动卖（bid volume）；`false` → 主动买（ask volume）。
  缺少该标记时直接报错，不猜测方向。
- **精度**：价格、数量以**十进制字符串**返回（`price`/`size`/`quoteSize`），避免前端浮点丢精度。
- **raw vs aggregated**：两者 ID 空间不同（`tradeId` vs `aggTradeId`），互不混用；
  Footprint 统一使用 `aggregated`（同日同价的成交已合并，按价位买/卖量与原始逐笔等价）。
- **成交量单位**：`baseAsset`（基础资产数量，非成交额）。
- **盘口**：绝对数量增量，`0` 表示删除；序号断档即重新获取基线，绝不累加或跳过。
- **归档时间**：2025-01-01 起为微秒，之前为毫秒；适配层统一到毫秒，并保留 `timestampMicros`。
- **SSE 回放**：`id` 为 `<epoch>:<seq>`；窗口内可回放，窗口外返回 `REPLAY_GAP` 并附当前快照。
- **慢消费者**：缓冲区满即断开（需回放），不静默丢帧。

## 目录结构

```
cmd/binance-connector/   — 启动入口
internal/config/         — 配置读取与校验（含代理）
internal/model/          — V1 契约（Bar/Trade/Level/Frame/Instrument）
internal/binance/        — REST、WebSocket、盘口、归档、限流
internal/stream/         — 共享订阅 Hub（回放、背压、空闲回收）
internal/server/         — HTTP/SSE/NDJSON 路由与 envelope
```

## 未完成（后续工作）

- **合约市场**：USDT-M / COIN-M 的成交、盘口与资金费率均未实现，当前只有现货。
- **按 ID 深历史**：`historicalTrades` 依赖 `api.binance.com`，本网络需代理；无代理时
  深度回补以归档为主。
- **前端接入**：`packages/core` 已提供统一 `trades` Provider 能力与 Footprint 指标 Layer。
- **多交易所**：Bybit / OKX 未实现。
- **鉴权与部署**：仅允许本机浏览器来源；远程部署需自行配置反向代理与 CORS。

实现范围见 KlineChartQuant Issue #296。

## Footprint 接入

在同级 kmap 仓库运行 `pnpm dev -c binance`，选择 Binance Spot 的 BTCUSDT，切到 1min，
从指标面板添加 Footprint。价格行过密时增大“每行价格跳数”，放大柱宽后显示 Bid × Ask 数字。

`/trades/range` 请求携带 `sourceId`、标准 `instrument` 引用、`from` 与 `to`（UTC 毫秒）。
已结束日期读取校验的 aggTrades 归档；当日以 aggTrades.startTime 定位聚合成交游标，再连续分页。
聚合成交把同价同方向的成交合并，按价位买/卖量与原始逐笔等价。范围超过七天、250000 笔或 250 页时明确报错。
