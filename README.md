# Binance-Connector

Binance 行情 Connector，为 KlineChartQuant 提供现货与 USDT-M 永续的 K 线、原始逐笔成交、
聚合成交、实时盘口以及历史成交归档，永续额外提供资金费率与未平仓量。目标是让 Footprint
（足迹图）和订单流类功能拿到**真实成交**和**主动买卖方向**，而不是把报价或盘口变化当成成交量。

- 语言：Go 1.24（单 module）
- 默认监听：`127.0.0.1:8091`
- 市场：`spot`（现货）与 `perp`（USDⓈ-M 永续；COIN-M 与季度合约未实现）
- 协议：`/api/v1/market-data`（V1 风格 envelope），另有 `/health`

## 品种标识

```
binance:<market>:<SYMBOL>      例如 binance:spot:BTCUSDT / binance:perp:BTCUSDT
```

`market` 只允许 `spot` 与 `perp`。市场**只能由 ID 前缀决定**，`providerRef.market`
只用于一致性校验，两者不一致直接报错。TradingView 的 `BTCUSDT.P` 对应
`binance:perp:BTCUSDT`——`.P` 只是前端展示习惯，不是本 Connector 的标识格式。

同一 `SYMBOL` 在两个市场是两条独立订阅：`stream.Key` 带 `market` 维度，
权重预算也按市场隔离（现货冷却不影响永续）。

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
| `BINANCE_REST_URL` | `https://data-api.binance.vision` | 现货公共行情 REST（免 Key） |
| `BINANCE_HISTORY_URL` | `https://api.binance.com` | 现货 `historicalTrades` 专用域名 |
| `BINANCE_WS_URL` | `wss://data-stream.binance.vision` | 现货公共行情 WebSocket |
| `BINANCE_ARCHIVE_URL` | `https://data.binance.vision` | 现货历史成交归档 |
| `BINANCE_PERP_REST_URL` | `https://fapi.binance.com` | 永续 REST（公开端点免 Key） |
| `BINANCE_PERP_HISTORY_URL` | `https://fapi.binance.com` | 永续历史成交宿主（同域） |
| `BINANCE_PERP_WS_URL` | `wss://fstream.binance.com` | 永续 WebSocket |
| `BINANCE_PERP_ARCHIVE_URL` | `https://data.binance.vision` | 永续历史成交归档 |
| `BINANCE_CACHE_DIR` | `data/archives` | 归档缓存目录 |
| `BINANCE_API_KEY` | 空 | 仅现货 `historicalTrades` 可能用到 |
| `BINANCE_PROXY` | `HTTP(S)_PROXY` → `http://127.0.0.1:6666` | HTTP 与 WS 共用；设为 `direct` 表示直连 |
| `BINANCE_HTTP_TIMEOUT` | `20s` | REST 超时 |
| `BINANCE_ARCHIVE_TIMEOUT` | `5m` | 归档下载超时 |
| `BINANCE_WEIGHT_PER_MINUTE` | `1200` | 现货出口请求权重预算 |
| `BINANCE_PERP_WEIGHT_PER_MINUTE` | `2400` | 永续出口请求权重预算（交易所按市场分 IP 计权重） |
| `BINANCE_CATCHUP_PAGES` | `20` | 断线回补最大分页 |
| `BINANCE_MAX_BOOK_LEVELS` | `50000` | 已知盘口档位上限 |

**代理说明**：`data-api.binance.vision` 通常可直连；`api.binance.com`（`historicalTrades`）
在部分网络需要代理。默认走 `127.0.0.1:6666`，如需直连设 `BINANCE_PROXY=direct`。

## 接口

| 方法 | 路径 | 用途 |
|---|---|---|
| GET | `/health` | 健康检查（返回 `sourceId` 与 `markets`） |
| GET | `/api/v1/market-data/sources/binance/probe` | 上游探测与能力清单（`?market=`） |
| POST | `/api/v1/market-data/instruments/search` | 品种搜索（`market` 缺省合并全部市场） |
| POST | `/api/v1/market-data/bars` | 历史 K 线（UTC、exclusive 游标） |
| POST | `/api/v1/market-data/trades` | 成交分页（`raw` / `aggregated`） |
| POST | `/api/v1/market-data/trades/range` | 聚合成交时间范围（左闭右开，完整覆盖） |
| GET | `/api/v1/market-data/sources/binance/depth` | 盘口 REST 基线（`?market=&symbol=`） |
| GET | `/api/v1/market-data/sources/binance/stream` | 实时 K 线 SSE（`?instruments=`） |
| GET | `/api/v1/market-data/sources/binance/quotes` | 报价 SSE（`?instruments=`） |
| GET | `/api/v1/market-data/sources/binance/trades/stream` | 实时成交 SSE（`?market=&symbol=&kind=`） |
| GET | `/api/v1/market-data/sources/binance/depth/stream` | 实时盘口 SSE（`?market=&symbol=`） |
| GET | `/api/v1/market-data/sources/binance/trades/archive` | 历史成交 NDJSON（`?market=&symbol=&date=`） |
| GET | `/api/v1/market-data/sources/binance/funding` | 永续资金费率（仅 `market=perp`） |
| GET | `/api/v1/market-data/sources/binance/open-interest` | 永续未平仓量（仅 `market=perp`） |

`market` 缺省为「全部市场」：搜索结果合并 `spot` 与 `perp`，同一代码返回两条独立身份
（`binance:spot:BTCUSDT` 与 `binance:perp:BTCUSDT`），这样不加后缀也能搜到永续；
显式声明 `market` 时只搜该市场。`/funding` 与 `/open-interest` 在现货上返回 `UNSUPPORTED_CAPABILITY`，
不返回空数组。

成功返回 `{"data":...,"requestId":...}`，失败返回 `{"error":{"code","message"},"requestId"}`。

## 关键语义

- **成交方向**：`isBuyerMaker=true` → 主动卖（bid volume）；`false` → 主动买（ask volume）。
  缺少该标记时直接报错，不猜测方向。
- **精度**：价格、数量以**十进制字符串**返回（`price`/`size`/`quoteSize`），避免前端浮点丢精度。
- **raw vs aggregated**：两者 ID 空间不同（`tradeId` vs `aggTradeId`），互不混用；
  Footprint 统一使用 `aggregated`（同日同价的成交已合并，按价位买/卖量与原始逐笔等价）。
- **成交量单位**：`baseAsset`（基础资产数量，非成交额）。
- **盘口**：绝对数量增量，`0` 表示删除；序号断档即重新获取基线，绝不累加或跳过。
- **归档时间**：现货 2025-01-01 起为微秒、之前毫秒，八列无表头；永续七列带表头、
  始终毫秒。适配层统一到毫秒，并保留 `timestampMicros`；列数与单位由市场布局声明，不按日期猜。
- **WS 路由**：2026-04-23 起永续按流类型拆分前缀——盘口与 `@trade` 走 `/public/ws/`，
  聚合成交与 K 线走 `/market/ws/`；旧 `/ws/` 已下线，混放会握手成功但收不到帧。
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

- **COIN-M 与季度合约**：只实现 USDⓈ-M `PERPETUAL`。`TRADIFI_PERPETUAL`（黄金、股票等
  传统资产永续）暂不纳入，因其交易时段与加密不同，需先有会话模型支持。
- **永续原始成交深历史**：`/fapi/v1/trades` 忽略 `fromId`，`/fapi/v1/historicalTrades`
  需签名，故 `raw` 不支持按 ID 回补；如需深历史请用 `aggregated` 或归档。
- **按 ID 深历史（现货）**：`historicalTrades` 依赖 `api.binance.com`，本网络需代理；无代理时
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
