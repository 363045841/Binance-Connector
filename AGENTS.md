# Binance-Connector — Agent Guide

Binance 行情连接器：现货（`spot`）与 USDT-M 永续（`perp`）的 K 线、原始逐笔成交、
聚合成交、实时盘口与历史成交归档，永续额外提供资金费率与未平仓量。
为 Footprint / 订单流功能提供**真实成交**与**主动买卖方向**。

## Quick start

```bash
go mod download
go run ./cmd/binance-connector          # 默认 127.0.0.1:8091
go test -race ./...                     # 不依赖外网
go vet ./...

# 真实 Binance 验证（需代理 127.0.0.1:6666 可用）
$env:BINANCE_LIVE_TEST='1'; go test ./internal/binance -run '^TestLive' -v
```

## 约定

- 单一 Go module `binanceconnector`，无嵌套 `go.mod`。
- 全部可调参数集中在 `internal/config`，禁止散落 `os.environ`。
- 时间统一为 epoch 毫秒（UTC）；归档微秒时间额外保留 `timestampMicros`。
- 价格/数量以十进制字符串对外，禁止在传输层转 float 造成精度丢失。
- 主动方向只认交易所标记（`isBuyerMaker`），缺失即报错，不臆测。
- 盘口是绝对数量增量，`0` 为删除；序号断档必须重新取基线。
- 成交与盘口是两套订阅（`trades` / `depth`），盘口变化不得计入成交量。
- 回退与兼容逻辑一律不写；不确定的状态用显式错误码暴露。
- **市场是显式维度**：品种 ID 形如 `binance:<market>:<SYMBOL>`，`market` 只允许
  `spot` 与 `perp`。市场只能由 ID 前缀决定，`providerRef.market` 仅用于校验一致性，
  禁止从其它字段推断市场，也禁止跨市场复用同一个订阅 key。

## 架构

| 包 | 职责 |
|---|---|
| `internal/config` | 配置与校验：按市场分组的 REST/历史/WS/归档地址与权重预算（含 `BINANCE_PROXY`，默认 `127.0.0.1:6666`） |
| `internal/model` | V1 契约：`Bar` / `Trade` / `Level` / `Frame` / `Instrument` / `Funding` / `OpenInterest`，以及市场与品种 ID 的唯一定义（`markets.go`） |
| `internal/binance` | REST、WebSocket、盘口同步、归档、出口限流 |
| `internal/stream` | 共享订阅 Hub：回放、背压、空闲回收 |
| `internal/server` | HTTP / SSE / NDJSON 路由与 envelope |

## WebSocket 路由（2026-04-23 起生效）

USDⓈ-M 永续的 WS 已按流类型拆分前缀，旧的 `wss://fstream.binance.com/ws` 与 `/stream`
已永久下线，混放会被静默忽略（握手成功但收不到任何帧）：

- `/public/ws/` — `@depth`、`@depth@100ms`、`@bookTicker`、`!bookTicker`、`@trade`
- `/market/ws/` — `@aggTrade`、`@markPrice@1s`、`@kline_<interval>`、`@ticker`、`@forceOrder`
- 现货仍用旧的 `/ws/`（`data-stream.binance.vision` 不支持新前缀）。

路由选择集中在 `internal/binance/live.go` 的 `streamPath`，新增流类型必须同步该表。

## 错误码

`INVALID_REQUEST`、`UNSUPPORTED_CAPABILITY`、`RATE_LIMITED`、`ACCESS_DENIED`、
`ARCHIVE_UNAVAILABLE`、`REPLAY_GAP`、`TRADE_GAP`、`UPSTREAM_UNAVAILABLE`、
`TIMEOUT`、`RESOURCE_EXHAUSTED`、`NOT_FOUND`。
来自 Binance 的 429/418 会带 `Retry-After`，共享冷却期间不继续访问上游。

## 测试

- 默认测试全部离线（`httptest` 伪造 REST/WS）；不得依赖真实 Binance。
- 外网验证统一由 `BINANCE_LIVE_TEST=1` 显式开启。
- 表驱动覆盖：盘口断序/容量/原子性、成交方向缺失、归档毫秒/微秒、限频冷却、SSE 回放与背压、
  两市场的权重分档、WS 路由分派、永续专属端点拒绝现货调用。
- 测试里修改市场地址一律走 `withMarket` / `editMarket` 这类辅助函数：`Settings.Markets`
  装的是值类型，直接写 `s.Markets[market].REST` 不会生效。

## 注释风格

- 每个文件头部注释说明用途；每个函数注释说明职责、参数、返回值。
- 注释正文中文，技术术语保留英文。
- 每个测试用例有中文注释说明验证的行为。

## 已知限制

- 永续只覆盖 USDⓈ-M `PERPETUAL`；`TRADIFI_PERPETUAL`（黄金、股票等传统资产永续）与
  COIN-M、季度合约均未实现——前者交易时段与加密不同，需先有会话模型支持。
- 永续原始成交（`raw`）不支持按 ID 深历史：`/fapi/v1/trades` 忽略 `fromId`，
  `/fapi/v1/historicalTrades` 需签名。按 ID 回补请改用 `aggregated` 或归档。
- 永续归档为七列带表头、时间戳毫秒；现货为八列无表头、2025-01-01 起微秒。
  两者差异由 `archiveLayouts` 声明，禁止用日期猜列数。
- `historicalTrades` 只在 `api.binance.com`，部分网络需代理。
- 前端 `packages/core` 的 Footprint 通过统一 Provider、指标实例计算和 Layer 接入，不建立专用图表链路。
