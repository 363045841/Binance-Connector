# Binance-Connector — Agent Guide

Binance 现货行情连接器：K 线、原始逐笔成交、聚合成交、实时盘口与历史成交归档。
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

## 架构

| 包 | 职责 |
|---|---|
| `internal/config` | 配置与校验（含 `BINANCE_PROXY`，默认 `127.0.0.1:6666`） |
| `internal/model` | V1 契约：`Bar` / `Trade` / `Level` / `Frame` / `Instrument` |
| `internal/binance` | REST、WebSocket、盘口同步、归档、出口限流 |
| `internal/stream` | 共享订阅 Hub：回放、背压、空闲回收 |
| `internal/server` | HTTP / SSE / NDJSON 路由与 envelope |

## 错误码

`INVALID_REQUEST`、`UNSUPPORTED_CAPABILITY`、`RATE_LIMITED`、`ACCESS_DENIED`、
`ARCHIVE_UNAVAILABLE`、`REPLAY_GAP`、`TRADE_GAP`、`UPSTREAM_UNAVAILABLE`、
`TIMEOUT`、`RESOURCE_EXHAUSTED`、`NOT_FOUND`。
来自 Binance 的 429/418 会带 `Retry-After`，共享冷却期间不继续访问上游。

## 测试

- 默认测试全部离线（`httptest` 伪造 REST/WS）；不得依赖真实 Binance。
- 外网验证统一由 `BINANCE_LIVE_TEST=1` 显式开启。
- 表驱动覆盖：盘口断序/容量/原子性、成交方向缺失、归档毫秒/微秒、限频冷却、SSE 回放与背压。

## 注释风格

- 每个文件头部注释说明用途；每个函数注释说明职责、参数、返回值。
- 注释正文中文，技术术语保留英文。
- 每个测试用例有中文注释说明验证的行为。

## 已知限制

- 仅 `spot`；合约（USDT-M / COIN-M）未实现。
- `historicalTrades` 只在 `api.binance.com`，部分网络需代理。
- 前端 `packages/core` 的 Footprint 通过统一 Provider、指标实例计算和 Layer 接入，不建立专用图表链路。
