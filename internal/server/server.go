// Package server 提供 V1 行情 REST、SSE 与历史成交 NDJSON，所有流共用 Hub。
package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"binanceconnector/internal/binance"
	"binanceconnector/internal/config"
	"binanceconnector/internal/model"
	"binanceconnector/internal/stream"
)

const prefix = "/api/v1/market-data"

// Server 持有依赖与路由；关闭流资源与 HTTP 监听分别由 main 编排。
type Server struct {
	Client   *binance.Client
	Hub      *stream.Hub
	Settings config.Settings
	handler  http.Handler
}

// New 装配标准库路由；成交与深度扩展不复用旧版 /api/binance 路径。
func New(c *binance.Client, s config.Settings) *Server {
	server := &Server{Client: c, Hub: stream.NewHub(s, c.Run), Settings: s}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		success(w, map[string]any{"status": "ok", "sourceId": model.SourceID, "market": model.Market})
	})
	mux.HandleFunc("GET "+prefix+"/sources/binance/probe", server.probe)
	mux.HandleFunc("POST "+prefix+"/instruments/search", server.search)
	mux.HandleFunc("POST "+prefix+"/bars", server.bars)
	mux.HandleFunc("POST "+prefix+"/trades", server.trades)
	mux.HandleFunc("POST "+prefix+"/trades/range", server.tradeRange)
	mux.HandleFunc("GET "+prefix+"/sources/binance/depth", server.depth)
	mux.HandleFunc("GET "+prefix+"/sources/binance/stream", server.liveBars)
	mux.HandleFunc("GET "+prefix+"/sources/binance/trades/stream", server.liveTrades)
	mux.HandleFunc("GET "+prefix+"/sources/binance/depth/stream", server.liveDepth)
	mux.HandleFunc("GET "+prefix+"/sources/binance/trades/archive", server.archive)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		failure(w, "NOT_FOUND", "接口不存在或请求方法不匹配", 404, 0)
	})
	server.handler = server.cors(mux)
	return server
}

// ServeHTTP 实现可供 httptest 和生产共享的 Handler。
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.handler.ServeHTTP(w, r) }

// Close 释放共享订阅及上游连接。
func (s *Server) Close() { s.Hub.Close(); s.Client.Close() }

// cors 允许本机开发来源；外部部署通过反向代理控制跨域来源。
func (s *Server) cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" {
			u, err := url.Parse(origin)
			if err != nil || (u.Scheme != "http" && u.Scheme != "https") || (u.Hostname() != "localhost" && u.Hostname() != "127.0.0.1" && u.Hostname() != "::1") {
				failure(w, "ACCESS_DENIED", "仅允许本机浏览器来源，请由部署代理配置远程 CORS", 403, 0)
				return
			}
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Add("Vary", "Origin")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Last-Event-ID")
			w.Header().Set("Access-Control-Expose-Headers", "Retry-After")
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// requestID 为 envelope 生成追踪 ID，不接受客户端任意反射值。
func requestID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	return hex.EncodeToString(b)
}

// success 发送 V1 成功 envelope。
func success(w http.ResponseWriter, data any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(map[string]any{"data": data, "requestId": requestID()})
}

// failure 发送确定的错误码与可选 Retry-After。
func failure(w http.ResponseWriter, code, message string, status int, delay time.Duration) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if delay > 0 {
		w.Header().Set("Retry-After", strconv.FormatInt(int64(delay.Seconds()+0.999), 10))
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"code": code, "message": message}, "requestId": requestID()})
}

// upstream 保留限频及地区错误语义；不自动把认证错误改成行情为空。
func upstream(w http.ResponseWriter, err error) {
	var remote *binance.Error
	if errors.As(err, &remote) {
		status := 502
		if remote.Code == "RATE_LIMITED" {
			status = 429
		}
		if remote.Code == "ACCESS_DENIED" {
			status = 403
		}
		if remote.Code == "INVALID_REQUEST" {
			status = 400
		}
		if remote.Code == "RESOURCE_EXHAUSTED" {
			status = 422
		}
		if remote.Status == 404 {
			status = 404
		}
		failure(w, remote.Code, remote.Message, status, remote.RetryAfter)
		return
	}
	if errors.Is(err, context.DeadlineExceeded) {
		failure(w, "TIMEOUT", err.Error(), 504, 0)
		return
	}
	failure(w, "UPSTREAM_UNAVAILABLE", err.Error(), 502, 0)
}

// decode 拒绝超大、未知字段或拼接多个 JSON 的请求。
func decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		failure(w, "INVALID_REQUEST", err.Error(), 400, 0)
		return false
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		failure(w, "INVALID_REQUEST", "请求只能包含一个 JSON", 400, 0)
		return false
	}
	return true
}

// instrumentInput 只接受 V1 的路由引用，不在 API 中保存用户图表状态。
type instrumentInput struct {
	ID          string         `json:"id"`
	Symbol      string         `json:"symbol"`
	Exchange    string         `json:"exchange,omitempty"`
	ProviderRef map[string]any `json:"providerRef,omitempty"`
}

// symbolFor 校验来源、市场和稳定 ID，防止跨市场混用成交与背景 K 线。
func symbolFor(source string, instrument instrumentInput) (string, error) {
	if source != model.SourceID {
		return "", fmt.Errorf("sourceId 必须为 binance")
	}
	symbol, err := model.Symbol(instrument.Symbol)
	if err != nil {
		return "", err
	}
	if instrument.ID != "binance:spot:"+symbol {
		return "", fmt.Errorf("instrument.id 必须为 binance:spot:%s", symbol)
	}
	if market, ok := instrument.ProviderRef["market"]; ok && market != model.Market {
		return "", fmt.Errorf("当前只支持 spot 市场")
	}
	if value, ok := instrument.ProviderRef["symbol"]; ok && value != symbol {
		return "", fmt.Errorf("providerRef.symbol 与 symbol 不一致")
	}
	return symbol, nil
}

// probe 报告真实 REST 探测结果，不把本机启动等同于上游可用。
func (s *Server) probe(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	status, message := "online", "Binance spot REST 可达；流连接状态由 SSE status 报告"
	if err := s.Client.Ping(r.Context()); err != nil {
		status = "offline"
		message = err.Error()
	}
	success(w, map[string]any{"status": status, "checkedAt": time.Now().UnixMilli(), "latencyMs": time.Since(started).Milliseconds(), "message": message, "market": model.Market, "capabilities": model.Capabilities()})
}

// search 搜索现货目录，明确只返回 crypto 类型。
func (s *Server) search(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SourceID     string   `json:"sourceId"`
		Keyword      string   `json:"keyword"`
		Limit        int      `json:"limit"`
		AssetClasses []string `json:"assetClasses,omitempty"`
	}
	if !decode(w, r, &body) {
		return
	}
	if body.SourceID != model.SourceID || body.Limit < 1 || body.Limit > 100 || len(body.Keyword) > 128 {
		failure(w, "INVALID_REQUEST", "sourceId/limit/keyword 无效", 400, 0)
		return
	}
	for _, asset := range body.AssetClasses {
		if asset != "crypto" {
			success(w, map[string]any{"items": []model.Instrument{}})
			return
		}
	}
	items, err := s.Client.Instruments(r.Context())
	if err != nil {
		upstream(w, err)
		return
	}
	keyword := strings.ToUpper(strings.TrimSpace(body.Keyword))
	out := make([]model.Instrument, 0, body.Limit)
	sort.Slice(items, func(i, j int) bool { return items[i].Symbol < items[j].Symbol })
	for _, item := range items {
		if strings.Contains(item.Symbol, keyword) || strings.Contains(strings.ToUpper(item.Name), keyword) {
			out = append(out, item)
			if len(out) == body.Limit {
				break
			}
		}
	}
	success(w, map[string]any{"items": out})
}

// bars 获取 exclusive 游标之前的 UTC 原生历史 K 线。
func (s *Server) bars(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SourceID       string          `json:"sourceId"`
		Instrument     instrumentInput `json:"instrument"`
		Period         string          `json:"period"`
		Adjustment     string          `json:"adjustment"`
		BarAggregation string          `json:"barAggregation"`
		Limit          int             `json:"limit"`
		Before         *int64          `json:"beforeTimestamp,omitempty"`
	}
	if !decode(w, r, &body) {
		return
	}
	symbol, err := symbolFor(body.SourceID, body.Instrument)
	if err != nil {
		failure(w, "INVALID_REQUEST", err.Error(), 400, 0)
		return
	}
	if _, ok := model.Periods[body.Period]; !ok {
		failure(w, "UNSUPPORTED_CAPABILITY", "不支持该周期", 400, 0)
		return
	}
	if body.Limit < 1 || body.Limit > 1000 || (body.Before != nil && *body.Before <= 0) {
		failure(w, "INVALID_REQUEST", "limit 必须为 1–1000，beforeTimestamp 必须为正毫秒", 400, 0)
		return
	}
	if (body.Adjustment != "" && body.Adjustment != "none") || (body.BarAggregation != "" && body.BarAggregation != "original") {
		failure(w, "UNSUPPORTED_CAPABILITY", "现货仅支持 none / original", 400, 0)
		return
	}
	items, err := s.Client.Bars(r.Context(), symbol, body.Period, body.Limit, body.Before)
	if err != nil {
		upstream(w, err)
		return
	}
	older := "unknown"
	if len(items) == 0 {
		older = "exhausted"
	}
	success(w, map[string]any{"instrumentId": body.Instrument.ID, "period": body.Period, "adjustment": "none", "barAggregation": "original", "timezone": "UTC", "volumeUnit": "baseAsset", "items": items, "olderData": older})
}

// trades 获取有界成交批次，raw 的游标是 tradeId，aggregated 的游标是 aggTradeId。
func (s *Server) trades(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SourceID   string          `json:"sourceId"`
		Instrument instrumentInput `json:"instrument"`
		Kind       string          `json:"kind"`
		Limit      int             `json:"limit"`
		FromID     *string         `json:"fromId,omitempty"`
		Start      *int64          `json:"startTime,omitempty"`
		End        *int64          `json:"endTime,omitempty"`
	}
	if !decode(w, r, &body) {
		return
	}
	symbol, err := symbolFor(body.SourceID, body.Instrument)
	if err != nil {
		failure(w, "INVALID_REQUEST", err.Error(), 400, 0)
		return
	}
	if body.Kind == "" {
		body.Kind = "raw"
	}
	if (body.Kind != "raw" && body.Kind != "aggregated") || body.Limit < 1 || body.Limit > 1000 {
		failure(w, "INVALID_REQUEST", "kind 为 raw/aggregated，limit 为 1–1000", 400, 0)
		return
	}
	var from *int64
	if body.FromID != nil {
		value, err := strconv.ParseInt(*body.FromID, 10, 64)
		if err != nil || value < 0 {
			failure(w, "INVALID_REQUEST", "fromId 必须为非负整数字符串", 400, 0)
			return
		}
		from = &value
	}
	if (body.Start != nil && *body.Start <= 0) || (body.End != nil && *body.End <= 0) || (body.Start != nil && body.End != nil && *body.End < *body.Start) {
		failure(w, "INVALID_REQUEST", "时间范围无效", 400, 0)
		return
	}
	if body.Kind == "raw" && (body.Start != nil || body.End != nil) {
		failure(w, "UNSUPPORTED_CAPABILITY", "raw 使用 fromId 分页；按日期读取请使用 archive", 400, 0)
		return
	}
	if from != nil && (body.Start != nil || body.End != nil) {
		failure(w, "INVALID_REQUEST", "fromId 与时间范围不能混用", 400, 0)
		return
	}
	var items []model.Trade
	if body.Kind == "raw" {
		items, err = s.Client.Trades(r.Context(), symbol, from, body.Limit)
	} else {
		q := url.Values{"limit": {strconv.Itoa(body.Limit)}}
		if from != nil {
			q.Set("fromId", strconv.FormatInt(*from, 10))
		}
		if body.Start != nil {
			q.Set("startTime", strconv.FormatInt(*body.Start, 10))
		}
		if body.End != nil {
			q.Set("endTime", strconv.FormatInt(*body.End, 10))
		}
		items, err = s.Client.AggTrades(r.Context(), symbol, q)
	}
	if err != nil {
		upstream(w, err)
		return
	}
	next := ""
	if len(items) > 0 {
		id, _ := strconv.ParseInt(items[len(items)-1].TradeID, 10, 64)
		next = strconv.FormatInt(id+1, 10)
	}
	success(w, map[string]any{"sourceId": model.SourceID, "instrumentId": body.Instrument.ID, "market": model.Market, "kind": body.Kind, "volumeUnit": "baseAsset", "items": items, "nextFromId": next, "coverage": map[string]any{"status": "page", "completeRange": false}})
}

// querySymbol 统一校验流和快照的交易对参数。
// tradeRange 返回具有确定覆盖边界的原始成交，失败时不发布半份统计数据。
func (s *Server) tradeRange(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SourceID   string          `json:"sourceId"`
		Instrument instrumentInput `json:"instrument"`
		From       int64           `json:"from"`
		To         int64           `json:"to"`
	}
	if !decode(w, r, &body) {
		return
	}
	symbol, err := symbolFor(body.SourceID, body.Instrument)
	if err != nil {
		failure(w, "INVALID_REQUEST", err.Error(), 400, 0)
		return
	}
	if body.From <= 0 || body.To <= body.From || body.To > time.Now().UnixMilli() || body.To-body.From > 7*24*time.Hour.Milliseconds() {
		failure(w, "INVALID_REQUEST", "成交时间范围必须为已发生的左闭右开区间，最多七天", 400, 0)
		return
	}
	items, err := s.Client.TradeRange(r.Context(), symbol, body.From, body.To)
	if err != nil {
		upstream(w, err)
		return
	}
	success(w, map[string]any{"items": items, "range": map[string]int64{"from": body.From, "to": body.To}, "complete": true})
}

func querySymbol(w http.ResponseWriter, r *http.Request) (string, bool) {
	symbol, err := model.Symbol(r.URL.Query().Get("symbol"))
	if err != nil {
		failure(w, "INVALID_REQUEST", err.Error(), 400, 0)
		return "", false
	}
	return symbol, true
}

// depth 返回已知档位的直接 REST 基线，而不是常驻缓存的旧快照。
func (s *Server) depth(w http.ResponseWriter, r *http.Request) {
	symbol, ok := querySymbol(w, r)
	if !ok {
		return
	}
	limit := s.Settings.SnapshotLimit
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 5000 {
			failure(w, "INVALID_REQUEST", "limit 为 1–5000", 400, 0)
			return
		}
		limit = n
	}
	snap, err := s.Client.Depth(r.Context(), symbol, limit)
	if err != nil {
		upstream(w, err)
		return
	}
	book, err := binance.NewBook(symbol, snap, s.Settings.MaxBookLevels)
	if err != nil {
		upstream(w, err)
		return
	}
	success(w, book.Snapshot())
}

// liveBars 复用当前图表 V1 实时 K 线 URL 与帧类型。
func (s *Server) liveBars(w http.ResponseWriter, r *http.Request) {
	symbol, ok := querySymbol(w, r)
	if !ok {
		return
	}
	period := r.URL.Query().Get("period")
	if _, ok := model.Periods[period]; !ok {
		failure(w, "UNSUPPORTED_CAPABILITY", "不支持该周期", 400, 0)
		return
	}
	if a := r.URL.Query().Get("barAggregation"); a != "" && a != "original" {
		failure(w, "UNSUPPORTED_CAPABILITY", "仅支持 original", 400, 0)
		return
	}
	s.sse(w, r, stream.Key{Kind: "bars", Symbol: symbol, Period: period})
}

// liveTrades 为 raw 和 aggregated 创建隔离的订阅及回放空间。
func (s *Server) liveTrades(w http.ResponseWriter, r *http.Request) {
	symbol, ok := querySymbol(w, r)
	if !ok {
		return
	}
	kind := r.URL.Query().Get("kind")
	keyKind := "trades"
	if kind == "aggregated" {
		keyKind = "aggTrades"
	} else if kind != "" && kind != "raw" {
		failure(w, "INVALID_REQUEST", "kind 为 raw/aggregated", 400, 0)
		return
	}
	s.sse(w, r, stream.Key{Kind: keyKind, Symbol: symbol})
}

// liveDepth 订阅经过本地连续性校验的盘口快照与绝对量增量。
func (s *Server) liveDepth(w http.ResponseWriter, r *http.Request) {
	symbol, ok := querySymbol(w, r)
	if !ok {
		return
	}
	s.sse(w, r, stream.Key{Kind: "depth", Symbol: symbol})
}

// sse 支持 Last-Event-ID；慢消费者被断开后从最后已接收帧恢复。
func (s *Server) sse(w http.ResponseWriter, r *http.Request, key stream.Key) {
	if _, ok := w.(http.Flusher); !ok {
		failure(w, "UNSUPPORTED_CAPABILITY", "HTTP Transport 不支持流式响应", 500, 0)
		return
	}
	sub, err := s.Hub.Subscribe(key, r.Header.Get("Last-Event-ID"))
	if err != nil {
		failure(w, "RESOURCE_EXHAUSTED", err.Error(), 503, 0)
		return
	}
	defer sub.Close()
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	control := http.NewResponseController(w)
	write := func(entry stream.Entry) error {
		_ = control.SetWriteDeadline(time.Now().Add(s.Settings.WriteTimeout))
		if entry.ID != "" {
			if _, err := fmt.Fprintf(w, "id: %s\n", entry.ID); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprintf(w, "data: %s\n\n", entry.Data); err != nil {
			return err
		}
		return control.Flush()
	}
	_ = control.SetWriteDeadline(time.Now().Add(s.Settings.WriteTimeout))
	if _, err := io.WriteString(w, "retry: 2000\n\n"); err != nil {
		return
	}
	if err := control.Flush(); err != nil {
		return
	}
	for _, entry := range sub.Replay() {
		if err := write(entry); err != nil {
			return
		}
	}
	timer := time.NewTicker(s.Settings.KeepAlive)
	defer timer.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case entry, ok := <-sub.Events:
			if !ok {
				return
			}
			if err := write(entry); err != nil {
				return
			}
		case <-timer.C:
			_ = control.SetWriteDeadline(time.Now().Add(s.Settings.WriteTimeout))
			if _, err := io.WriteString(w, ": keepalive\n\n"); err != nil {
				return
			}
			if err := control.Flush(); err != nil {
				return
			}
		}
	}
}

// archive 流式返回日聚合成交，只有终止 end.complete=true 表示完整读取成功。
func (s *Server) archive(w http.ResponseWriter, r *http.Request) {
	symbol, ok := querySymbol(w, r)
	if !ok {
		return
	}
	date := r.URL.Query().Get("date")
	day, err := time.Parse("2006-01-02", date)
	if err != nil || !day.Before(time.Now().UTC().Truncate(24*time.Hour)) {
		failure(w, "INVALID_REQUEST", "date 必须是 UTC 已结束日期 YYYY-MM-DD", 400, 0)
		return
	}
	filename, err := s.Client.Archive(r.Context(), symbol, date)
	if err != nil {
		upstream(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/x-ndjson; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	control := http.NewResponseController(w)
	encoder := json.NewEncoder(w)
	count := 0
	write := func(value any) error {
		_ = control.SetWriteDeadline(time.Now().Add(s.Settings.WriteTimeout))
		return encoder.Encode(value)
	}
	if err := write(map[string]any{"type": "begin", "sourceId": model.SourceID, "market": model.Market, "symbol": symbol, "date": date, "kind": "aggregated", "volumeUnit": "baseAsset"}); err != nil {
		return
	}
	err = binance.ReadArchive(r.Context(), filename, date, func(t model.Trade) error {
		if err := write(map[string]any{"type": "trade", "trade": t}); err != nil {
			return err
		}
		count++
		if count%1000 == 0 {
			return control.Flush()
		}
		return nil
	})
	if err != nil {
		_ = write(map[string]any{"type": "error", "complete": false, "code": "ARCHIVE_READ_FAILED", "message": err.Error()})
		_ = control.Flush()
		return
	}
	_ = write(map[string]any{"type": "end", "complete": true, "count": count})
	_ = control.Flush()
}
