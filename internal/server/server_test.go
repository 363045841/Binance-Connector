// 本文件验证 V1 HTTP 契约和跨真实 HTTP/WebSocket/SSE 的成交回补行为。
package server

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"binanceconnector/internal/binance"
	"binanceconnector/internal/config"
	"binanceconnector/internal/model"
	"github.com/gorilla/websocket"
)

// TestUpstreamErrorMapping 让各类上游错误映射为确定的 HTTP 语义，不被压成统一的 502。
func TestUpstreamErrorMapping(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		status int
		code   string
	}{
		{"预算超限", &binance.Error{Status: 422, Code: "RESOURCE_EXHAUSTED", Message: "成交范围超过预算"}, 422, "RESOURCE_EXHAUSTED"},
		{"限频", &binance.Error{Status: 429, Code: "RATE_LIMITED", Message: "Too many requests"}, 429, "RATE_LIMITED"},
		{"区域受限", &binance.Error{Status: 403, Code: "ACCESS_DENIED", Message: "restricted"}, 403, "ACCESS_DENIED"},
		{"请求无效", &binance.Error{Status: 400, Code: "INVALID_REQUEST", Message: "bad request"}, 400, "INVALID_REQUEST"},
		{"上游 404", &binance.Error{Status: 404, Code: "NOT_FOUND", Message: "unknown symbol"}, 404, "NOT_FOUND"},
		{"超时", context.DeadlineExceeded, 504, "TIMEOUT"},
		{"其它错误", errors.New("connection refused"), 502, "UPSTREAM_UNAVAILABLE"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			upstream(w, tc.err)
			if w.Code != tc.status {
				t.Fatalf("状态 = %d，期望 %d: %s", w.Code, tc.status, w.Body.String())
			}
			var result struct {
				Error struct {
					Code string `json:"code"`
				} `json:"error"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
			if result.Error.Code != tc.code {
				t.Fatalf("错误码 = %q，期望 %q", result.Error.Code, tc.code)
			}
		})
	}
}

// apiSettings 使用正式配置，仅替换测试监听和归档目录。
func apiSettings(t *testing.T) config.Settings {
	t.Helper()
	s, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	s.CacheDir = t.TempDir()
	return s
}

// editMarket 就地修改单个市场地址组；map 内是值类型，必须回写。
func editMarket(s config.Settings, market string, edit func(*config.MarketSettings)) config.Settings {
	settings := s.Markets[market]
	edit(&settings)
	s.Markets[market] = settings
	return s
}

// TestValidationAndCapabilities 验证跨市场拒绝、V1 envelope 和健康检查。
func TestValidationAndCapabilities(t *testing.T) {
	s := editMarket(apiSettings(t), model.MarketSpot, func(m *config.MarketSettings) {
		m.REST = "http://127.0.0.1:1"
	})
	api := New(binance.New(s), s)
	defer api.Close()
	for _, tc := range []struct {
		method, path, body string
		status             int
	}{{"GET", "/health", "", 200}, {"POST", prefix + "/trades", `{"sourceId":"binance","instrument":{"id":"binance:spot:BTCUSDT","symbol":"BTCUSDT","providerRef":{"market":"usdm"}},"limit":10}`, 400}, {"POST", prefix + "/bars", `{"sourceId":"binance","instrument":{"id":"binance:spot:BTCUSDT","symbol":"BTCUSDT"},"period":"quarterly","limit":10}`, 400}, {"GET", prefix + "/sources/binance/trades/stream?symbol=BTCUSDT%2Fbad", "", 400}, {"GET", prefix + "/sources/binance/trades/archive?symbol=BTCUSDT&date=bad", "", 400}, {"GET", prefix + "/sources/binance/funding?symbol=BTCUSDT", "", 400}, {"GET", prefix + "/sources/binance/open-interest?symbol=BTCUSDT", "", 400}} {
		t.Run(tc.path, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			w := httptest.NewRecorder()
			api.ServeHTTP(w, req)
			if w.Code != tc.status {
				t.Fatalf("错误状态: %d %s", w.Code, w.Body.String())
			}
			var envelope map[string]json.RawMessage
			if err := json.Unmarshal(w.Body.Bytes(), &envelope); err != nil {
				t.Fatal(err)
			}
			if len(envelope["requestId"]) == 0 {
				t.Fatal("缺少追踪 ID")
			}
		})
	}
}

// TestInstrumentMarketResolution 验证市场只由 instrument.id 前缀决定，跨市场装配必须失败。
func TestInstrumentMarketResolution(t *testing.T) {
	for _, tc := range []struct {
		name     string
		input    instrumentInput
		wantMark string
		wantErr  bool
	}{
		{"现货", instrumentInput{ID: "binance:spot:BTCUSDT"}, model.MarketSpot, false},
		{"永续", instrumentInput{ID: "binance:perp:BTCUSDT"}, model.MarketPerp, false},
		{"缺 ID", instrumentInput{Symbol: "BTCUSDT"}, "", true},
		{"未知市场", instrumentInput{ID: "binance:usdm:BTCUSDT"}, "", true},
		{"symbol 与 ID 不一致", instrumentInput{ID: "binance:perp:BTCUSDT", Symbol: "ETHUSDT"}, "", true},
		{"providerRef.market 不一致", instrumentInput{ID: "binance:spot:BTCUSDT", ProviderRef: map[string]any{"market": "perp"}}, "", true},
		{"providerRef.symbol 不一致", instrumentInput{ID: "binance:perp:BTCUSDT", ProviderRef: map[string]any{"symbol": "ETHUSDT"}}, "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			market, _, err := resolveInstrument("binance", tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("应拒绝: %+v", tc.input)
				}
				return
			}
			if err != nil || market != tc.wantMark {
				t.Fatalf("market = %q err=%v", market, err)
			}
		})
	}
}

// TestSearchMergesMarkets 验证搜索省略 market 时合并现货与永续目录：
// 同一代码在两个市场各返回一条独立身份，显式声明 market 时只返回该市场。
func TestSearchMergesMarkets(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v3/exchangeInfo":
			_, _ = w.Write([]byte(`{"symbols":[{"symbol":"BTCUSDT","status":"TRADING","baseAsset":"BTC","quoteAsset":"USDT","filters":[{"filterType":"PRICE_FILTER","tickSize":"0.01"},{"filterType":"LOT_SIZE","stepSize":"0.00001"}]},{"symbol":"ETHBTC","status":"TRADING","baseAsset":"ETH","quoteAsset":"BTC","filters":[{"filterType":"PRICE_FILTER","tickSize":"0.0001"},{"filterType":"LOT_SIZE","stepSize":"0.0001"}]},{"symbol":"AAAUSDT","status":"TRADING","baseAsset":"AAA","quoteAsset":"USDT","filters":[{"filterType":"PRICE_FILTER","tickSize":"0.01"},{"filterType":"LOT_SIZE","stepSize":"1"}]},{"symbol":"BBBUSDT","status":"TRADING","baseAsset":"BBB","quoteAsset":"USDT","filters":[{"filterType":"PRICE_FILTER","tickSize":"0.01"},{"filterType":"LOT_SIZE","stepSize":"1"}]}]}`))
		case "/fapi/v1/exchangeInfo":
			// 季度合约不是永续，必须被目录过滤掉，不能出现在搜索结果里。
			_, _ = w.Write([]byte(`{"symbols":[{"symbol":"BTCUSDT","status":"TRADING","baseAsset":"BTC","quoteAsset":"USDT","contractType":"PERPETUAL","filters":[{"filterType":"PRICE_FILTER","tickSize":"0.10"},{"filterType":"LOT_SIZE","stepSize":"0.001"}]},{"symbol":"AAAUSDT","status":"TRADING","baseAsset":"AAA","quoteAsset":"USDT","contractType":"PERPETUAL","filters":[{"filterType":"PRICE_FILTER","tickSize":"0.0001"},{"filterType":"LOT_SIZE","stepSize":"1"}]},{"symbol":"BBBUSDT","status":"TRADING","baseAsset":"BBB","quoteAsset":"USDT","contractType":"PERPETUAL","filters":[{"filterType":"PRICE_FILTER","tickSize":"0.0001"},{"filterType":"LOT_SIZE","stepSize":"1"}]},{"symbol":"BTCUSDT_241227","status":"TRADING","baseAsset":"BTC","quoteAsset":"USDT","contractType":"CURRENT_QUARTER","filters":[]}]}`))
		default:
			t.Errorf("意外路径: %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer up.Close()
	s := editMarket(apiSettings(t), model.MarketSpot, func(m *config.MarketSettings) { m.REST = up.URL })
	s = editMarket(s, model.MarketPerp, func(m *config.MarketSettings) { m.REST = up.URL })
	api := New(binance.New(s), s)
	defer api.Close()
	search := func(body string) []model.Instrument {
		w := httptest.NewRecorder()
		api.ServeHTTP(w, httptest.NewRequest("POST", prefix+"/instruments/search", strings.NewReader(body)))
		if w.Code != 200 {
			t.Fatalf("搜索失败: %d %s", w.Code, w.Body.String())
		}
		var envelope struct {
			Data struct {
				Items []model.Instrument `json:"items"`
			} `json:"data"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &envelope); err != nil {
			t.Fatal(err)
		}
		return envelope.Data.Items
	}
	ids := func(items []model.Instrument) []string {
		out := make([]string, 0, len(items))
		for _, item := range items {
			out = append(out, item.ID)
		}
		return out
	}
	// 省略 market 时两个市场都搜，同代码现货在前（与「不带后缀即现货」一致）。
	if got := strings.Join(ids(search(`{"sourceId":"binance","keyword":"BTCUSDT","limit":10}`)), ","); got != "binance:spot:BTCUSDT,binance:perp:BTCUSDT" {
		t.Fatalf("跨市场搜索应同时返回现货与永续，实际 %s", got)
	}
	// 显式限定市场时只返回该市场。
	if got := strings.Join(ids(search(`{"sourceId":"binance","keyword":"BTCUSDT","market":"perp","limit":10}`)), ","); got != "binance:perp:BTCUSDT" {
		t.Fatalf("限定永续市场不应返回现货，实际 %s", got)
	}
	// 截断发生在合并之后：limit=2 时两个市场的 BTCUSDT 都在，不会被截掉其中一个。
	if got := strings.Join(ids(search(`{"sourceId":"binance","keyword":"BTCUSDT","limit":2}`)), ","); got != "binance:spot:BTCUSDT,binance:perp:BTCUSDT" {
		t.Fatalf("截断必须在合并之后，实际 %s", got)
	}
	// 合并后按代码排序再截断：limit=3 时 AAAUSDT 的现货与永续都出现，
	// 说明名额没有被现货目录占满（先拼现货再截断会漏掉永续）。
	if got := strings.Join(ids(search(`{"sourceId":"binance","keyword":"","limit":3}`)), ","); got != "binance:spot:AAAUSDT,binance:perp:AAAUSDT,binance:spot:BBBUSDT" {
		t.Fatalf("截断前必须按代码排序，实际 %s", got)
	}
	// 未知市场仍然显式拒绝，不退化成跨市场搜索。
	w := httptest.NewRecorder()
	api.ServeHTTP(w, httptest.NewRequest("POST", prefix+"/instruments/search", strings.NewReader(`{"sourceId":"binance","keyword":"BTCUSDT","market":"usdm","limit":10}`)))
	if w.Code != 400 {
		t.Fatalf("未知市场应拒绝，实际 %d", w.Code)
	}
}

// TestCORSAllowlistAndPrivateNetwork 验证白名单来源放行与 PNA 预检允许头。
func TestCORSAllowlistAndPrivateNetwork(t *testing.T) {
	s := editMarket(apiSettings(t), model.MarketSpot, func(m *config.MarketSettings) { m.REST = "http://127.0.0.1:1" })
	s.AllowedOrigins = []string{"https://demo.example"}
	api := New(binance.New(s), s)
	defer api.Close()

	allowed := httptest.NewRecorder()
	allowedRequest := httptest.NewRequest("GET", "/health", nil)
	allowedRequest.Header.Set("Origin", "https://demo.example")
	api.ServeHTTP(allowed, allowedRequest)
	if allowed.Code != 200 || allowed.Header().Get("Access-Control-Allow-Origin") != "https://demo.example" {
		t.Fatalf("白名单来源未放行: %d %s", allowed.Code, allowed.Body.String())
	}

	preflight := httptest.NewRecorder()
	preflightRequest := httptest.NewRequest("OPTIONS", prefix+"/bars", nil)
	preflightRequest.Header.Set("Origin", "https://demo.example")
	preflightRequest.Header.Set("Access-Control-Request-Method", "POST")
	preflightRequest.Header.Set("Access-Control-Request-Private-Network", "true")
	api.ServeHTTP(preflight, preflightRequest)
	if preflight.Code != 204 || preflight.Header().Get("Access-Control-Allow-Private-Network") != "true" {
		t.Fatalf("PNA 预检未放行: %d", preflight.Code)
	}

	blocked := httptest.NewRecorder()
	blockedRequest := httptest.NewRequest("GET", "/health", nil)
	blockedRequest.Header.Set("Origin", "https://unrelated.example")
	api.ServeHTTP(blocked, blockedRequest)
	if blocked.Code != 403 {
		t.Fatalf("非白名单来源应拒绝: %d", blocked.Code)
	}
}

// TestSSETradeCatchup 验证官方 WS 格式跨 ID 缺口时先 REST 回补再发布后续成交。
func TestSSETradeCatchup(t *testing.T) {
	upgrader := websocket.Upgrader{}
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v3/historicalTrades" {
			if r.URL.Query().Get("fromId") != "12346" {
				t.Error("回补游标错误")
			}
			_, _ = w.Write([]byte(`[{"id":12346,"price":"0.001","qty":"100","time":1672515782136,"isBuyerMaker":false}]`))
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		for _, id := range []int64{12345, 12347} {
			payload := fmt.Sprintf(`{"e":"trade","E":1672515782136,"s":"BNBBTC","t":%d,"p":"0.001","q":"100","T":1672515782136,"m":true,"M":true}`, id)
			if err := conn.WriteMessage(websocket.TextMessage, []byte(payload)); err != nil {
				return
			}
		}
		_, _, _ = conn.ReadMessage()
	}))
	defer up.Close()
	s := editMarket(apiSettings(t), model.MarketSpot, func(m *config.MarketSettings) {
		m.WS = "ws" + strings.TrimPrefix(up.URL, "http")
		m.Historical = up.URL
		m.REST = up.URL
	})
	api := New(binance.New(s), s)
	defer api.Close()
	local := httptest.NewServer(api)
	defer local.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", local.URL+prefix+"/sources/binance/trades/stream?symbol=BNBBTC", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatal(resp.Status)
	}
	ids := make([]string, 0, 3)
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var frame model.Frame
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &frame); err != nil {
			t.Fatal(err)
		}
		if frame.Code == "TRADE_GAP" {
			t.Fatalf("应该能够补齐: %+v", frame)
		}
		for _, trade := range frame.Trades {
			ids = append(ids, trade.TradeID)
		}
		if len(ids) >= 3 {
			break
		}
	}
	if strings.Join(ids, ",") != "12345,12346,12347" {
		t.Fatalf("成交丢失、重复或乱序: %v; %v", ids, scanner.Err())
	}
}
