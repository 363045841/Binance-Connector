// 本文件验证 V1 HTTP 契约和跨真实 HTTP/WebSocket/SSE 的成交回补行为。
package server

import (
	"bufio"
	"context"
	"encoding/json"
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

// TestTradeBudgetError 超限是明确的资源预算错误，不伪装为 Binance 服务不可用。
func TestTradeBudgetError(t *testing.T) {
	w := httptest.NewRecorder()
	upstream(w, &binance.Error{Status: 422, Code: "RESOURCE_EXHAUSTED", Message: "成交范围超过预算"})
	if w.Code != 422 {
		t.Fatalf("预算错误状态: %d", w.Code)
	}
	var result struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Error.Code != "RESOURCE_EXHAUSTED" {
		t.Fatalf("预算错误被隐藏: %s", w.Body.String())
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

// TestValidationAndCapabilities 验证跨市场拒绝、V1 envelope 和健康检查。
func TestValidationAndCapabilities(t *testing.T) {
	s := apiSettings(t)
	s.RESTURL = "http://127.0.0.1:1"
	api := New(binance.New(s), s)
	defer api.Close()
	for _, tc := range []struct {
		method, path, body string
		status             int
	}{{"GET", "/health", "", 200}, {"POST", prefix + "/trades", `{"sourceId":"binance","instrument":{"id":"binance:spot:BTCUSDT","symbol":"BTCUSDT","providerRef":{"market":"usdm"}},"limit":10}`, 400}, {"POST", prefix + "/bars", `{"sourceId":"binance","instrument":{"id":"binance:spot:BTCUSDT","symbol":"BTCUSDT"},"period":"quarterly","limit":10}`, 400}, {"GET", prefix + "/sources/binance/trades/stream?symbol=BTCUSDT%2Fbad", "", 400}, {"GET", prefix + "/sources/binance/trades/archive?symbol=BTCUSDT&date=bad", "", 400}} {
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
	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/health", nil)
	req.Header.Set("Origin", "https://unrelated.example")
	api.ServeHTTP(w, req)
	if w.Code != 403 {
		t.Fatal("外部浏览器来源不应默认放行")
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
	s := apiSettings(t)
	s.WSURL = "ws" + strings.TrimPrefix(up.URL, "http")
	s.HistoricalURL = up.URL
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
