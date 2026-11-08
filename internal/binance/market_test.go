// 本文件以 Binance 官方示例验证 REST/WS 适配、限频、历史单位与盘口连续性。
package binance

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"binanceconnector/internal/config"
	"binanceconnector/internal/model"
	"binanceconnector/internal/stream"
)

// settingsForTest 使用正式配置，测试只修改上游地址与缓存位置。
func settingsForTest(t *testing.T) config.Settings {
	t.Helper()
	s, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	// 上游均为本机测试服务器，不经默认代理。
	s.Proxy = config.ProxyOff
	s.CacheDir = t.TempDir()
	return s
}

// TestTradeDirectionAndPrecision 验证官方 trade 示例的主动方向与原始精度。
func TestTradeDirectionAndPrecision(t *testing.T) {
	data := []byte(`{"e":"trade","E":1672515782136,"s":"BNBBTC","t":12345,"p":"0.00100000","q":"100.00000000","T":1672515782136,"m":true,"M":true}`)
	trade, err := ParseLiveTrade(data, "trades", "BNBBTC")
	if err != nil {
		t.Fatal(err)
	}
	if trade.Side != "sell" || trade.TradeID != "12345" || trade.Price != "0.00100000" || trade.Size != "100.00000000" || trade.Kind != "raw" {
		t.Fatalf("错误映射: %+v", trade)
	}
	missing := strings.Replace(string(data), `,"m":true`, "", 1)
	if _, err := ParseLiveTrade([]byte(missing), "trades", "BNBBTC"); err == nil {
		t.Fatal("缺失主动方向不能默认判定为 buy")
	}
	if _, err := ParseLiveTrade(data, "trades", "BTCUSDT"); err == nil {
		t.Fatal("不应接受其他品种的成交")
	}
	agg := []byte(`{"e":"aggTrade","E":1672515782136,"s":"BNBBTC","a":12345,"p":"0.001","q":"100","f":100,"l":105,"T":1672515782136,"m":false,"M":true}`)
	tt, err := ParseLiveTrade(agg, "aggTrades", "BNBBTC")
	if err != nil || tt.Side != "buy" || tt.FirstTradeID != "100" || tt.LastTradeID != "105" || tt.Kind != "aggregated" {
		t.Fatalf("聚合成交映射错误: %+v %v", tt, err)
	}
}

// TestDepthBridgeGapAndAtomicity 验证重叠基线、绝对量更新、删除和断序拒绝。
func TestDepthBridgeGapAndAtomicity(t *testing.T) {
	book, err := NewBook("BNBBTC", DepthSnapshot{LastID: 160, Bids: []model.Level{{"0.0024", "10"}}, Asks: []model.Level{{"0.0026", "100"}}}, 100)
	if err != nil {
		t.Fatal(err)
	}
	duplicate := DepthEvent{Symbol: "BNBBTC", FirstID: 150, FinalID: 160}
	if applied, err := book.Apply(duplicate); err != nil || applied {
		t.Fatal("重复基线不应重应用")
	}
	bridge := DepthEvent{Symbol: "BNBBTC", FirstID: 159, FinalID: 162, Bids: []model.Level{{"0.0024", "12"}}, Asks: []model.Level{{"0.0026", "0"}}}
	if applied, err := book.Apply(bridge); err != nil || !applied {
		t.Fatalf("应接受覆盖下一序号的事件: %v", err)
	}
	snap := book.Snapshot()
	if snap.LastUpdateID != "162" || len(snap.Asks) != 0 || snap.Bids[0][1] != "12" {
		t.Fatalf("数量必须绝对替换，不是累加: %+v", snap)
	}
	if _, err := book.Apply(DepthEvent{Symbol: "BNBBTC", FirstID: 164, FinalID: 165}); err == nil {
		t.Fatal("断序必须失败")
	}
	bad := DepthEvent{Symbol: "BNBBTC", FirstID: 163, FinalID: 163, Bids: []model.Level{{"0.0024", "15"}}, Asks: []model.Level{{"0.0026", "NaN"}}}
	if _, err := book.Apply(bad); err == nil {
		t.Fatal("无效盘口数量必须失败")
	}
	if after := book.Snapshot(); after.LastUpdateID != "162" || after.Bids[0][1] != "12" {
		t.Fatal("无效事件不能留下部分盘口更新")
	}
}

// TestBookCapacity 原子计算删除与新增后的预算，超限事件不写入盘口。
func TestBookCapacity(t *testing.T) {
	b, err := NewBook("BNBBTC", DepthSnapshot{LastID: 160, Bids: []model.Level{{"0.0024", "10"}}, Asks: []model.Level{{"0.0026", "100"}}}, 2)
	if err != nil {
		t.Fatal(err)
	}
	e := DepthEvent{Symbol: "BNBBTC", FirstID: 161, FinalID: 161, Asks: []model.Level{{"0.0026", "0"}, {"0.0027", "1"}, {"0.0027", "2"}}}
	if applied, err := b.Apply(e); err != nil || !applied {
		t.Fatalf("等量替换不能误算超限: %v", err)
	}
	if _, err := b.Apply(DepthEvent{Symbol: "BNBBTC", FirstID: 162, FinalID: 162, Bids: []model.Level{{"0.0023", "1"}}}); err == nil {
		t.Fatal("盘口超过容量仍增长")
	}
	if snap := b.Snapshot(); snap.LastUpdateID != "161" || len(snap.Bids) != 1 {
		t.Fatal("超限写入必须原子拒绝")
	}
}

// TestArchiveTimestampUnits 验证官方 2025 微秒文件与之前毫秒文件使用不同单位。
func TestArchiveTimestampUnits(t *testing.T) {
	for _, tc := range []struct{ date, stamp, want, micros string }{{"2025-01-01", "1735689600010866", "1735689600010", "1735689600010866"}, {"2024-12-31", "1735603200010", "1735603200010", "1735603200010000"}} {
		t.Run(tc.date, func(t *testing.T) {
			row := []string{"51175358", "17.80180000", "5.69000000", "100", "105", tc.stamp, "True", "True"}
			trade, err := ParseArchiveTrade(row, tc.date)
			if err != nil {
				t.Fatal(err)
			}
			if trade.TimestampMicros != tc.micros {
				t.Fatalf("微秒精度丢失: %+v", trade)
			}
			if fmtMillis(trade.Timestamp) != tc.want {
				t.Fatalf("单位错误: %+v", trade)
			}
		})
	}
	if _, err := ParseArchiveTrade([]string{"1", "1", "1", "1", "1", "1735689600000000", "true", "true"}, "2024-12-31"); err == nil {
		t.Fatal("单位错误不能默默产生未来成交")
	}
}

// fmtMillis 用整数格式检查时间戳，不通过浮点数比较精度。
func fmtMillis(value int64) string {
	return strconv.FormatInt(value, 10)
}

// TestRESTCursorAndBars 验证独立历史域名、API Key 头和 K 线排他游标。
func TestRESTCursorAndBars(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v3/historicalTrades":
			if r.URL.Query().Get("fromId") != "28457" || r.Header.Get("X-MBX-APIKEY") != "test-key" {
				t.Error("历史游标或 key 头错误")
			}
			_, _ = w.Write([]byte(`[{"id":28457,"price":"4.00000100","qty":"12.00000000","quoteQty":"48.000012","time":1499865549590,"isBuyerMaker":true}]`))
		case "/api/v3/klines":
			if r.URL.Query().Get("endTime") != "1499040000000" || r.URL.Query().Get("interval") != "1m" {
				t.Error("K 线游标非排他")
			}
			_, _ = w.Write([]byte(`[[1499040000000,"0.01634790","0.80000000","0.01575800","0.01577100","148976.11427815",1499644799999,"2434.19055334",308,"1756.87402397","28.46694368","0"]]`))
		default:
			t.Errorf("意外路径: %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer up.Close()
	s := settingsForTest(t)
	s.HistoricalURL = up.URL
	s.RESTURL = up.URL
	s.APIKey = "test-key"
	c := New(s)
	defer c.Close()
	from := int64(28457)
	trades, err := c.Trades(context.Background(), "LTCBTC", &from, 1000)
	if err != nil || len(trades) != 1 || trades[0].Side != "sell" {
		t.Fatalf("历史映射失败: %v %v", trades, err)
	}
	before := int64(1499040000001)
	bars, err := c.Bars(context.Background(), "LTCBTC", "1min", 1, &before)
	if err != nil || len(bars) != 1 || bars[0].Volume != 148976.11427815 || bars[0].Turnover != 2434.19055334 {
		t.Fatalf("成交量/额混淆: %v %v", bars, err)
	}
}

// TestRateLimitCooldown 验证 429 不自动重试且其他请求遵守共享冷却。
func TestRateLimitCooldown(t *testing.T) {
	calls := 0
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Retry-After", "60")
		w.WriteHeader(429)
		_, _ = w.Write([]byte(`{"code":-1003,"msg":"Too many requests"}`))
	}))
	defer up.Close()
	s := settingsForTest(t)
	s.RESTURL = up.URL
	c := New(s)
	defer c.Close()
	err := c.Ping(context.Background())
	var remote *Error
	if !errors.As(err, &remote) || remote.Code != "RATE_LIMITED" || remote.RetryAfter != time.Minute {
		t.Fatalf("限频信息丢失: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := c.Ping(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("冷却应可取消: %v", err)
	}
	if calls != 1 {
		t.Fatalf("冷却期间仍访问上游: %d", calls)
	}
}

// TestCatchupRejectsMissingIDs 验证历史页缺笔时不能宣称已完整回补。
func TestCatchupRejectsMissingIDs(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`[{"id":28459,"price":"4.00000100","qty":"12.00000000","time":1499865549590,"isBuyerMaker":true}]`))
	}))
	defer up.Close()
	s := settingsForTest(t)
	s.HistoricalURL = up.URL
	c := New(s)
	defer c.Close()
	if _, err := c.catchup(context.Background(), stream.Key{Kind: "trades", Symbol: "LTCBTC"}, 28458, 28460); err == nil {
		t.Fatal("历史页缺 ID 必须报告 gap")
	}
}
