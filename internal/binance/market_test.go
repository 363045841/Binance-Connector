// 本文件以 Binance 官方示例验证 REST/WS 适配、限频、历史单位、盘口连续性以及现货/永续两市场差异。
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
	s.CacheDir = t.TempDir()
	return s
}

// withMarket 复制并替换单个市场的地址组；map 内是值类型，必须回写。
func withMarket(s config.Settings, market string, edit func(*config.MarketSettings)) config.Settings {
	settings := s.Markets[market]
	edit(&settings)
	s.Markets[market] = settings
	return s
}

// withArchive 把指定市场的归档宿主指向本地服务。
func withArchive(s config.Settings, market, base string) config.Settings {
	return withMarket(s, market, func(m *config.MarketSettings) { m.Archive = base })
}

// withREST 把指定市场的主 REST 与历史宿主都指向本地服务。
func withREST(s config.Settings, market, base string) config.Settings {
	return withMarket(s, market, func(m *config.MarketSettings) {
		m.REST = base
		m.Historical = base
	})
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
	book, err := NewBook(model.MarketSpot, "BNBBTC", DepthSnapshot{LastID: 160, Bids: []model.Level{{"0.0024", "10"}}, Asks: []model.Level{{"0.0026", "100"}}}, 100)
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
	if snap.InstrumentID != "binance:spot:BNBBTC" {
		t.Fatalf("盘口帧必须携带品种身份: %+v", snap)
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
	b, err := NewBook(model.MarketPerp, "BTCUSDT", DepthSnapshot{LastID: 160, Bids: []model.Level{{"0.0024", "10"}}, Asks: []model.Level{{"0.0026", "100"}}}, 2)
	if err != nil {
		t.Fatal(err)
	}
	e := DepthEvent{Symbol: "BTCUSDT", FirstID: 161, FinalID: 161, Asks: []model.Level{{"0.0026", "0"}, {"0.0027", "1"}, {"0.0027", "2"}}}
	if applied, err := b.Apply(e); err != nil || !applied {
		t.Fatalf("等量替换不能误算超限: %v", err)
	}
	if _, err := b.Apply(DepthEvent{Symbol: "BTCUSDT", FirstID: 162, FinalID: 162, Bids: []model.Level{{"0.0023", "1"}}}); err == nil {
		t.Fatal("盘口超过容量仍增长")
	}
	if snap := b.Snapshot(); snap.LastUpdateID != "161" || len(snap.Bids) != 1 || snap.InstrumentID != "binance:perp:BTCUSDT" {
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

// TestRESTCursorAndBars 验证现货独立历史域名、API Key 头和 K 线排他游标。
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
	s := withREST(settingsForTest(t), model.MarketSpot, up.URL)
	s.APIKey = "test-key"
	c := New(s)
	defer c.Close()
	from := int64(28457)
	trades, err := c.Trades(context.Background(), model.MarketSpot, "LTCBTC", &from, 1000)
	if err != nil || len(trades) != 1 || trades[0].Side != "sell" {
		t.Fatalf("历史映射失败: %v %v", trades, err)
	}
	before := int64(1499040000001)
	bars, err := c.Bars(context.Background(), model.MarketSpot, "LTCBTC", "1min", 1, &before)
	if err != nil || len(bars) != 1 || bars[0].Volume != 148976.11427815 || bars[0].Turnover != 2434.19055334 {
		t.Fatalf("成交量/额混淆: %v %v", bars, err)
	}
}

// TestPerpEndpoints 验证永续走 fapi 路径、不带历史域名与 API Key，并解析提币价与持仓量。
func TestPerpEndpoints(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-MBX-APIKEY") != "" {
			t.Error("永续公开端点不应携带 API Key")
		}
		switch r.URL.Path {
		case "/fapi/v1/trades":
			if r.URL.Query().Get("symbol") != "BTCUSDT" || r.URL.Query().Get("fromId") != "" {
				t.Errorf("永续原始成交不应带 fromId，实际 %s", r.URL.RawQuery)
			}
			_, _ = w.Write([]byte(`[{"id":8244485759,"price":"76000.10","qty":"0.010","quoteQty":"760.001","time":1794003611000,"isBuyerMaker":false}]`))
		case "/fapi/v1/aggTrades":
			_, _ = w.Write([]byte(`[{"a":3511610949,"p":"75995.50","q":"0.002","nq":"0.002","f":8244437656,"l":8244437657,"T":1794003611143,"m":false}]`))
		case "/fapi/v1/klines":
			if r.URL.Query().Get("endTime") != "1794003600000" {
				t.Error("永续 K 线游标非排他")
			}
			_, _ = w.Write([]byte(`[[1794003000000,"75990.0","76010.0","75980.0","76000.0","120.5",1794003599999,"9157030.0",420,"60.2","4576000.0","0"]]`))
		case "/fapi/v1/premiumIndex":
			_, _ = w.Write([]byte(`{"symbol":"BTCUSDT","markPrice":"76003.20","indexPrice":"76039.13","lastFundingRate":"0.00010000","nextFundingTime":1794009600000,"time":1794003610000}`))
		case "/fapi/v1/openInterest":
			_, _ = w.Write([]byte(`{"symbol":"BTCUSDT","openInterest":"96269.071","time":1794003607958}`))
		case "/fapi/v1/fundingRate":
			_, _ = w.Write([]byte(`[{"symbol":"BTCUSDT","fundingTime":1793952000000,"fundingRate":"0.00005491","markPrice":"75582.81737681","rateType":"Regular"}]`))
		default:
			t.Errorf("意外路径: %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer up.Close()
	s := withMarket(settingsForTest(t), model.MarketPerp, func(m *config.MarketSettings) { m.REST = up.URL })
	s.APIKey = "test-key"
	c := New(s)
	defer c.Close()
	ctx := context.Background()
	raw, err := c.Trades(ctx, model.MarketPerp, "BTCUSDT", nil, 2)
	if err != nil || len(raw) != 1 || raw[0].Side != "buy" {
		t.Fatalf("永续原始成交失败: %v %v", raw, err)
	}
	// 永续忽略 fromId 且 historicalTrades 需签名，按 ID 回补必须明确拒绝。
	from := int64(8244485759)
	if _, err := c.Trades(ctx, model.MarketPerp, "BTCUSDT", &from, 2); err == nil {
		t.Fatal("永续原始成交按 ID 回补必须拒绝，不能返回最新成交冒充历史")
	}
	agg, err := c.AggTrades(ctx, model.MarketPerp, "BTCUSDT", nil)
	if err != nil || len(agg) != 1 || agg[0].TradeID != "3511610949" || agg[0].Price != "75995.50" {
		t.Fatalf("永续聚合成交失败: %v %v", agg, err)
	}
	before := int64(1794003600001)
	bars, err := c.Bars(ctx, model.MarketPerp, "BTCUSDT", "60min", 1, &before)
	if err != nil || len(bars) != 1 || bars[0].Close != 76000 {
		t.Fatalf("永续 K 线失败: %v %v", bars, err)
	}
	funding, err := c.PremiumIndex(ctx, model.MarketPerp, "BTCUSDT")
	if err != nil || funding.Rate != "0.00010000" || funding.NextFundingAt != 1794009600000 || funding.MarkPrice != "76003.20" {
		t.Fatalf("提币价失败: %+v %v", funding, err)
	}
	interest, err := c.OpenInterest(ctx, model.MarketPerp, "BTCUSDT")
	if err != nil || interest.Size != "96269.071" || interest.Timestamp != 1794003607958 {
		t.Fatalf("持仓量失败: %+v %v", interest, err)
	}
	history, err := c.FundingRate(ctx, model.MarketPerp, "BTCUSDT", 10)
	if err != nil || len(history) != 1 || history[0].Rate != "0.00005491" {
		t.Fatalf("资金费率历史失败: %+v %v", history, err)
	}
}

// TestPerpOnlyCapabilities 验证非永续市场调用永续专属端点时明确拒绝，且不发网络请求。
func TestPerpOnlyCapabilities(t *testing.T) {
	s := settingsForTest(t)
	// 指向不可达地址：若实现仍发请求，会因超时或连接失败暴露，而不是立刻返回能力错误。
	s = withREST(s, model.MarketSpot, "http://127.0.0.1:1")
	c := New(s)
	defer c.Close()
	ctx := context.Background()
	if _, err := c.PremiumIndex(ctx, model.MarketSpot, "BTCUSDT"); err == nil {
		t.Fatal("现货不应支持资金费率")
	}
	if _, err := c.OpenInterest(ctx, model.MarketSpot, "BTCUSDT"); err == nil {
		t.Fatal("现货不应支持持仓量")
	}
	if _, err := c.FundingRate(ctx, model.MarketSpot, "BTCUSDT", 1); err == nil {
		t.Fatal("现货不应支持资金费率历史")
	}
	if _, err := c.PremiumIndex(ctx, "usdm", "BTCUSDT"); err == nil {
		t.Fatal("未知市场必须拒绝")
	}
	if _, err := c.Trades(ctx, "usdm", "BTCUSDT", nil, 1); err == nil {
		t.Fatal("未知市场必须拒绝")
	}
}

// TestWeightTables 验证两市场权重分档，避免共用一套导致限频误判。
func TestWeightTables(t *testing.T) {
	for _, tc := range []struct{ market string; limit, want int }{
		{model.MarketSpot, 100, 5}, {model.MarketSpot, 500, 25}, {model.MarketSpot, 1000, 50}, {model.MarketSpot, 5000, 250},
		{model.MarketPerp, 20, 2}, {model.MarketPerp, 100, 5}, {model.MarketPerp, 500, 10}, {model.MarketPerp, 1000, 20},
	} {
		if got := depthWeight(tc.market, tc.limit); got != tc.want {
			t.Fatalf("%s depth limit=%d 权重 = %d, want %d", tc.market, tc.limit, got, tc.want)
		}
	}
	for _, tc := range []struct{ market string; limit, want int }{
		{model.MarketSpot, 500, 2}, {model.MarketSpot, 1500, 2},
		{model.MarketPerp, 50, 1}, {model.MarketPerp, 100, 2}, {model.MarketPerp, 500, 5}, {model.MarketPerp, 1000, 5}, {model.MarketPerp, 1500, 10},
	} {
		if got := klinesWeight(tc.market, tc.limit); got != tc.want {
			t.Fatalf("%s klines limit=%d 权重 = %d, want %d", tc.market, tc.limit, got, tc.want)
		}
	}
}

// TestStreamPaths 验证永续按流类型拆分路由，现货保持旧路径。
func TestStreamPaths(t *testing.T) {
	for _, tc := range []struct{ market, kind, want string }{
		{model.MarketSpot, "trades", "/ws/"},
		{model.MarketSpot, "depth", "/ws/"},
		{model.MarketSpot, "bars", "/ws/"},
		{model.MarketPerp, "trades", "/public/ws/"},
		{model.MarketPerp, "depth", "/public/ws/"},
		{model.MarketPerp, "aggTrades", "/market/ws/"},
		{model.MarketPerp, "bars", "/market/ws/"},
	} {
		got, err := streamPath(tc.market, tc.kind)
		if err != nil || got != tc.want {
			t.Fatalf("%s/%s = %q (%v), want %q", tc.market, tc.kind, got, err, tc.want)
		}
	}
	if _, err := streamPath(model.MarketPerp, "quotes"); err == nil {
		t.Fatal("未知流类型必须拒绝")
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
	s := withREST(settingsForTest(t), model.MarketSpot, up.URL)
	c := New(s)
	defer c.Close()
	err := c.Ping(context.Background(), model.MarketSpot)
	var remote *Error
	if !errors.As(err, &remote) || remote.Code != "RATE_LIMITED" || remote.RetryAfter != time.Minute {
		t.Fatalf("限频信息丢失: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := c.Ping(ctx, model.MarketSpot); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("冷却应可取消: %v", err)
	}
	if calls != 1 {
		t.Fatalf("冷却期间仍访问上游: %d", calls)
	}
}

// TestRateLimitIsolatedPerMarket 验证现货冷却不影响永续预算。
func TestRateLimitIsolatedPerMarket(t *testing.T) {
	limited := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "60")
		w.WriteHeader(429)
		_, _ = w.Write([]byte(`{"code":-1003,"msg":"Too many requests"}`))
	}))
	defer limited.Close()
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{}`))
	}))
	defer healthy.Close()
	s := withREST(settingsForTest(t), model.MarketSpot, limited.URL)
	s = withREST(s, model.MarketPerp, healthy.URL)
	c := New(s)
	defer c.Close()
	if err := c.Ping(context.Background(), model.MarketSpot); err == nil {
		t.Fatal("现货应触发限频")
	}
	if err := c.Ping(context.Background(), model.MarketPerp); err != nil {
		t.Fatalf("现货冷却不应波及永续: %v", err)
	}
}

// TestCatchupRejectsMissingIDs 验证历史页缺笔时不能宣称已完整回补。
func TestCatchupRejectsMissingIDs(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`[{"id":28459,"price":"4.00000100","qty":"12.00000000","time":1499865549590,"isBuyerMaker":true}]`))
	}))
	defer up.Close()
	s := withREST(settingsForTest(t), model.MarketSpot, up.URL)
	c := New(s)
	defer c.Close()
	if _, err := c.catchup(context.Background(), stream.Key{Market: model.MarketSpot, Kind: "trades", Symbol: "LTCBTC"}, 28458, 28460); err == nil {
		t.Fatal("历史页缺 ID 必须报告 gap")
	}
}
