// 本文件提供显式开启的真实 Binance 连通性验证，默认测试不依赖外网。
package binance

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"testing"
	"time"

	"binanceconnector/internal/config"
	"binanceconnector/internal/model"
	"binanceconnector/internal/stream"
)

// liveSettings 读取真实配置，供 BINANCE_LIVE_TEST=1 的手工验证使用。
func liveSettings(t *testing.T) config.Settings {
	t.Helper()
	s, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// TestLiveTradeRange 验证真实时间范围查询返回聚合成交，并按连续成交 ID 覆盖区间。
func TestLiveTradeRange(t *testing.T) {
	if os.Getenv("BINANCE_LIVE_TEST") != "1" {
		t.Skip("设置 BINANCE_LIVE_TEST=1 执行外网验证")
	}
	c := New(liveSettings(t))
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	to := time.Now().Add(-5 * time.Second).UnixMilli()
	from := to - 10000
	trades, err := c.TradeRange(ctx, model.MarketSpot, "BTCUSDT", from, to)
	if err != nil {
		t.Fatal(err)
	}
	if len(trades) == 0 {
		t.Fatal("BTCUSDT 十秒区间无成交")
	}
	var previous int64
	for index, trade := range trades {
		id, err := strconv.ParseInt(trade.TradeID, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		if index > 0 && id != previous+1 {
			t.Fatalf("逐笔成交断序: %d -> %d", previous, id)
		}
		if trade.Timestamp < from || trade.Timestamp >= to || trade.Kind != "aggregated" {
			t.Fatalf("逐笔身份或时间不符: %+v", trade)
		}
		previous = id
	}
	t.Logf("真实十秒范围: %d 笔聚合成交，首笔 %s，末笔 %s", len(trades), trades[0].TradeID, trades[len(trades)-1].TradeID)
}

// TestLivePublicMarket 验证实际 REST/WS 成交可读取，不把本地夹具当成网络实测。
func TestLivePublicMarket(t *testing.T) {
	if os.Getenv("BINANCE_LIVE_TEST") != "1" {
		t.Skip("设置 BINANCE_LIVE_TEST=1 执行外网验证")
	}
	c := New(liveSettings(t))
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := c.Ping(ctx, model.MarketSpot); err != nil {
		t.Fatal("Binance REST 不可达:", err)
	}
	trades, err := c.Trades(ctx, model.MarketSpot, "BTCUSDT", nil, 2)
	if err != nil || len(trades) == 0 {
		t.Fatalf("实时成交 REST 失败: %v", err)
	}
	from, _ := strconv.ParseInt(trades[0].TradeID, 10, 64)
	// historicalTrades 仅在 api.binance.com 提供，该域名可能受地区/网络限制；
	// 这里只做可达性探测，不把端点波动判为逻辑缺陷。
	if history, err := c.Trades(ctx, model.MarketSpot, "BTCUSDT", &from, 2); err != nil {
		t.Logf("按 ID 历史回补在当前网络不可用（可用归档替代）: %v", err)
	} else if len(history) == 0 || history[0].TradeID != trades[0].TradeID {
		t.Fatalf("历史成交返回身份不一致: %v", history)
	}
	depth, err := c.Depth(ctx, model.MarketSpot, "BTCUSDT", 5)
	if err != nil || len(depth.Bids) == 0 || len(depth.Asks) == 0 {
		t.Fatalf("盘口 REST 失败: %v", err)
	}
	socket, err := c.connect(ctx, model.MarketSpot, "trades", "btcusdt@trade")
	if err != nil {
		t.Fatal("WebSocket 不可达:", err)
	}
	defer func() { socket.close(); <-socket.done }()
	select {
	case data := <-socket.messages:
		trade, err := ParseLiveTrade(data, "trades", "BTCUSDT")
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("实际成交: id=%s time=%d price=%s size=%s side=%s", trade.TradeID, trade.Timestamp, trade.Price, trade.Size, trade.Side)
	case err := <-socket.errors:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

// TestLivePerpMarket 验证永续新 WS 路由、聚合成交与资金费率/持仓量端点真实可用。
func TestLivePerpMarket(t *testing.T) {
	if os.Getenv("BINANCE_LIVE_TEST") != "1" {
		t.Skip("设置 BINANCE_LIVE_TEST=1 执行外网验证")
	}
	c := New(liveSettings(t))
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := c.Ping(ctx, model.MarketPerp); err != nil {
		t.Fatal("永续 REST 不可达:", err)
	}
	agg, err := c.AggTrades(ctx, model.MarketPerp, "BTCUSDT", nil)
	if err != nil || len(agg) == 0 {
		t.Fatalf("永续聚合成交失败: %v", err)
	}
	funding, err := c.PremiumIndex(ctx, model.MarketPerp, "BTCUSDT")
	if err != nil || funding.NextFundingAt == 0 {
		t.Fatalf("永续资金费率失败: %v", err)
	}
	interest, err := c.OpenInterest(ctx, model.MarketPerp, "BTCUSDT")
	if err != nil || interest.Size == "" {
		t.Fatalf("永续持仓量失败: %v", err)
	}
	// 永续 2026-04-23 后按流类型拆分路由，这里验证 /market 前缀确实收得到聚合成交。
	socket, err := c.connect(ctx, model.MarketPerp, "aggTrades", "btcusdt@aggTrade")
	if err != nil {
		t.Fatal("永续 WebSocket 不可达:", err)
	}
	defer func() { socket.close(); <-socket.done }()
	select {
	case data := <-socket.messages:
		trade, err := ParseLiveTrade(data, "aggTrades", "BTCUSDT")
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("永续聚合成交: id=%s price=%s", trade.TradeID, trade.Price)
	case err := <-socket.errors:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

// TestLiveDepthAndBars 验证真实上游盘口完成连续同步，Kline 保留正确开盘时间和数量。
func TestLiveDepthAndBars(t *testing.T) {
	if os.Getenv("BINANCE_LIVE_TEST") != "1" {
		t.Skip("设置 BINANCE_LIVE_TEST=1 执行外网验证")
	}
	s := liveSettings(t)
	c := New(s)
	defer c.Close()
	hub := stream.NewHub(s, c.Run)
	defer hub.Close()
	depth, err := hub.Subscribe(stream.Key{Market: model.MarketSpot, Kind: "depth", Symbol: "BTCUSDT"}, "")
	if err != nil {
		t.Fatal(err)
	}
	defer depth.Close()
	bars, err := hub.Subscribe(stream.Key{Market: model.MarketSpot, Kind: "bars", Symbol: "BTCUSDT", Period: "1min"}, "")
	if err != nil {
		t.Fatal(err)
	}
	defer bars.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var baseline int64
	deltaSeen, barSeen := false, false
	for !deltaSeen || !barSeen {
		select {
		case <-ctx.Done():
			t.Fatalf("真实流验证超时: depth=%v bars=%v", deltaSeen, barSeen)
		case entry := <-depth.Events:
			var f model.Frame
			if err := json.Unmarshal(entry.Data, &f); err != nil {
				t.Fatal(err)
			}
			if f.Type == "depthSnapshot" {
				baseline, _ = strconv.ParseInt(f.LastUpdateID, 10, 64)
				if len(f.Bids) == 0 || len(f.Asks) == 0 {
					t.Fatal("真实盘口无有效档位")
				}
				if f.InstrumentID != model.InstrumentID(model.MarketSpot, "BTCUSDT") {
					t.Fatalf("盘口帧身份错误: %+v", f)
				}
			}
			if f.Type == "depthDelta" {
				first, _ := strconv.ParseInt(f.FirstUpdateID, 10, 64)
				last, _ := strconv.ParseInt(f.LastUpdateID, 10, 64)
				if baseline == 0 || first > baseline+1 || last <= baseline {
					t.Fatalf("盘口不连续: baseline=%d first=%d last=%d", baseline, first, last)
				}
				baseline = last
				deltaSeen = true
			}
		case entry := <-bars.Events:
			var f model.Frame
			if err := json.Unmarshal(entry.Data, &f); err != nil {
				t.Fatal(err)
			}
			if f.Type == "forming" || f.Type == "closed" {
				if f.Bar == nil || f.Bar.Timestamp%60000 != 0 || f.Bar.Volume < 0 {
					t.Fatalf("实际 K 线字段无效: %+v", f.Bar)
				}
				barSeen = true
			}
		}
	}
	t.Log("实际盘口快照/连续增量及 Kline 已接收")
}
