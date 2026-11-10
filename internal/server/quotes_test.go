// 报价投影：昨收基准由 REST 确认，实时快照只有最新一根时也不能丢失基准。
package server

import (
	"encoding/json"
	"testing"

	"binanceconnector/internal/model"
	"binanceconnector/internal/stream"
)

const (
	quoteTestInstrument = "binance:spot:BTCUSDT"
	quoteTestSymbol     = "BTCUSDT"
	dayOne              = int64(1794182400000)
	dayTwo              = int64(1794268800000)
	dayThree            = int64(1794355200000)
)

// dailyBar 构造一根只关心时间与收盘价的日线。
func dailyBar(timestamp int64, close float64) model.Bar {
	return model.Bar{Timestamp: timestamp, Open: close, High: close, Low: close, Close: close}
}

// mapFrame 序列化成 SSE 载荷后走一次投影映射，返回报价载荷。
func mapFrame(t *testing.T, projection *quoteProjection, frame model.Frame) (map[string]any, bool) {
	t.Helper()
	data, err := json.Marshal(frame)
	if err != nil {
		t.Fatalf("序列化帧失败: %v", err)
	}
	entry, ok := projection.mapEntry(stream.Entry{Data: data})
	if !ok {
		return nil, false
	}
	var quote map[string]any
	if err := json.Unmarshal(entry.Data, &quote); err != nil {
		t.Fatalf("解析报价帧失败: %v", err)
	}
	return quote, true
}

// snapshotFrame 构造实时流的日线快照帧。
func snapshotFrame(bars ...model.Bar) model.Frame {
	frame := model.LiveBarsFrame("snapshot", model.MarketSpot, quoteTestSymbol)
	frame.Bars = bars
	return frame
}

// formingFrame 构造实时流的日线形成中帧。
func formingFrame(bar model.Bar) model.Frame {
	frame := model.LiveBarsFrame("forming", model.MarketSpot, quoteTestSymbol)
	frame.Bar = &bar
	return frame
}

// assertQuote 校验报价的最新价与昨收。
func assertQuote(t *testing.T, quote map[string]any, last, prevClose float64) {
	t.Helper()
	if quote["type"] != "quote" || quote["instrumentId"] != quoteTestInstrument {
		t.Fatalf("报价帧身份错误: %#v", quote)
	}
	if quote["last"] != last || quote["prevClose"] != prevClose {
		t.Fatalf("报价 = last %v / prevClose %v，期望 %v / %v", quote["last"], quote["prevClose"], last, prevClose)
	}
}

func TestQuoteNeedsTwoBarBasis(t *testing.T) {
	for _, tc := range []struct {
		name    string
		basis   []model.Bar
		wantOut bool
	}{
		{"基准补齐昨收", []model.Bar{dailyBar(dayOne, 100), dailyBar(dayTwo, 110)}, true},
		{"没有基准", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			projection := newQuoteProjection(map[string][]model.Bar{quoteTestInstrument: tc.basis})
			quote, ok := mapFrame(t, projection, snapshotFrame(dailyBar(dayTwo, 111)))
			if ok != tc.wantOut {
				t.Fatalf("产出报价 = %v，期望 %v", ok, tc.wantOut)
			}
			if ok {
				assertQuote(t, quote, 111, 100)
			}
		})
	}
}

func TestQuoteSkipsRepeatedValues(t *testing.T) {
	projection := newQuoteProjection(map[string][]model.Bar{
		quoteTestInstrument: {dailyBar(dayOne, 100), dailyBar(dayTwo, 110)},
	})
	frame := snapshotFrame(dailyBar(dayTwo, 110))
	if _, ok := mapFrame(t, projection, frame); !ok {
		t.Fatal("首帧应产出报价")
	}
	if _, ok := mapFrame(t, projection, frame); ok {
		t.Fatal("数值未变时不应重复推送")
	}
}

func TestQuoteFollowsFormingBarAcrossTradingDay(t *testing.T) {
	projection := newQuoteProjection(map[string][]model.Bar{
		quoteTestInstrument: {dailyBar(dayOne, 100), dailyBar(dayTwo, 110)},
	})
	mapFrame(t, projection, snapshotFrame(dailyBar(dayTwo, 110)))

	quote, ok := mapFrame(t, projection, formingFrame(dailyBar(dayTwo, 112)))
	if !ok {
		t.Fatal("形成中的日线应有新报价")
	}
	assertQuote(t, quote, 112, 100)

	quote, ok = mapFrame(t, projection, formingFrame(dailyBar(dayThree, 120)))
	if !ok {
		t.Fatal("跨交易日后应有新报价")
	}
	// 昨收取上一交易日最后一根已知收盘价，收线帧到达后自然被覆盖。
	assertQuote(t, quote, 120, 112)
}
