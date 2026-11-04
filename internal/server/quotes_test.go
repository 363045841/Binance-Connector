// 报价投影与批量身份解析测试：不触达上游，只验证协议转换。
package server

import (
	"encoding/json"
	"testing"

	"binanceconnector/internal/model"
	"binanceconnector/internal/stream"
)

// bar 构造测试用日线。
func bar(timestamp int64, close float64) model.Bar {
	return model.Bar{Timestamp: timestamp, Open: close, High: close, Low: close, Close: close}
}

// entryOf 把帧编码成 stream.Entry，模拟 Hub 投递。
func entryOf(t *testing.T, value any) stream.Entry {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return stream.Entry{ID: "1:1", Seq: 1, Data: data}
}

// decodeQuote 解码映射后的报价帧。
func decodeQuote(t *testing.T, entry stream.Entry) map[string]any {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal(entry.Data, &payload); err != nil {
		t.Fatal(err)
	}
	return payload
}

// 验证快照直接产出最新价与昨收，值不变时不重复推送。
func TestQuoteProjectionSnapshot(t *testing.T) {
	projection := newQuoteProjection()
	instrumentID := model.InstrumentID("BTCUSDT")

	frame := model.LiveBarsFrame("snapshot", "BTCUSDT")
	frame.Bars = []model.Bar{bar(1000, 10), bar(2000, 11)}

	entry, ok := projection.mapEntry(entryOf(t, frame))
	if !ok {
		t.Fatal("snapshot should produce a quote")
	}
	payload := decodeQuote(t, entry)
	if payload["type"] != "quote" || payload["instrumentId"] != instrumentID {
		t.Fatalf("quote = %#v", payload)
	}
	if payload["last"] != float64(11) || payload["prevClose"] != float64(10) {
		t.Fatalf("quote = %#v, want last=11 prevClose=10", payload)
	}

	if _, ok := projection.mapEntry(entryOf(t, frame)); ok {
		t.Fatal("unchanged quote should not be pushed again")
	}
}

// 验证 forming 推进后报价随之更新，且只保留尾部两根。
func TestQuoteProjectionForming(t *testing.T) {
	projection := newQuoteProjection()
	instrumentID := model.InstrumentID("BTCUSDT")
	snapshot := model.LiveBarsFrame("snapshot", "BTCUSDT")
	snapshot.Bars = []model.Bar{bar(1000, 10), bar(2000, 11)}
	if _, ok := projection.mapEntry(entryOf(t, snapshot)); !ok {
		t.Fatal("snapshot should produce a quote")
	}

	forming := model.LiveBarsFrame("forming", "BTCUSDT")
	next := bar(2000, 12.5)
	forming.Bar = &next
	entry, ok := projection.mapEntry(entryOf(t, forming))
	if !ok {
		t.Fatal("forming change should produce a quote")
	}
	payload := decodeQuote(t, entry)
	if payload["last"] != 12.5 || payload["prevClose"] != float64(10) {
		t.Fatalf("quote = %#v, want last=12.5 prevClose=10", payload)
	}
	if payload["instrumentId"] != instrumentID {
		t.Fatalf("identity = %#v", payload["instrumentId"])
	}
}

// 验证状态帧不产出报价，避免把连接事件当成行情。
func TestQuoteProjectionIgnoresStatus(t *testing.T) {
	projection := newQuoteProjection()
	status := model.LiveBarsFrame("status", "BTCUSDT")
	status.Code = "CONNECTED"
	if _, ok := projection.mapEntry(entryOf(t, status)); ok {
		t.Fatal("status frame must not produce a quote")
	}
}

// 验证品种 ID 与交易对互转，前缀不匹配时明确拒绝。
func TestInstrumentIdentity(t *testing.T) {
	id := model.InstrumentID("BTCUSDT")
	if id != "binance:spot:BTCUSDT" {
		t.Fatalf("id = %q", id)
	}
	symbol, ok := model.SymbolFromInstrumentID(id)
	if !ok || symbol != "BTCUSDT" {
		t.Fatalf("symbol = %q ok=%v", symbol, ok)
	}
	if _, ok := model.SymbolFromInstrumentID("btc:spot:BTCUSDT"); ok {
		t.Fatal("foreign prefix must be rejected")
	}
}
