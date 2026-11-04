// 报价端点：把日线实时流投影成最新价与昨收，不新增上游订阅路径。
package server

import (
	"encoding/json"
	"net/http"

	"binanceconnector/internal/model"
	"binanceconnector/internal/stream"
)

// quoteBarLimit 只保留尾部窗口：两根用于计算，多留一根给收线后的修订。
const quoteBarLimit = 3

// quoteMarker 已推送的报价内容；值不变时不重复推送。
type quoteMarker struct {
	last      float64
	prevClose float64
}

// quoteProjection 按品种维护日线尾部，输出最新价与昨收。
type quoteProjection struct {
	bars    map[string][]model.Bar
	emitted map[string]quoteMarker
}

// newQuoteProjection 创建空投影；状态只属于当前连接。
func newQuoteProjection() *quoteProjection {
	return &quoteProjection{bars: map[string][]model.Bar{}, emitted: map[string]quoteMarker{}}
}

// mapEntry 把日线帧改写成报价帧；没有新数值时不推送。
func (p *quoteProjection) mapEntry(entry stream.Entry) (stream.Entry, bool) {
	var frame struct {
		Type         string      `json:"type"`
		InstrumentID string      `json:"instrumentId"`
		Bar          *model.Bar  `json:"bar"`
		Bars         []model.Bar `json:"bars"`
	}
	if err := json.Unmarshal(entry.Data, &frame); err != nil || frame.InstrumentID == "" {
		return entry, false
	}
	switch frame.Type {
	case "snapshot":
		p.bars[frame.InstrumentID] = tailBars(frame.Bars, quoteBarLimit)
	case "forming", "closed":
		if frame.Bar == nil {
			return entry, false
		}
		p.upsert(frame.InstrumentID, *frame.Bar)
	default:
		return entry, false
	}
	data, ok := p.quote(frame.InstrumentID)
	if !ok {
		return entry, false
	}
	return stream.Entry{ID: entry.ID, Seq: entry.Seq, Data: data}, true
}

// upsert 按时间戳升序插入或覆盖，只保留尾部窗口。
func (p *quoteProjection) upsert(instrumentID string, bar model.Bar) {
	bars := p.bars[instrumentID]
	for i, existing := range bars {
		if existing.Timestamp == bar.Timestamp {
			bars[i] = bar
			return
		}
		if existing.Timestamp > bar.Timestamp {
			bars = append(bars, model.Bar{})
			copy(bars[i+1:], bars[i:])
			bars[i] = bar
			p.bars[instrumentID] = bars
			return
		}
	}
	p.bars[instrumentID] = tailBars(append(bars, bar), quoteBarLimit)
}

// quote 倒数两根即最新价与昨收；不足两根时没有可比基准。
func (p *quoteProjection) quote(instrumentID string) ([]byte, bool) {
	bars := p.bars[instrumentID]
	if len(bars) < 2 {
		return nil, false
	}
	last, previous := bars[len(bars)-1], bars[len(bars)-2]
	marker := quoteMarker{last: last.Close, prevClose: previous.Close}
	if p.emitted[instrumentID] == marker {
		return nil, false
	}
	p.emitted[instrumentID] = marker
	data, err := json.Marshal(struct {
		Type         string  `json:"type"`
		InstrumentID string  `json:"instrumentId"`
		Last         float64 `json:"last"`
		PrevClose    float64 `json:"prevClose"`
		Timestamp    int64   `json:"timestamp"`
	}{"quote", instrumentID, last.Close, previous.Close, last.Timestamp})
	if err != nil {
		return nil, false
	}
	return data, true
}

// tailBars 返回切片末尾至多 n 个元素的副本。
func tailBars(bars []model.Bar, n int) []model.Bar {
	if len(bars) <= n {
		return append([]model.Bar(nil), bars...)
	}
	return append([]model.Bar(nil), bars[len(bars)-n:]...)
}

// liveQuotes 订阅日线实时流并投影成报价帧。
func (s *Server) liveQuotes(w http.ResponseWriter, r *http.Request) {
	symbols, ok := queryInstruments(w, r)
	if !ok {
		return
	}
	keys := make([]stream.Key, len(symbols))
	for i, symbol := range symbols {
		keys[i] = stream.Key{Kind: "bars", Symbol: symbol, Period: "daily"}
	}
	s.sse(w, r, keys, newQuoteProjection().mapEntry)
}
