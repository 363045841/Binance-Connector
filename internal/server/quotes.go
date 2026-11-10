// 报价端点：把日线实时流投影成最新价与昨收；昨收基准在连接建立时由 REST 确认。
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"binanceconnector/internal/model"
	"binanceconnector/internal/stream"
)

// quoteBarLimit 只保留尾部窗口：两根用于计算，多留一根给收线后的修订。
const quoteBarLimit = 3

// quoteBasisSize 报价基准所需的日线根数：今收与昨收，缺一根就算不出涨跌。
const quoteBasisSize = 2

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

// newQuoteProjection 用连接建立前确认的日线基准初始化投影；状态只属于当前连接。
// 实时流的快照只保证最新一根，基准必须由此处补齐，否则报价永远发不出来。
func newQuoteProjection(basis map[string][]model.Bar) *quoteProjection {
	bars := make(map[string][]model.Bar, len(basis))
	for instrumentID, tail := range basis {
		bars[instrumentID] = tailBars(tail, quoteBarLimit)
	}
	return &quoteProjection{bars: bars, emitted: map[string]quoteMarker{}}
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
		// 快照只带最新一根时不能覆盖基准，按时间戳合并。
		for _, bar := range frame.Bars {
			p.upsert(frame.InstrumentID, bar)
		}
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

// liveQuotes 订阅日线实时流并投影成报价帧；现货与永续各自一条订阅。
func (s *Server) liveQuotes(w http.ResponseWriter, r *http.Request) {
	instruments, ok := queryInstruments(w, r)
	if !ok {
		return
	}
	basis, err := s.quoteBasis(r.Context(), instruments)
	if err != nil {
		upstream(w, err)
		return
	}
	keys := make([]stream.Key, len(instruments))
	for i, instrument := range instruments {
		keys[i] = stream.Key{Market: instrument.Market, Kind: "bars", Symbol: instrument.Symbol, Period: "daily"}
	}
	s.sse(w, r, keys, newQuoteProjection(basis).mapEntry)
}

// quoteBasis 在连接建立前确认每个品种的今收与昨收；不足两根时显式失败，
// 而不是让报价通道静默等一个永远不会到来的基准。
func (s *Server) quoteBasis(ctx context.Context, instruments []marketSymbol) (map[string][]model.Bar, error) {
	basis := make(map[string][]model.Bar, len(instruments))
	for _, instrument := range instruments {
		bars, err := s.Client.Bars(ctx, instrument.Market, instrument.Symbol, "daily", quoteBasisSize, nil)
		if err != nil {
			return nil, err
		}
		if len(bars) < quoteBasisSize {
			return nil, fmt.Errorf("%s: 日线只有 %d 根，缺少昨收基准", instrument.Symbol, len(bars))
		}
		basis[model.InstrumentID(instrument.Market, instrument.Symbol)] = bars
	}
	return basis, nil
}
