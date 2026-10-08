// 本文件编排共享 WebSocket、成交缺口回补、盘口同步和 K 线实时帧。
package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/url"
	"strconv"
	"strings"
	"time"

	"binanceconnector/internal/model"
	"binanceconnector/internal/stream"
	"github.com/gorilla/websocket"
)

// socket 持有可取消的读取队列，满队列触发 TCP 背压而非丢帧。
type socket struct {
	messages chan []byte
	errors   chan error
	close    func()
	done     chan struct{}
}

// connect 建立官方 raw stream；自动响应服务器 ping，并响应 context 关闭。
func (c *Client) connect(ctx context.Context, name string) (*socket, error) {
	dialer := websocket.Dialer{Proxy: ProxyFunc(c.Settings), HandshakeTimeout: c.Settings.HTTPTimeout}
	conn, resp, err := dialer.DialContext(ctx, strings.TrimRight(c.Settings.WSURL, "/")+"/ws/"+name, nil)
	if err != nil && resp != nil && resp.Body != nil {
		resp.Body.Close()
	}
	if err != nil {
		return nil, err
	}
	conn.SetReadLimit(8 << 20)
	_ = conn.SetReadDeadline(time.Now().Add(90 * time.Second))
	conn.SetPingHandler(func(payload string) error {
		_ = conn.SetReadDeadline(time.Now().Add(90 * time.Second))
		return conn.WriteControl(websocket.PongMessage, []byte(payload), time.Now().Add(5*time.Second))
	})
	child, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(child, func() { conn.Close() })
	s := &socket{messages: make(chan []byte, c.Settings.BufferSize), errors: make(chan error, 1), done: make(chan struct{})}
	s.close = func() { cancel(); conn.Close(); stop() }
	go func() {
		defer close(s.done)
		defer s.close()
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				select {
				case s.errors <- err:
				default:
				}
				return
			}
			_ = conn.SetReadDeadline(time.Now().Add(90 * time.Second))
			select {
			case s.messages <- data:
			case <-child.Done():
				return
			}
		}
	}()
	return s, nil
}

// Run 管理 topic 的完整生命周期；重连保持成交游标，盘口重新获取基线。
func (c *Client) Run(ctx context.Context, key stream.Key, topic *stream.Topic) {
	delay := c.Settings.ReconnectMin
	var lastTradeID *int64
	for ctx.Err() == nil {
		started := time.Now()
		status := model.NewFrame("status", key.Symbol)
		status.Code = "CONNECTING"
		topic.Publish(status, nil)
		err := c.session(ctx, key, topic, &lastTradeID)
		topic.ClearBootstrap()
		if ctx.Err() != nil {
			return
		}
		status = model.NewFrame("status", key.Symbol)
		status.Code = "DISCONNECTED"
		status.Message = fmt.Sprint(err)
		complete := false
		status.Complete = &complete
		topic.Publish(status, nil)
		if time.Since(started) > 30*time.Second {
			delay = c.Settings.ReconnectMin
		}
		if err := Sleep(ctx, delay+time.Duration(rand.Float64()*float64(delay)/4)); err != nil {
			return
		}
		delay *= 2
		if delay > c.Settings.ReconnectMax {
			delay = c.Settings.ReconnectMax
		}
	}
}

// session 根据已验证的 key 选择一种市场流，原始与聚合成交使用独立 topic。
func (c *Client) session(ctx context.Context, key stream.Key, topic *stream.Topic, lastID **int64) error {
	name := strings.ToLower(key.Symbol)
	switch key.Kind {
	case "trades":
		name += "@trade"
	case "aggTrades":
		name += "@aggTrade"
	case "depth":
		name += "@depth@100ms"
	case "bars":
		name += "@kline_" + model.Periods[key.Period]
	default:
		return fmt.Errorf("未知订阅类型")
	}
	s, err := c.connect(ctx, name)
	if err != nil {
		return err
	}
	defer func() { s.close(); <-s.done }()
	if key.Kind == "depth" {
		return c.depthSession(ctx, key, topic, s)
	}
	if key.Kind == "bars" {
		bars, err := c.Bars(ctx, key.Symbol, key.Period, 2, nil)
		if err != nil {
			return err
		}
		f := model.NewFrame("snapshot", key.Symbol)
		f.Period = key.Period
		f.BarAggregation = "original"
		f.Bars = bars
		topic.Publish(f, &f)
	}
	status := model.NewFrame("status", key.Symbol)
	status.Code = "CONNECTED"
	topic.Publish(status, nil)
	pending := make([]model.Trade, 0, 256)
	flush := func() {
		if len(pending) == 0 {
			return
		}
		f := model.NewFrame("trades", key.Symbol)
		f.Trades = pending
		topic.Publish(f, nil)
		pending = make([]model.Trade, 0, 256)
	}
	defer flush()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-s.errors:
			return err
		case <-ticker.C:
			flush()
		case data := <-s.messages:
			var header struct {
				Event     string `json:"e"`
				EventTime int64  `json:"E"`
			}
			if err := json.Unmarshal(data, &header); err != nil {
				return err
			}
			if header.Event == "serverShutdown" {
				return fmt.Errorf("Binance 服务器计划重启")
			}
			if key.Kind == "bars" {
				if header.Event != "kline" {
					return fmt.Errorf("无效 K 线事件")
				}
				if err := publishBar(data, key, topic); err != nil {
					return err
				}
				continue
			}
			trade, err := ParseLiveTrade(data, key.Kind, key.Symbol)
			if err != nil {
				return err
			}
			id, _ := strconv.ParseInt(trade.TradeID, 10, 64)
			if *lastID != nil {
				if id <= **lastID {
					continue
				}
				if id > **lastID+1 {
					flush()
					missing, err := c.catchup(ctx, key, **lastID+1, id)
					if err != nil {
						f := model.NewFrame("status", key.Symbol)
						f.Code = "TRADE_GAP"
						f.Message = err.Error()
						f.FromID = strconv.FormatInt(**lastID+1, 10)
						f.ToID = strconv.FormatInt(id-1, 10)
						complete := false
						f.Complete = &complete
						topic.Publish(f, nil)
					} else {
						pending = append(pending, missing...)
					}
				}
			}
			pending = append(pending, trade)
			value := id
			*lastID = &value
			if len(pending) >= 256 {
				flush()
			}
		}
	}
}

// ParseLiveTrade 验证事件类型及 symbol，并显式区分 raw/aggregated 身份。
func ParseLiveTrade(data []byte, kind, symbol string) (model.Trade, error) {
	var e struct {
		Event     string `json:"e"`
		EventTime int64  `json:"E"`
		Symbol    string `json:"s"`
		ID        *int64 `json:"t"`
		AggID     *int64 `json:"a"`
		Time      int64  `json:"T"`
		Price     string `json:"p"`
		Size      string `json:"q"`
		Maker     *bool  `json:"m"`
		Ignore    bool   `json:"M"`
		First     int64  `json:"f"`
		Last      int64  `json:"l"`
	}
	if err := json.Unmarshal(data, &e); err != nil {
		return model.Trade{}, err
	}
	id, event, tradeKind := e.ID, "trade", "raw"
	if kind == "aggTrades" {
		id = e.AggID
		event = "aggTrade"
		tradeKind = "aggregated"
	}
	if e.Symbol != symbol || e.Event != event || id == nil || e.Maker == nil {
		return model.Trade{}, fmt.Errorf("成交事件与订阅身份不一致")
	}
	t, err := model.NewTrade(*id, e.Time, e.Price, e.Size, "", *e.Maker, tradeKind)
	if kind == "aggTrades" {
		t.FirstTradeID = strconv.FormatInt(e.First, 10)
		t.LastTradeID = strconv.FormatInt(e.Last, 10)
	}
	return t, err
}

// catchup 在有界分页内要求连续 ID；无法完整补齐时返回错误，不宣称完整。
func (c *Client) catchup(ctx context.Context, key stream.Key, start, exclusiveEnd int64) ([]model.Trade, error) {
	next := start
	out := make([]model.Trade, 0)
	for page := 0; page < c.Settings.CatchupPages && next < exclusiveEnd; page++ {
		var trades []model.Trade
		var err error
		if key.Kind == "trades" {
			trades, err = c.Trades(ctx, key.Symbol, &next, 1000)
		} else {
			trades, err = c.AggTrades(ctx, key.Symbol, url.Values{"fromId": {strconv.FormatInt(next, 10)}, "limit": {"1000"}})
		}
		if err != nil {
			return nil, err
		}
		previous := next
		for _, t := range trades {
			id, err := strconv.ParseInt(t.TradeID, 10, 64)
			if err != nil {
				return nil, err
			}
			if id < next {
				continue
			}
			if id >= exclusiveEnd {
				break
			}
			if id != next {
				return nil, fmt.Errorf("历史成交不连续: expected %d, got %d", next, id)
			}
			out = append(out, t)
			next++
		}
		if next == previous {
			return nil, fmt.Errorf("历史接口未覆盖缺失成交")
		}
	}
	if next < exclusiveEnd {
		return nil, fmt.Errorf("缺口超过 %d 页回补预算", c.Settings.CatchupPages)
	}
	return out, nil
}

// depthSession 先缓冲 WebSocket 再获取 REST，首个事件必须覆盖基线下一序号。
func (c *Client) depthSession(ctx context.Context, key stream.Key, topic *stream.Topic, s *socket) error {
	snapshot, err := c.Depth(ctx, key.Symbol, c.Settings.SnapshotLimit)
	if err != nil {
		return err
	}
	book, err := NewBook(key.Symbol, snapshot, c.Settings.MaxBookLevels)
	if err != nil {
		return err
	}
	ready := false
	timer := time.NewTimer(c.Settings.HTTPTimeout)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-s.errors:
			return err
		case <-timer.C:
			if !ready {
				return fmt.Errorf("盘口首个增量同步超时")
			}
		case data := <-s.messages:
			var event DepthEvent
			if err := json.Unmarshal(data, &event); err != nil {
				return err
			}
			if event.EventType == "serverShutdown" {
				return fmt.Errorf("Binance 服务器计划重启")
			}
			if event.EventType != "depthUpdate" {
				return fmt.Errorf("未知盘口事件")
			}
			applied, err := book.Apply(event)
			if err != nil {
				return err
			}
			if !applied {
				continue
			}
			if !ready {
				ready = true
				timer.Stop()
				topic.SetSnapshot(book.Snapshot)
				frame := book.Snapshot()
				topic.Publish(frame, nil)
				continue
			}
			f := model.NewFrame("depthDelta", key.Symbol)
			f.Timestamp = event.EventTime
			f.FirstUpdateID = strconv.FormatInt(event.FirstID, 10)
			f.LastUpdateID = strconv.FormatInt(event.FinalID, 10)
			f.Bids = event.Bids
			f.Asks = event.Asks
			topic.Publish(f, nil)
		}
	}
}

// publishBar 映射原生 forming/closed，并保存可用于新客户端的尾部基线。
func publishBar(data []byte, key stream.Key, topic *stream.Topic) error {
	var event struct {
		Symbol string `json:"s"`
		K      struct {
			Time        int64  `json:"t"`
			EndTime     int64  `json:"T"`
			LastTradeID int64  `json:"L"`
			Open        string `json:"o"`
			High        string `json:"h"`
			Low         string `json:"l"`
			Close       string `json:"c"`
			Volume      string `json:"v"`
			TakerVolume string `json:"V"`
			Quote       string `json:"q"`
			TakerQuote  string `json:"Q"`
			Closed      bool   `json:"x"`
			Interval    string `json:"i"`
		} `json:"k"`
	}
	if err := json.Unmarshal(data, &event); err != nil {
		return err
	}
	if event.Symbol != key.Symbol || event.K.Interval != model.Periods[key.Period] {
		return fmt.Errorf("K 线身份不一致")
	}
	values := make([]float64, 0, 6)
	for _, s := range []string{event.K.Open, event.K.High, event.K.Low, event.K.Close, event.K.Volume, event.K.Quote} {
		n, err := model.Number(s)
		if err != nil {
			return err
		}
		values = append(values, n)
	}
	bar := model.Bar{Timestamp: event.K.Time, Open: values[0], High: values[1], Low: values[2], Close: values[3], Volume: values[4], Turnover: values[5]}
	typeName := "forming"
	if event.K.Closed {
		typeName = "closed"
	}
	f := model.NewFrame(typeName, key.Symbol)
	f.Period = key.Period
	f.BarAggregation = "original"
	f.Bar = &bar
	bootstrap := model.NewFrame("snapshot", key.Symbol)
	bootstrap.Period = key.Period
	bootstrap.BarAggregation = "original"
	bootstrap.Bars = []model.Bar{bar}
	topic.Publish(f, &bootstrap)
	return nil
}
