// 本文件实现 V1 品种目录、K 线、成交分页和盘口快照适配。
package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"binanceconnector/internal/model"
)

// rawTrade 对应 Binance 官方 recent/historicalTrades 成交格式。
type rawTrade struct {
	ID         *int64 `json:"id"`
	Price      string `json:"price"`
	Qty        string `json:"qty"`
	QuoteQty   string `json:"quoteQty"`
	Time       int64  `json:"time"`
	BuyerMaker *bool  `json:"isBuyerMaker"`
}

// Trades 返回原始成交；fromId 仅走支持历史 ID 游标的独立域名。
func (c *Client) Trades(ctx context.Context, symbol string, fromID *int64, limit int) ([]model.Trade, error) {
	q := url.Values{"symbol": {symbol}, "limit": {strconv.Itoa(limit)}}
	base, path, key := c.Settings.RESTURL, "/api/v3/trades", false
	if fromID != nil {
		base = c.Settings.HistoricalURL
		path = "/api/v3/historicalTrades"
		q.Set("fromId", strconv.FormatInt(*fromID, 10))
		key = true
	}
	var raw []rawTrade
	if err := c.get(ctx, base, path, q, 25, key, &raw); err != nil {
		return nil, err
	}
	result := make([]model.Trade, 0, len(raw))
	for _, item := range raw {
		if item.ID == nil || item.BuyerMaker == nil {
			return nil, fmt.Errorf("成交缺少 ID 或主动方向标记")
		}
		t, err := model.NewTrade(*item.ID, item.Time, item.Price, item.Qty, item.QuoteQty, *item.BuyerMaker, "raw")
		if err != nil {
			return nil, err
		}
		result = append(result, t)
	}
	return result, nil
}

// AggTrades 按独立聚合成交身份分页，不混用原始 tradeId。
func (c *Client) AggTrades(ctx context.Context, symbol string, query url.Values) ([]model.Trade, error) {
	query.Set("symbol", symbol)
	var raw []struct {
		ID     *int64 `json:"a"`
		Price  string `json:"p"`
		Qty    string `json:"q"`
		First  int64  `json:"f"`
		Last   int64  `json:"l"`
		Time   int64  `json:"T"`
		Maker  *bool  `json:"m"`
		Ignore bool   `json:"M"`
	}
	if err := c.get(ctx, c.Settings.RESTURL, "/api/v3/aggTrades", query, 4, false, &raw); err != nil {
		return nil, err
	}
	result := make([]model.Trade, 0, len(raw))
	for _, item := range raw {
		if item.ID == nil || item.Maker == nil {
			return nil, fmt.Errorf("聚合成交缺少 ID 或主动方向标记")
		}
		t, err := model.NewTrade(*item.ID, item.Time, item.Price, item.Qty, "", *item.Maker, "aggregated")
		if err != nil {
			return nil, err
		}
		t.FirstTradeID = strconv.FormatInt(item.First, 10)
		t.LastTradeID = strconv.FormatInt(item.Last, 10)
		result = append(result, t)
	}
	return result, nil
}

// Bars 按 exclusive beforeTimestamp 查询 UTC 原生 K 线。
func (c *Client) Bars(ctx context.Context, symbol, period string, limit int, before *int64) ([]model.Bar, error) {
	q := url.Values{"symbol": {symbol}, "interval": {model.Periods[period]}, "limit": {strconv.Itoa(limit)}}
	if before != nil {
		q.Set("endTime", strconv.FormatInt(*before-1, 10))
	}
	var raw [][]json.RawMessage
	if err := c.get(ctx, c.Settings.RESTURL, "/api/v3/klines", q, 2, false, &raw); err != nil {
		return nil, err
	}
	result := make([]model.Bar, 0, len(raw))
	for _, row := range raw {
		if len(row) < 8 {
			return nil, fmt.Errorf("无效 K 线列数")
		}
		var timestamp int64
		if err := json.Unmarshal(row[0], &timestamp); err != nil {
			return nil, err
		}
		values := make([]float64, 0, 6)
		for _, i := range []int{1, 2, 3, 4, 5, 7} {
			var text string
			if err := json.Unmarshal(row[i], &text); err != nil {
				return nil, err
			}
			n, err := model.Number(text)
			if err != nil {
				return nil, err
			}
			values = append(values, n)
		}
		result = append(result, model.Bar{Timestamp: timestamp, Open: values[0], High: values[1], Low: values[2], Close: values[3], Volume: values[4], Turnover: values[5]})
	}
	return result, nil
}

// DepthSnapshot 是官方同步协议的 REST 基线。
type DepthSnapshot struct {
	LastID int64         `json:"lastUpdateId"`
	Bids   []model.Level `json:"bids"`
	Asks   []model.Level `json:"asks"`
}

// Depth 获取快照，正确申报按档位数量变化的请求权重。
func (c *Client) Depth(ctx context.Context, symbol string, limit int) (DepthSnapshot, error) {
	weight := 5
	if limit > 100 {
		weight = 25
	}
	if limit > 500 {
		weight = 50
	}
	if limit > 1000 {
		weight = 250
	}
	var snap DepthSnapshot
	err := c.get(ctx, c.Settings.RESTURL, "/api/v3/depth", url.Values{"symbol": {symbol}, "limit": {strconv.Itoa(limit)}}, weight, false, &snap)
	return snap, err
}

// Instruments 共用小时级目录缓存；首个上游错误不会写入缓存。
func (c *Client) Instruments(ctx context.Context) ([]model.Instrument, error) {
	c.directoryMu.Lock()
	defer c.directoryMu.Unlock()
	var raw struct {
		Symbols []struct {
			Symbol, Status, BaseAsset, QuoteAsset string
			Filters                               []struct {
				Type string `json:"filterType"`
				Tick string `json:"tickSize"`
				Step string `json:"stepSize"`
			}
		}
	}
	if time.Since(c.directoryAt) < time.Hour && len(c.directory) > 0 {
		if err := json.Unmarshal(c.directory, &raw); err != nil {
			return nil, err
		}
	} else {
		if err := c.get(ctx, c.Settings.RESTURL, "/api/v3/exchangeInfo", url.Values{"showPermissionSets": {"false"}}, 20, false, &raw); err != nil {
			return nil, err
		}
		c.directory, _ = json.Marshal(raw)
		c.directoryAt = time.Now()
	}
	result := make([]model.Instrument, 0, len(raw.Symbols))
	for _, s := range raw.Symbols {
		if s.Status != "TRADING" {
			continue
		}
		if _, err := model.Symbol(s.Symbol); err != nil {
			continue
		}
		tick, lot := 0.0, 0.0
		for _, f := range s.Filters {
			if f.Type == "PRICE_FILTER" {
				var err error
				tick, err = model.Number(f.Tick)
				if err != nil {
					return nil, err
				}
			}
			if f.Type == "LOT_SIZE" {
				var err error
				lot, err = model.Number(f.Step)
				if err != nil {
					return nil, err
				}
			}
		}
		result = append(result, model.Instrument{ID: "binance:spot:" + s.Symbol, SourceID: model.SourceID, Symbol: s.Symbol, Name: s.BaseAsset + " / " + s.QuoteAsset, AssetClass: "crypto", Exchange: "BINANCE", SessionID: "BINANCE_SPOT", Currency: s.QuoteAsset, TickSize: tick, LotSize: lot, ProviderRef: map[string]string{"symbol": s.Symbol, "market": model.Market}, Capabilities: model.Capabilities()})
	}
	return result, nil
}

// Ping 仅验证实际 Binance HTTP 可达性，不伪装成完整 WebSocket 健康状态。
func (c *Client) Ping(ctx context.Context) error {
	var out map[string]any
	return c.get(ctx, c.Settings.RESTURL, "/api/v3/ping", nil, 1, false, &out)
}
