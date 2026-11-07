// 本文件实现 V1 品种目录、K 线、成交分页和盘口快照适配。
// 现货（spot）与 USDT-M 永续（perp）共用同一套协议，差别集中在端点路径、请求权重与归档列格式，
// 这三项都按市场显式声明，不在实现里靠分支猜测。
package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"binanceconnector/internal/config"
	"binanceconnector/internal/model"
)

// endpoints 是单个市场的 REST 端点与权重声明。
// 权重取自官方文档：现货 aggTrades 4 / exchangeInfo 20 / klines 2 / depth 分档 5-250；
// 永续 aggTrades 20 / exchangeInfo 1 / klines 按 limit 分档 / depth 分档 2-20。
type endpoints struct {
	trades        string
	historical    string
	aggTrades     string
	klines        string
	depth         string
	exchangeInfo  string
	ping          string
	premiumIndex  string
	openInterest  string
	fundingRate   string
	aggTradesCost int
	exchangeCost  int
	klinesByLimit bool
}

// marketEndpoints 按市场返回端点声明；未知市场返回 false。
func marketEndpoints(market string) (endpoints, bool) {
	switch market {
	case model.MarketSpot:
		return endpoints{
			trades:        "/api/v3/trades",
			historical:    "/api/v3/historicalTrades",
			aggTrades:     "/api/v3/aggTrades",
			klines:        "/api/v3/klines",
			depth:         "/api/v3/depth",
			exchangeInfo:  "/api/v3/exchangeInfo",
			ping:          "/api/v3/ping",
			aggTradesCost: 4,
			exchangeCost:  20,
		}, true
	case model.MarketPerp:
		return endpoints{
			trades:        "/fapi/v1/trades",
			aggTrades:     "/fapi/v1/aggTrades",
			klines:        "/fapi/v1/klines",
			depth:         "/fapi/v1/depth",
			exchangeInfo:  "/fapi/v1/exchangeInfo",
			ping:          "/fapi/v1/ping",
			premiumIndex:  "/fapi/v1/premiumIndex",
			openInterest:  "/fapi/v1/openInterest",
			fundingRate:   "/fapi/v1/fundingRate",
			aggTradesCost: 20,
			exchangeCost:  1,
			klinesByLimit: true,
		}, true
	}
	return endpoints{}, false
}

// rawTrade 对应 Binance 官方 recent/historicalTrades 成交格式。
type rawTrade struct {
	ID         *int64 `json:"id"`
	Price      string `json:"price"`
	Qty        string `json:"qty"`
	QuoteQty   string `json:"quoteQty"`
	Time       int64  `json:"time"`
	BuyerMaker *bool  `json:"isBuyerMaker"`
}

// Trades 返回原始成交；现货的 fromId 只在该市场独立的历史域名提供（需 API Key），
// 永续同一端点原生支持 fromId，因此按市场分别选择端点族，不做隐式回退。
func (c *Client) Trades(ctx context.Context, market, symbol string, fromID *int64, limit int) ([]model.Trade, error) {
	spec, ok := marketEndpoints(market)
	if !ok {
		return nil, fmt.Errorf("不支持的市场: %s", market)
	}
	q := url.Values{"symbol": {symbol}, "limit": {strconv.Itoa(limit)}}
	endpoint, path, key := config.EndpointREST, spec.trades, false
	if fromID != nil {
		// 永续 /fapi/v1/trades 忽略 fromId，historicalTrades 又需签名，
		// 因此按 ID 回补原始成交在永续上不可用；明确拒绝而不是返回最新成交冒充历史。
		if spec.historical == "" {
			return nil, fmt.Errorf("永续原始成交不支持按 ID 回补，请使用聚合成交或归档")
		}
		q.Set("fromId", strconv.FormatInt(*fromID, 10))
		endpoint, path, key = config.EndpointHistory, spec.historical, true
	}
	var raw []rawTrade
	if err := c.get(ctx, market, endpoint, path, q, 25, key, &raw); err != nil {
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

// AggTrades 按独立聚合成交身份分页，不混用原始 tradeId；query 为 nil 时等价于空参数。
func (c *Client) AggTrades(ctx context.Context, market, symbol string, query url.Values) ([]model.Trade, error) {
	spec, ok := marketEndpoints(market)
	if !ok {
		return nil, fmt.Errorf("不支持的市场: %s", market)
	}
	if query == nil {
		query = url.Values{}
	}
	query.Set("symbol", symbol)
	var raw []struct {
		ID    *int64 `json:"a"`
		Price string `json:"p"`
		Qty   string `json:"q"`
		First int64  `json:"f"`
		Last  int64  `json:"l"`
		Time  int64  `json:"T"`
		Maker *bool  `json:"m"`
	}
	if err := c.get(ctx, market, config.EndpointREST, spec.aggTrades, query, spec.aggTradesCost, false, &raw); err != nil {
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

// klinesWeight 计算 K 线请求权重：现货固定 2，永续按 limit 分档 1/2/5/10。
func klinesWeight(market string, limit int) int {
	if market != model.MarketPerp {
		return 2
	}
	switch {
	case limit < 100:
		return 1
	case limit < 500:
		return 2
	case limit <= 1000:
		return 5
	default:
		return 10
	}
}

// Bars 按 exclusive beforeTimestamp 查询 UTC 原生 K 线。
func (c *Client) Bars(ctx context.Context, market, symbol, period string, limit int, before *int64) ([]model.Bar, error) {
	spec, ok := marketEndpoints(market)
	if !ok {
		return nil, fmt.Errorf("不支持的市场: %s", market)
	}
	q := url.Values{"symbol": {symbol}, "interval": {model.Periods[period]}, "limit": {strconv.Itoa(limit)}}
	if before != nil {
		q.Set("endTime", strconv.FormatInt(*before-1, 10))
	}
	var raw [][]json.RawMessage
	if err := c.get(ctx, market, config.EndpointREST, spec.klines, q, klinesWeight(market, limit), false, &raw); err != nil {
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

// depthWeight 按官方分档申报权重：现货 5/25/50/250，永续 2/5/10/20。
func depthWeight(market string, limit int) int {
	if market == model.MarketPerp {
		switch {
		case limit <= 50:
			return 2
		case limit <= 100:
			return 5
		case limit <= 500:
			return 10
		default:
			return 20
		}
	}
	switch {
	case limit > 1000:
		return 250
	case limit > 500:
		return 50
	case limit > 100:
		return 25
	default:
		return 5
	}
}

// Depth 获取快照；limit 必须落在官方允许集合内，由调用方保证。
func (c *Client) Depth(ctx context.Context, market, symbol string, limit int) (DepthSnapshot, error) {
	spec, ok := marketEndpoints(market)
	if !ok {
		return DepthSnapshot{}, fmt.Errorf("不支持的市场: %s", market)
	}
	var snap DepthSnapshot
	err := c.get(ctx, market, config.EndpointREST, spec.depth, url.Values{"symbol": {symbol}, "limit": {strconv.Itoa(limit)}}, depthWeight(market, limit), false, &snap)
	return snap, err
}

// exchangeEntry 是现货与永续共有的目录字段；永续额外带 contractType 与结算资产。
type exchangeEntry struct {
	Symbol, Status, BaseAsset, QuoteAsset, MarginAsset string
	ContractType                                       string
	Filters                                            []struct {
		Type string `json:"filterType"`
		Tick string `json:"tickSize"`
		Step string `json:"stepSize"`
	}
}

// Instruments 返回市场的品种目录；目录缓存按市场隔离并小时级复用，
// 首个上游错误不会写入缓存。永续只收录 crypto 永续（PERPETUAL），
// 传统资产永续（TRADIFI_PERPETUAL）交易时段与加密不同，待会话模型支持后再纳入。
func (c *Client) Instruments(ctx context.Context, market string) ([]model.Instrument, error) {
	spec, ok := marketEndpoints(market)
	if !ok {
		return nil, fmt.Errorf("不支持的市场: %s", market)
	}
	slot, err := c.marketDirectory(market)
	if err != nil {
		return nil, err
	}
	slot.mu.Lock()
	defer slot.mu.Unlock()
	var raw struct {
		Symbols []exchangeEntry
	}
	if time.Since(slot.at) < time.Hour && len(slot.bytes) > 0 {
		if err := json.Unmarshal(slot.bytes, &raw); err != nil {
			return nil, err
		}
	} else {
		query := url.Values{}
		if market == model.MarketSpot {
			query.Set("showPermissionSets", "false")
		}
		if err := c.get(ctx, market, config.EndpointREST, spec.exchangeInfo, query, spec.exchangeCost, false, &raw); err != nil {
			return nil, err
		}
		slot.bytes, _ = json.Marshal(raw)
		slot.at = time.Now()
	}
	result := make([]model.Instrument, 0, len(raw.Symbols))
	for _, s := range raw.Symbols {
		if s.Status != "TRADING" {
			continue
		}
		if market == model.MarketPerp && s.ContractType != "PERPETUAL" {
			continue
		}
		if _, err := model.Symbol(s.Symbol); err != nil {
			continue
		}
		tick, lot := 0.0, 0.0
		for _, f := range s.Filters {
			if f.Type == "PRICE_FILTER" {
				tick, err = model.Number(f.Tick)
				if err != nil {
					return nil, err
				}
			}
			if f.Type == "LOT_SIZE" {
				lot, err = model.Number(f.Step)
				if err != nil {
					return nil, err
				}
			}
		}
		id := model.InstrumentID(market, s.Symbol)
		if id == "" {
			continue
		}
		capabilities := model.Capabilities()
		session := "BINANCE_SPOT"
		if market == model.MarketPerp {
			capabilities = model.PerpCapabilities()
			session = "BINANCE_PERP"
		}
		result = append(result, model.Instrument{
			ID: id, SourceID: model.SourceID, Symbol: s.Symbol, Name: s.BaseAsset + " / " + s.QuoteAsset,
			InstrumentType: "crypto", Exchange: "BINANCE", SessionID: session, Currency: s.QuoteAsset,
			TickSize: tick, LotSize: lot,
			ProviderRef:  map[string]string{"symbol": s.Symbol, "market": market},
			Capabilities: capabilities,
		})
	}
	return result, nil
}

// fundingEndpoints 返回永续专属端点；市场参数是显式的，现货调用直接拒绝而不发网络请求。
func fundingEndpoints(market string) (endpoints, error) {
	spec, ok := marketEndpoints(market)
	if !ok {
		return endpoints{}, fmt.Errorf("不支持的市场: %s", market)
	}
	if market != model.MarketPerp {
		return endpoints{}, fmt.Errorf("资金费率与未平仓量只存在于永续市场")
	}
	return spec, nil
}

// PremiumIndex 读取永续标记价格与资金费率快照；现货没有等价端点，返回错误而不是伪造数值。
func (c *Client) PremiumIndex(ctx context.Context, market, symbol string) (model.Funding, error) {
	spec, err := fundingEndpoints(market)
	if err != nil {
		return model.Funding{}, err
	}
	var raw struct {
		MarkPrice     string `json:"markPrice"`
		LastFunding   string `json:"lastFundingRate"`
		NextFundingAt int64  `json:"nextFundingTime"`
		Time          int64  `json:"time"`
	}
	if err := c.get(ctx, market, config.EndpointREST, spec.premiumIndex, url.Values{"symbol": {symbol}}, 1, false, &raw); err != nil {
		return model.Funding{}, err
	}
	return model.Funding{Rate: raw.LastFunding, MarkPrice: raw.MarkPrice, NextFundingAt: raw.NextFundingAt, Timestamp: raw.Time}, nil
}

// OpenInterest 读取永续未平仓合约量快照，单位为基础资产。
func (c *Client) OpenInterest(ctx context.Context, market, symbol string) (model.OpenInterest, error) {
	spec, err := fundingEndpoints(market)
	if err != nil {
		return model.OpenInterest{}, err
	}
	var raw struct {
		OpenInterest string `json:"openInterest"`
		Time         int64  `json:"time"`
	}
	if err := c.get(ctx, market, config.EndpointREST, spec.openInterest, url.Values{"symbol": {symbol}}, 1, false, &raw); err != nil {
		return model.OpenInterest{}, err
	}
	return model.OpenInterest{Size: raw.OpenInterest, Timestamp: raw.Time}, nil
}

// FundingRate 读取永续资金费率历史，用于资金成本口径。
func (c *Client) FundingRate(ctx context.Context, market, symbol string, limit int) ([]model.Funding, error) {
	spec, err := fundingEndpoints(market)
	if err != nil {
		return nil, err
	}
	var raw []struct {
		FundingRate string `json:"fundingRate"`
		FundingTime int64  `json:"fundingTime"`
		MarkPrice   string `json:"markPrice"`
	}
	query := url.Values{"symbol": {symbol}, "limit": {strconv.Itoa(limit)}}
	if err := c.get(ctx, market, config.EndpointREST, spec.fundingRate, query, 1, false, &raw); err != nil {
		return nil, err
	}
	out := make([]model.Funding, 0, len(raw))
	for _, item := range raw {
		out = append(out, model.Funding{Rate: item.FundingRate, MarkPrice: item.MarkPrice, Timestamp: item.FundingTime})
	}
	return out, nil
}

// Ping 验证该市场 REST 可达性，不伪装成完整 WebSocket 健康状态。
func (c *Client) Ping(ctx context.Context, market string) error {
	spec, ok := marketEndpoints(market)
	if !ok {
		return fmt.Errorf("不支持的市场: %s", market)
	}
	var out map[string]any
	return c.get(ctx, market, config.EndpointREST, spec.ping, nil, 1, false, &out)
}
