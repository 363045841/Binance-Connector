// Package model 定义对外 V1 行情、成交和订单簿契约。
package model

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

const SourceID = "binance"

// Periods 映射图表标准周期到 Binance 原生 UTC 周期。
var Periods = map[string]string{"1min": "1m", "5min": "5m", "15min": "15m", "30min": "30m", "60min": "1h", "4h": "4h", "daily": "1d", "weekly": "1w", "monthly": "1M"}
var PeriodNames = []string{"1min", "5min", "15min", "30min", "60min", "4h", "daily", "weekly", "monthly"}

// Bar 为图表 V1 数值 K 线，volume 使用基础资产数量。
type Bar struct {
	Timestamp int64   `json:"timestamp"`
	Open      float64 `json:"open"`
	High      float64 `json:"high"`
	Low       float64 `json:"low"`
	Close     float64 `json:"close"`
	Volume    float64 `json:"volume"`
	Turnover  float64 `json:"turnover"`
}

// Trade 保留十进制字符串和字符串 ID，避免前端精度丢失；Side 是主动方向。
type Trade struct {
	TradeID         string `json:"tradeId"`
	Timestamp       int64  `json:"timestamp"`
	Price           string `json:"price"`
	TimestampMicros string `json:"timestampMicros,omitempty"`
	Size            string `json:"size"`
	QuoteSize       string `json:"quoteSize,omitempty"`
	Side            string `json:"side"`
	SideSource      string `json:"sideSource"`
	IsBuyerMaker    bool   `json:"isBuyerMaker"`
	Kind            string `json:"kind"`
	FirstTradeID    string `json:"firstTradeId,omitempty"`
	LastTradeID     string `json:"lastTradeId,omitempty"`
}

// Level 是价格与绝对挂单数量，数量为零表示删除该价位。
type Level [2]string

// Frame 为 SSE 帧；盘口序号与 SSE 回放 ID 属于不同身份空间。
type Frame struct {
	Type           string  `json:"type"`
	SourceID       string  `json:"sourceId"`
	Market         string  `json:"market"`
	Symbol         string  `json:"symbol,omitempty"`
	InstrumentID   string  `json:"instrumentId,omitempty"`
	Timestamp      int64   `json:"timestamp,omitempty"`
	Trades         []Trade `json:"trades,omitempty"`
	Bar            *Bar    `json:"bar,omitempty"`
	Bars           []Bar   `json:"bars"`
	Period         string  `json:"period,omitempty"`
	BarAggregation string  `json:"barAggregation,omitempty"`
	FirstUpdateID  string  `json:"firstUpdateId,omitempty"`
	LastUpdateID   string  `json:"lastUpdateId,omitempty"`
	Bids           []Level `json:"bids"`
	Asks           []Level `json:"asks"`
	Complete       *bool   `json:"complete,omitempty"`
	Code           string  `json:"code,omitempty"`
	Message        string  `json:"message,omitempty"`
	FromID         string  `json:"fromId,omitempty"`
	ToID           string  `json:"toId,omitempty"`
	Funding        *Funding `json:"funding,omitempty"`
	OpenInterest   *OpenInterest `json:"openInterest,omitempty"`
}

// Funding 是永续资金费率快照；资金费率只属永续市场，现货帧不填充。
type Funding struct {
	Rate           string `json:"rate"`
	MarkPrice      string `json:"markPrice"`
	NextFundingAt  int64  `json:"nextFundingAt,omitempty"`
	Timestamp      int64  `json:"timestamp,omitempty"`
}

// OpenInterest 是永续未平仓合约量快照，单位为基础资产。
type OpenInterest struct {
	Size      string `json:"size"`
	Timestamp int64  `json:"timestamp,omitempty"`
}

// NewFrame 填充所有流共用的来源身份；instrumentId 是协议里的稳定身份。
func NewFrame(kind, market, symbol string) Frame {
	return Frame{Type: kind, SourceID: SourceID, Market: market, Symbol: symbol, InstrumentID: InstrumentID(market, symbol)}
}

// LiveBarsFrame 构造实时 K 线帧：身份是 instrumentId，周期属于连接参数。
func LiveBarsFrame(kind, market, symbol string) Frame {
	frame := NewFrame(kind, market, symbol)
	frame.Symbol = ""
	return frame
}

// Instrument 为 V1 品种目录的稳定标识及最小价格/数量步长。
type Instrument struct {
	ID             string            `json:"id"`
	SourceID       string            `json:"sourceId"`
	Symbol         string            `json:"symbol"`
	Name           string            `json:"name"`
	InstrumentType string            `json:"instrumentType"`
	Exchange       string            `json:"exchange"`
	SessionID      string            `json:"sessionId"`
	Currency       string            `json:"currency"`
	TickSize       float64           `json:"tickSize"`
	LotSize        float64           `json:"lotSize"`
	ProviderRef    map[string]string `json:"providerRef"`
	Capabilities   map[string]any    `json:"capabilities"`
}

// Capabilities 明确区分报价与真实成交，历史归档与实时分页均公开声明。
func Capabilities() map[string]any {
	return map[string]any{
		"instrumentTypes": []string{"crypto"},
		"bars":            map[string]any{"periods": PeriodNames, "adjustments": []string{"none"}, "barAggregations": []string{"original"}},
		"liveBars":        true, "depth": true,
		"trades": map[string]any{"recent": true, "historyById": true, "archive": true, "live": true, "raw": true, "aggregated": true, "aggressorSide": "exchange", "volumeUnit": "baseAsset"},
	}
}

// PerpCapabilities 在现货能力之上补充永续独有的资金费率与未平仓量。
func PerpCapabilities() map[string]any {
	out := Capabilities()
	out["funding"] = true
	out["openInterest"] = true
	return out
}

// Symbol 规范化交易对，并禁止其改变上游路径和订阅语义。
func Symbol(value string) (string, error) {
	s := strings.ToUpper(strings.TrimSpace(value))
	if len(s) < 2 || len(s) > 40 {
		return "", fmt.Errorf("symbol 长度必须为 2–40")
	}
	for _, c := range s {
		if !(c >= 'A' && c <= 'Z') && !(c >= '0' && c <= '9') {
			return "", fmt.Errorf("symbol 只支持 ASCII 字母和数字")
		}
	}
	return s, nil
}

// Number 验证上游十进制数并转换图表需要的有限浮点数。
func Number(value string) (float64, error) {
	v, err := strconv.ParseFloat(value, 64)
	if err != nil || math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
		return 0, fmt.Errorf("无效非负数 %q", value)
	}
	return v, nil
}

// NewTrade 显式映射 buyer maker 到主动卖，保留原始交易标记。
func NewTrade(id, timestamp int64, price, size, quote string, buyerMaker bool, kind string) (Trade, error) {
	p, err := Number(price)
	if err != nil || p <= 0 {
		return Trade{}, fmt.Errorf("无效成交价格")
	}
	q, err := Number(size)
	if err != nil || q <= 0 || id < 0 || timestamp <= 0 {
		return Trade{}, fmt.Errorf("无效成交数量或身份")
	}
	side := "buy"
	if buyerMaker {
		side = "sell"
	}
	return Trade{TradeID: strconv.FormatInt(id, 10), Timestamp: timestamp, Price: price, Size: size, QuoteSize: quote, Side: side, SideSource: "exchange", IsBuyerMaker: buyerMaker, Kind: kind}, nil
}
