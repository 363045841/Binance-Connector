// 本文件按左闭右开时间范围返回原始成交；聚合成交仅用于定位原始 ID，不参与成交量统计。
package binance

import (
	"binanceconnector/internal/model"
	"context"
	"fmt"
	"net/url"
	"strconv"
	"time"
)

const maxRangeTrades = 250000
const maxRangePages = 250

// TradeRange 读取结束日期的校验归档及当日原始成交分页，不把部分覆盖当成完整范围。
func (c *Client) TradeRange(ctx context.Context, symbol string, from, to int64) ([]model.Trade, error) {
	result := make([]model.Trade, 0)
	appendTrade := func(t model.Trade) error {
		if t.Timestamp < from || t.Timestamp >= to {
			return nil
		}
		if len(result) >= maxRangeTrades {
			return &Error{Status: 422, Code: "RESOURCE_EXHAUSTED", Message: fmt.Sprintf("成交范围超过 %d 笔，请缩小图表范围", maxRangeTrades)}
		}
		result = append(result, t)
		return nil
	}
	today := time.Now().UTC().Truncate(24 * time.Hour)
	for day := time.UnixMilli(from).UTC().Truncate(24 * time.Hour); day.UnixMilli() < to; day = day.Add(24 * time.Hour) {
		if day.Before(today) {
			date := day.Format("2006-01-02")
			path, err := c.Archive(ctx, symbol, date)
			if err != nil {
				return nil, err
			}
			if err := ReadArchive(ctx, path, date, appendTrade); err != nil {
				return nil, err
			}
			continue
		}
		start := from
		if start < day.UnixMilli() {
			start = day.UnixMilli()
		}
		// Binance aggTrades 返回的首个原始成交 ID 提供时间到游标的确定定位。
		var seed []model.Trade
		for window := start; window < to && len(seed) == 0; {
			end := window + time.Hour.Milliseconds()
			if end > to {
				end = to
			}
			var err error
			seed, err = c.AggTrades(ctx, symbol, url.Values{"startTime": {strconv.FormatInt(window, 10)}, "endTime": {strconv.FormatInt(end-1, 10)}, "limit": {"1"}})
			if err != nil {
				return nil, err
			}
			window = end
		}
		if len(seed) == 0 {
			continue
		}
		next, err := strconv.ParseInt(seed[0].FirstTradeID, 10, 64)
		if err != nil {
			return nil, err
		}
		done := false
		for page := 0; page < maxRangePages; page++ {
			trades, err := c.Trades(ctx, symbol, &next, 1000)
			if err != nil {
				return nil, err
			}
			if len(trades) == 0 {
				return nil, fmt.Errorf("原始成交分页未覆盖范围尾部")
			}
			for _, t := range trades {
				id, err := strconv.ParseInt(t.TradeID, 10, 64)
				if err != nil {
					return nil, err
				}
				if id != next {
					return nil, fmt.Errorf("原始成交断序: expected %d got %d", next, id)
				}
				next++
				if t.Timestamp >= to {
					done = true
					break
				}
				if err := appendTrade(t); err != nil {
					return nil, err
				}
			}
			if done || len(trades) < 1000 {
				done = true
				break
			}
		}
		if !done {
			return nil, &Error{Status: 422, Code: "RESOURCE_EXHAUSTED", Message: "成交时间范围超过分页预算，请缩小图表范围"}
		}
	}
	return result, nil
}
