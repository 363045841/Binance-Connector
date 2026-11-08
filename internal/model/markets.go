// 本文件集中定义市场维度与品种 ID 的唯一定义处，避免各层重复拼接前缀。
package model

import "strings"

// V1 支持的上游市场：同源不同合约的产品靠市场维度区分，ID 前缀即市场。
const (
	MarketSpot = "spot"
	MarketPerp = "perp"
)

// marketPrefix 是市场与 V1 品种 ID 前缀的唯一定义处；解析按声明顺序匹配。
var marketPrefix = []struct {
	market string
	prefix string
}{
	{MarketSpot, SourceID + ":" + MarketSpot + ":"},
	{MarketPerp, SourceID + ":" + MarketPerp + ":"},
}

// Markets 按稳定顺序返回全部市场标识，供目录与健康检查枚举。
func Markets() []string { return []string{MarketSpot, MarketPerp} }

// ValidMarket 判断市场标识是否受支持。
func ValidMarket(market string) bool {
	for _, item := range marketPrefix {
		if item.market == market {
			return true
		}
	}
	return false
}

// InstrumentID 把市场与交易对转换为 V1 稳定品种 ID；未知市场返回空串。
func InstrumentID(market, symbol string) string {
	for _, item := range marketPrefix {
		if item.market == market {
			return item.prefix + symbol
		}
	}
	return ""
}

// SymbolFromInstrumentID 解析 V1 品种 ID 的市场与交易对；前缀或交易对非法时返回 false。
func SymbolFromInstrumentID(value string) (market, symbol string, ok bool) {
	for _, item := range marketPrefix {
		if rest, found := strings.CutPrefix(value, item.prefix); found {
			symbol, err := Symbol(rest)
			if err != nil {
				return "", "", false
			}
			return item.market, symbol, true
		}
	}
	return "", "", false
}
