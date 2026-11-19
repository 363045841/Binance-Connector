// 本文件是「交易所合约 → V1 会话与品种类别」的唯一映射处。
// 目录、能力清单与搜索都从这里取判断，避免各层各写一套 contractType 判等。
package model

// exchangeInfo 的 contractType 取值；只声明目录会用到的类别，其余（季度合约等）不进目录。
const (
	ContractPerpetual       = "PERPETUAL"
	ContractTradFiPerpetual = "TRADIFI_PERPETUAL"
)

// V1 品种类别，取值与前端 InstrumentType 一致。
const (
	InstrumentTypeCrypto = "crypto"
	InstrumentTypeStock  = "stock"
	InstrumentTypeFuture = "future"
	InstrumentTypeForex  = "forex"
)

// 会话标识；前端按 sessionId 取时区与交易时段。
const (
	SessionSpot = "BINANCE_SPOT"
	SessionPerp = "BINANCE_PERP"
)

// tradfiInstrumentTypes 把 TradFi 永续的 underlyingType 映射到品种类别。
// 只登记 Binance 永续目录实际出现的标的类型；未登记的取值不猜类别，直接不进目录。
var tradfiInstrumentTypes = map[string]string{
	"EQUITY":    InstrumentTypeStock,
	"HK_EQUITY": InstrumentTypeStock,
	"KR_EQUITY": InstrumentTypeStock,
	"CN_EQUITY": InstrumentTypeStock,
	"PREMARKET": InstrumentTypeStock,
	"COMMODITY": InstrumentTypeFuture,
	"FX":        InstrumentTypeForex,
}

// InstrumentClass 返回合约在目录中的会话与品种类别。
// TradFi 永续与加密永续共用 BINANCE_PERP：实测两者都是 24×7 连续成交（含周末，
// 美股与商品品类在各时区的成交量和笔数都不为 0），没有需要单独建模的交易时段。
func InstrumentClass(contractType, underlyingType string) (sessionID, instrumentType string, ok bool) {
	switch contractType {
	case ContractPerpetual:
		return SessionPerp, InstrumentTypeCrypto, true
	case ContractTradFiPerpetual:
		if value, found := tradfiInstrumentTypes[underlyingType]; found {
			return SessionPerp, value, true
		}
	}
	return "", "", false
}

// InstrumentTypes 返回本源目录覆盖的全部品种类别，供能力清单声明。
// 顺序稳定，便于前端按同一份清单做路由与展示。
func InstrumentTypes() []string {
	return []string{InstrumentTypeCrypto, InstrumentTypeStock, InstrumentTypeFuture, InstrumentTypeForex}
}
