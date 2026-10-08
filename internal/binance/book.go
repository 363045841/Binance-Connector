// 本文件实现 Binance Diff Depth 的快照基线、连续序号校验和原子盘口更新。
package binance

import (
	"fmt"
	"sort"
	"strconv"
	"sync"

	"binanceconnector/internal/model"
)

// DepthEvent 为官方 U/u 绝对数量增量，不能当作成交量或数量差值。
type DepthEvent struct {
	EventType string        `json:"e"`
	EventTime int64         `json:"E"`
	Symbol    string        `json:"s"`
	FirstID   int64         `json:"U"`
	FinalID   int64         `json:"u"`
	Bids      []model.Level `json:"b"`
	Asks      []model.Level `json:"a"`
}

// Book 保留 REST 已知档位及后续更新；首次增量必须覆盖 lastUpdateId+1。
type Book struct {
	mu         sync.RWMutex
	Symbol     string
	lastID     int64
	bids, asks map[string]string
	timestamp  int64
	maxLevels  int
}

// NewBook 从有效 REST 基线创建本地订单簿。
func NewBook(symbol string, snapshot DepthSnapshot, maxLevels int) (*Book, error) {
	if snapshot.LastID < 0 {
		return nil, fmt.Errorf("无效快照序号")
	}
	b := &Book{Symbol: symbol, lastID: snapshot.LastID, bids: make(map[string]string), asks: make(map[string]string), maxLevels: maxLevels}
	if err := validateLevels(snapshot.Bids, snapshot.Asks); err != nil {
		return nil, err
	}
	applyLevels(b.bids, snapshot.Bids)
	applyLevels(b.asks, snapshot.Asks)
	if len(b.bids)+len(b.asks) > maxLevels {
		return nil, fmt.Errorf("盘口基线超过已知档位预算")
	}
	return b, nil
}

// Apply 丢弃重复包，拒绝断序，并在完整校验后写入所有档位。
func (b *Book) Apply(event DepthEvent) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if event.Symbol != b.Symbol || event.FirstID > event.FinalID {
		return false, fmt.Errorf("盘口事件身份或序号无效")
	}
	if event.FinalID <= b.lastID {
		return false, nil
	}
	if event.FirstID > b.lastID+1 {
		return false, fmt.Errorf("盘口断序: expected %d, got %d", b.lastID+1, event.FirstID)
	}
	if err := validateLevels(event.Bids, event.Asks); err != nil {
		return false, err
	}
	if projectedCount(b.bids, event.Bids)+projectedCount(b.asks, event.Asks) > b.maxLevels {
		return false, fmt.Errorf("已知盘口档位超过预算，必须重新获取基线")
	}
	applyLevels(b.bids, event.Bids)
	applyLevels(b.asks, event.Asks)
	b.lastID = event.FinalID
	b.timestamp = event.EventTime
	return true, nil
}

// projectedCount 计算增量后的档位数，重复价格和删除不误计资源预算。
func projectedCount(current map[string]string, levels []model.Level) int {
	count := len(current)
	states := make(map[string]bool, len(levels))
	for _, level := range levels {
		before, seen := states[level[0]]
		if !seen {
			_, before = current[level[0]]
		}
		quantity, _ := model.Number(level[1])
		after := quantity > 0
		if before != after {
			if after {
				count++
			} else {
				count--
			}
		}
		states[level[0]] = after
	}
	return count
}

// validateLevels 校验价格及绝对数量，零数量是合法删除指令。
func validateLevels(groups ...[]model.Level) error {
	for _, levels := range groups {
		for _, level := range levels {
			p, err := model.Number(level[0])
			if err != nil || p <= 0 {
				return fmt.Errorf("无效盘口价格")
			}
			if _, err := model.Number(level[1]); err != nil {
				return err
			}
		}
	}
	return nil
}

// applyLevels 只由持锁调用方使用，保留价格和数量原始精度。
func applyLevels(target map[string]string, levels []model.Level) {
	for _, level := range levels {
		q, _ := model.Number(level[1])
		if q == 0 {
			delete(target, level[0])
		} else {
			target[level[0]] = level[1]
		}
	}
}

// Snapshot 按需构建全部已知档位的有序快照；初始范围受 REST limit 限制。
func (b *Book) Snapshot() model.Frame {
	b.mu.RLock()
	defer b.mu.RUnlock()
	f := model.NewFrame("depthSnapshot", b.Symbol)
	f.LastUpdateID = strconv.FormatInt(b.lastID, 10)
	f.Bids = sortedLevels(b.bids, false)
	f.Asks = sortedLevels(b.asks, true)
	f.Timestamp = b.timestamp
	return f
}

// sortedLevels 只在快照读取时排序，不在每个实时增量内复制整本盘口。
func sortedLevels(levels map[string]string, ascending bool) []model.Level {
	type item struct {
		level model.Level
		price float64
	}
	items := make([]item, 0, len(levels))
	for price, size := range levels {
		p, _ := model.Number(price)
		items = append(items, item{model.Level{price, size}, p})
	}
	sort.Slice(items, func(i, j int) bool {
		if ascending {
			return items[i].price < items[j].price
		}
		return items[i].price > items[j].price
	})
	result := make([]model.Level, 0, len(items))
	for _, entry := range items {
		result = append(result, entry.level)
	}
	return result
}
