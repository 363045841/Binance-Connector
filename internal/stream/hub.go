// Package stream 共用上游订阅，提供有界 SSE 回放、背压断开和空闲资源回收。
package stream

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"binanceconnector/internal/config"
	"binanceconnector/internal/model"
)

// Key 隔离不同成交模式、盘口及 K 线周期。
type Key struct{ Kind, Symbol, Period string }

// Entry 为 Connector 进程内有序帧，ID 在上游重连时继续递增。
type Entry struct {
	ID   string
	Seq  uint64
	Data []byte
}

// Runner 管理一次共享订阅任务，生命周期由 Hub 的 context 决定。
type Runner func(context.Context, Key, *Topic)

// Topic 持有独立互斥锁；Publish 与新订阅的快照边界原子化。
type Topic struct {
	mu        sync.Mutex
	epoch     string
	sequence  uint64
	ring      []Entry
	limit     int
	subs      map[*Subscription]struct{}
	bootstrap []byte
	snapshot  func() model.Frame
	cancel    context.CancelFunc
	idle      *time.Timer
}

// Subscription 的 channel 满时关闭，强制客户端回放，禁止静默丢成交。
type Subscription struct {
	Events  chan Entry
	replay  []Entry
	once    sync.Once
	release func()
}

// Replay 返回注册时原子获取的历史帧或 bootstrap。
func (s *Subscription) Replay() []Entry { return s.replay }

// Close 释放订阅；重复调用无操作。
func (s *Subscription) Close() { s.once.Do(s.release) }

// Hub 管理所有 topic；上游 goroutine 和空闲计时器在关闭时一并取消。
type Hub struct {
	mu       sync.Mutex
	topics   map[Key]*Topic
	ctx      context.Context
	cancel   context.CancelFunc
	settings config.Settings
	runner   Runner
	closed   bool
	wg       sync.WaitGroup
}

// NewHub 创建共享订阅管理器，不启动任何上游连接。
func NewHub(s config.Settings, runner Runner) *Hub {
	ctx, cancel := context.WithCancel(context.Background())
	return &Hub{topics: make(map[Key]*Topic), ctx: ctx, cancel: cancel, settings: s, runner: runner}
}

// Subscribe 在回放窗口内恢复，否则明确返回 REPLAY_GAP；新客户端获得当前 bootstrap。
func (h *Hub) Subscribe(key Key, lastID string) (*Subscription, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil, fmt.Errorf("服务正在关闭")
	}
	t, ok := h.topics[key]
	if !ok {
		if len(h.topics) >= h.settings.MaxStreams {
			return nil, fmt.Errorf("共享上游订阅达到上限")
		}
		bytes := make([]byte, 12)
		if _, err := rand.Read(bytes); err != nil {
			return nil, err
		}
		ctx, cancel := context.WithCancel(h.ctx)
		t = &Topic{epoch: hex.EncodeToString(bytes), limit: h.settings.ReplaySize, subs: make(map[*Subscription]struct{}), cancel: cancel}
		h.topics[key] = t
		h.wg.Add(1)
		go func() { defer h.wg.Done(); h.runner(ctx, key, t) }()
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.idle != nil {
		t.idle.Stop()
		t.idle = nil
	}
	s := &Subscription{Events: make(chan Entry, h.settings.BufferSize)}
	s.release = func() { h.unsubscribe(key, t, s) }
	bootstrap := t.bootstrap
	if t.snapshot != nil {
		bootstrap, _ = json.Marshal(t.snapshot())
	}
	if lastID != "" {
		parts := strings.Split(lastID, ":")
		seq, err := uint64(0), error(nil)
		if len(parts) == 2 {
			seq, err = strconv.ParseUint(parts[1], 10, 64)
		} else {
			err = fmt.Errorf("无效 SSE ID")
		}
		valid := err == nil && parts[0] == t.epoch && seq <= t.sequence
		if len(t.ring) > 0 && seq < t.ring[0].Seq-1 {
			valid = false
		}
		if valid {
			for _, entry := range t.ring {
				if entry.Seq > seq {
					s.replay = append(s.replay, entry)
				}
			}
		} else {
			f := model.NewFrame("status", key.Symbol)
			f.Code = "REPLAY_GAP"
			f.Message = "回放窗口不可用，请按 tradeId 回补或重新获取盘口快照"
			complete := false
			f.Complete = &complete
			data, _ := json.Marshal(f)
			s.replay = append(s.replay, Entry{Data: data})
			if len(bootstrap) > 0 {
				s.replay = append(s.replay, Entry{ID: t.id(), Seq: t.sequence, Data: bootstrap})
			}
		}
	} else if len(bootstrap) > 0 {
		s.replay = append(s.replay, Entry{ID: t.id(), Seq: t.sequence, Data: bootstrap})
	}
	t.subs[s] = struct{}{}
	return s, nil
}

// id 生成当前 topic 的唯一 Connector 帧游标；调用方持有锁。
func (t *Topic) id() string { return t.epoch + ":" + strconv.FormatUint(t.sequence, 10) }

// Publish 分发已验证的帧，并更新可供新客户端读取的同步快照。
func (t *Topic) Publish(frame model.Frame, bootstrap *model.Frame) {
	data, err := json.Marshal(frame)
	if err != nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if bootstrap != nil {
		t.bootstrap, _ = json.Marshal(bootstrap)
	}
	t.sequence++
	entry := Entry{ID: t.id(), Seq: t.sequence, Data: data}
	if len(t.ring) == t.limit {
		copy(t.ring, t.ring[1:])
		t.ring = t.ring[:len(t.ring)-1]
	}
	t.ring = append(t.ring, entry)
	for s := range t.subs {
		select {
		case s.Events <- entry:
		default:
			delete(t.subs, s)
			close(s.Events)
		}
	}
}

// ClearBootstrap 在上游断开或重同步时撤销已失效的盘口/尾部快照。
func (t *Topic) ClearBootstrap() { t.mu.Lock(); t.bootstrap = nil; t.snapshot = nil; t.mu.Unlock() }

// SetSnapshot 安装按需快照，避免每个盘口增量都排序及复制整本订单簿。
func (t *Topic) SetSnapshot(snapshot func() model.Frame) {
	t.mu.Lock()
	t.snapshot = snapshot
	t.mu.Unlock()
}

// unsubscribe 最后一个消费者离开后延迟回收 topic，上游不常驻空闲品种。
func (h *Hub) unsubscribe(key Key, t *Topic, s *Subscription) {
	h.mu.Lock()
	defer h.mu.Unlock()
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.subs[s]; ok {
		delete(t.subs, s)
		close(s.Events)
	}
	if len(t.subs) > 0 || t.idle != nil || h.closed {
		return
	}
	t.idle = time.AfterFunc(h.settings.IdleGrace, func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		t.mu.Lock()
		defer t.mu.Unlock()
		if len(t.subs) > 0 || h.topics[key] != t {
			return
		}
		t.cancel()
		delete(h.topics, key)
	})
}

// Close 停止所有上游和订阅，让阻塞中的 SSE 请求及时结束。
func (h *Hub) Close() {
	h.mu.Lock()
	h.closed = true
	h.cancel()
	for _, t := range h.topics {
		t.mu.Lock()
		if t.idle != nil {
			t.idle.Stop()
		}
		for s := range t.subs {
			close(s.Events)
			delete(t.subs, s)
		}
		t.mu.Unlock()
	}
	h.mu.Unlock()
	h.wg.Wait()
}
