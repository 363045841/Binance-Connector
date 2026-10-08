// 本文件验证 SSE 游标回放、共享订阅、背压断开及资源回收。
package stream

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"binanceconnector/internal/config"
	"binanceconnector/internal/model"
)

// hubFixture 使用可控 runner 观察生命周期，避免依赖网络或固定 sleep。
func hubFixture(t *testing.T, buffer, replay, max int) (*Hub, chan *Topic, chan struct{}) {
	t.Helper()
	s, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	s.BufferSize = buffer
	s.ReplaySize = replay
	s.MaxStreams = max
	s.IdleGrace = 5 * time.Millisecond
	started := make(chan *Topic, 10)
	stopped := make(chan struct{}, 10)
	h := NewHub(s, func(ctx context.Context, key Key, topic *Topic) {
		started <- topic
		<-ctx.Done()
		stopped <- struct{}{}
	})
	t.Cleanup(h.Close)
	return h, started, stopped
}

// receive 用超时保护真实 channel 行为，不引入时序猜测。
func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(time.Second):
		t.Fatal("等待事件超时")
		var zero T
		return zero
	}
}

// TestReplayAndSharedSubscription 验证同品种共用上游，Last-Event-ID 排他回放。
func TestReplayAndSharedSubscription(t *testing.T) {
	h, started, _ := hubFixture(t, 16, 4, 1)
	key := Key{Kind: "trades", Symbol: "BTCUSDT"}
	first, err := h.Subscribe(key, "")
	if err != nil {
		t.Fatal(err)
	}
	topic := receive(t, started)
	topic.Publish(model.NewFrame("trades", key.Symbol), nil)
	entry := receive(t, first.Events)
	topic.Publish(model.NewFrame("status", key.Symbol), nil)
	secondEntry := receive(t, first.Events)
	second, err := h.Subscribe(key, entry.ID)
	if err != nil {
		t.Fatal(err)
	}
	if replay := second.Replay(); len(replay) != 1 || replay[0].ID != secondEntry.ID {
		t.Fatalf("回放边界错误: %+v", replay)
	}
	select {
	case <-started:
		t.Fatal("同 key 不应启动第二个上游")
	default:
	}
	if _, err := h.Subscribe(Key{Kind: "depth", Symbol: key.Symbol}, ""); err == nil {
		t.Fatal("不同能力应占用独立订阅预算")
	}
	first.Close()
	second.Close()
}

// TestReplayGapAndSnapshot 验证过期游标明确报告 gap，盘口以当前快照恢复。
func TestReplayGapAndSnapshot(t *testing.T) {
	h, started, _ := hubFixture(t, 16, 1, 1)
	key := Key{Kind: "depth", Symbol: "BTCUSDT"}
	sub, err := h.Subscribe(key, "")
	if err != nil {
		t.Fatal(err)
	}
	topic := receive(t, started)
	topic.Publish(model.NewFrame("status", key.Symbol), nil)
	old := receive(t, sub.Events)
	for i := 0; i < 3; i++ {
		topic.Publish(model.NewFrame("depthDelta", key.Symbol), nil)
	}
	topic.SetSnapshot(func() model.Frame { f := model.NewFrame("depthSnapshot", key.Symbol); f.LastUpdateID = "162"; return f })
	recovered, err := h.Subscribe(key, old.ID)
	if err != nil {
		t.Fatal(err)
	}
	replay := recovered.Replay()
	if len(replay) != 2 {
		t.Fatalf("应提供 gap + snapshot: %+v", replay)
	}
	var gap model.Frame
	if err := json.Unmarshal(replay[0].Data, &gap); err != nil {
		t.Fatal(err)
	}
	if gap.Code != "REPLAY_GAP" || gap.Complete == nil || *gap.Complete {
		t.Fatalf("不应声称完整恢复: %+v", gap)
	}
	sub.Close()
	recovered.Close()
}

// TestSlowConsumerAndIdleRelease 验证满队列断开且上游最终取消，不静默漏帧。
func TestSlowConsumerAndIdleRelease(t *testing.T) {
	h, started, stopped := hubFixture(t, 1, 8, 1)
	key := Key{Kind: "trades", Symbol: "BTCUSDT"}
	sub, err := h.Subscribe(key, "")
	if err != nil {
		t.Fatal(err)
	}
	topic := receive(t, started)
	topic.Publish(model.NewFrame("trades", key.Symbol), nil)
	topic.Publish(model.NewFrame("trades", key.Symbol), nil)
	_ = receive(t, sub.Events)
	if _, ok := <-sub.Events; ok {
		t.Fatal("慢消费者应被断开")
	}
	sub.Close()
	receive(t, stopped)
	next, err := h.Subscribe(Key{Kind: "depth", Symbol: key.Symbol}, "")
	if err != nil {
		t.Fatal("空闲资源未回收:", err)
	}
	receive(t, started)
	next.Close()
}

// TestConcurrentPublishAndSubscribe 验证并发注册/注销与广播无数据竞争。
func TestConcurrentPublishAndSubscribe(t *testing.T) {
	h, started, _ := hubFixture(t, 512, 32, 1)
	key := Key{Kind: "trades", Symbol: "BTCUSDT"}
	anchor, err := h.Subscribe(key, "")
	if err != nil {
		t.Fatal(err)
	}
	topic := receive(t, started)
	var workers sync.WaitGroup
	for i := 0; i < 10; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for j := 0; j < 20; j++ {
				s, err := h.Subscribe(key, "")
				if err == nil {
					s.Close()
				}
				topic.Publish(model.NewFrame("status", key.Symbol), nil)
			}
		}()
	}
	workers.Wait()
	anchor.Close()
}
