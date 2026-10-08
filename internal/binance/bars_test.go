// 本文件验证官方完整 Kline 报文中的大小写字段不会覆盖时间、价格和成交量。
package binance

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"binanceconnector/internal/model"
	"binanceconnector/internal/stream"
)

// TestCompleteKlinePayload 验证 t/T、l/L、v/V、q/Q 是不同业务字段。
func TestCompleteKlinePayload(t *testing.T) {
	s := settingsForTest(t)
	ready := make(chan *stream.Topic, 1)
	hub := stream.NewHub(s, func(ctx context.Context, key stream.Key, topic *stream.Topic) { ready <- topic; <-ctx.Done() })
	defer hub.Close()
	key := stream.Key{Kind: "bars", Symbol: "BNBBTC", Period: "1min"}
	sub, err := hub.Subscribe(key, "")
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	var topic *stream.Topic
	select {
	case topic = <-ready:
	case <-time.After(time.Second):
		t.Fatal("runner 未启动")
	}
	data := []byte(`{"e":"kline","E":1672515782136,"s":"BNBBTC","k":{"t":1672515780000,"T":1672515839999,"s":"BNBBTC","i":"1m","f":100,"L":200,"o":"0.0010","c":"0.0020","h":"0.0025","l":"0.0015","v":"1000","n":100,"x":false,"q":"1.0000","V":"500","Q":"0.500","B":"123456"}}`)
	if err := publishBar(data, key, topic); err != nil {
		t.Fatal(err)
	}
	var frame model.Frame
	select {
	case entry := <-sub.Events:
		if err := json.Unmarshal(entry.Data, &frame); err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Kline 未发布")
	}
	if frame.Type != "forming" || frame.Bar == nil || frame.Bar.Timestamp != 1672515780000 || frame.Bar.Low != 0.0015 || frame.Bar.Volume != 1000 || frame.Bar.Turnover != 1 {
		t.Fatalf("字段被同名大小写覆盖: %+v", frame.Bar)
	}
}
