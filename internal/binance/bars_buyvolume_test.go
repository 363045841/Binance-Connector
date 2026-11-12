// 本文件验证 K 线主动买量契约：主动买入基础量必须从 Binance 第 10 列带出，
// 否则图表侧无法计算精确 CVD，只能退回收盘位置估算。
package binance

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"binanceconnector/internal/model"
)

// TestBarsCarryTakerBuyVolume 确认 bars 响应带出主动买量，并保留原始精度。
func TestBarsCarryTakerBuyVolume(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v3/klines" {
			t.Errorf("意外路径: %s", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(`[[1499040000000,"0.01634790","0.80000000","0.01575800","0.01577100","148976.11427815",1499644799999,"2434.19055334",308,"1756.87402397","28.46694368","0"]]`))
	}))
	defer up.Close()

	s := withREST(settingsForTest(t), model.MarketSpot, up.URL)
	c := New(s)
	defer c.Close()

	bars, err := c.Bars(context.Background(), model.MarketSpot, "LTCBTC", "1min", 1, nil)
	if err != nil {
		t.Fatalf("取 K 线失败: %v", err)
	}
	if len(bars) != 1 {
		t.Fatalf("K 线数量 = %d, want 1", len(bars))
	}
	if bars[0].BuyVolume == nil {
		t.Fatal("主动买量缺失：数据源提供了该列，响应里不能为空")
	}
	if got := *bars[0].BuyVolume; got != 1756.87402397 {
		t.Fatalf("主动买量 = %v, want 1756.87402397", got)
	}
}
