// 本文件验证真实 ZIP/CSV 通路、SHA256 拒绝、并发缓存复用以及现货/永续两种归档布局。
package binance

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"binanceconnector/internal/model"
)

// spotRow 官方文档的现货归档行：八列、无表头、2025 起为微秒。
const spotRow = "51175358,17.80180000,5.69000000,100,105,1735689600010866,True,True\n"

// perpHeader 永续归档表头，用于确认读取器按市场跳过首行。
const perpHeader = "agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker\n"

// perpRow 永续归档行：七列、毫秒时间戳。
const perpRow = "3508075764,80466.4,0.012,8235648673,8235648673,1793836800003,true\n"

// archiveBytes 把给定 CSV 内容封装为目标市场的公开归档 ZIP。
func archiveBytes(t *testing.T, name, content string) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := zip.NewWriter(&buffer)
	csv, err := writer.Create(name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = csv.Write([]byte(content)); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

// TestArchiveChecksumStreamingAndReuse 验证不整日加载、并发只下载一次且精度保留。
func TestArchiveChecksumStreamingAndReuse(t *testing.T) {
	data := archiveBytes(t, "BNBBTC-aggTrades-2025-01-01.csv", spotRow)
	hash := sha256.Sum256(data)
	var downloads atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, ".CHECKSUM") {
			_, _ = fmt.Fprintf(w, "%x BNBBTC-aggTrades-2025-01-01.zip", hash)
			return
		}
		downloads.Add(1)
		_, _ = w.Write(data)
	}))
	defer up.Close()
	s := withArchive(settingsForTest(t), model.MarketSpot, up.URL)
	c := New(s)
	defer c.Close()
	var wg sync.WaitGroup
	paths := make(chan string, 8)
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			path, err := c.Archive(context.Background(), model.MarketSpot, "BNBBTC", "2025-01-01")
			if err != nil {
				errs <- err
				return
			}
			paths <- path
		}()
	}
	wg.Wait()
	close(errs)
	close(paths)
	for err := range errs {
		t.Error(err)
	}
	if downloads.Load() != 1 {
		t.Fatalf("并发重复下载: %d", downloads.Load())
	}
	for path := range paths {
		count := 0
		err := ReadArchive(context.Background(), path, model.MarketSpot, "2025-01-01", func(trade model.Trade) error {
			count++
			if trade.Timestamp != 1735689600010 || trade.TimestampMicros != "1735689600010866" || trade.Side != "sell" {
				t.Fatalf("归档错误: %+v", trade)
			}
			return nil
		})
		if err != nil || count != 1 {
			t.Fatalf("CSV 流式读取失败: %d %v", count, err)
		}
	}
}

// TestArchivePerpLayout 验证永续归档按七列与毫秒解析，并跳过表头。
func TestArchivePerpLayout(t *testing.T) {
	data := archiveBytes(t, "BTCUSDT-aggTrades-2026-11-05.csv", perpHeader+perpRow)
	hash := sha256.Sum256(data)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, ".CHECKSUM") {
			_, _ = fmt.Fprintf(w, "%x BTCUSDT-aggTrades-2026-11-05.zip", hash)
			return
		}
		if !strings.Contains(r.URL.Path, "/data/futures/um/daily/aggTrades/BTCUSDT/") {
			t.Errorf("永续归档路径错误: %s", r.URL.Path)
		}
		_, _ = w.Write(data)
	}))
	defer up.Close()
	s := withArchive(settingsForTest(t), model.MarketPerp, up.URL)
	c := New(s)
	defer c.Close()
	path, err := c.Archive(context.Background(), model.MarketPerp, "BTCUSDT", "2026-11-05")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(path, model.MarketPerp) {
		t.Fatalf("缓存文件名必须含市场，避免与现货同日撞车: %s", path)
	}
	seen := 0
	err = ReadArchive(context.Background(), path, model.MarketPerp, "2026-11-05", func(trade model.Trade) error {
		seen++
		if trade.TradeID != "3508075764" || trade.Price != "80466.4" || trade.Size != "0.012" {
			t.Fatalf("永续归档字段错误: %+v", trade)
		}
		if trade.Timestamp != 1793836800003 || trade.TimestampMicros != "1793836800003000" {
			t.Fatalf("永续时间戳应为毫秒: %+v", trade)
		}
		if trade.Side != "sell" || trade.Kind != "aggregated" {
			t.Fatalf("主动方向错误: %+v", trade)
		}
		return nil
	})
	if err != nil || seen != 1 {
		t.Fatalf("永续 CSV 读取失败: %d %v", seen, err)
	}
}

// TestArchiveRejectsChecksumMismatch 验证下载损坏不发布缓存。
func TestArchiveRejectsChecksumMismatch(t *testing.T) {
	data := archiveBytes(t, "BNBBTC-aggTrades-2025-01-01.csv", spotRow)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, ".CHECKSUM") {
			_, _ = w.Write([]byte(strings.Repeat("0", 64)))
			return
		}
		_, _ = w.Write(data)
	}))
	defer up.Close()
	s := withArchive(settingsForTest(t), model.MarketSpot, up.URL)
	c := New(s)
	defer c.Close()
	if _, err := c.Archive(context.Background(), model.MarketSpot, "BNBBTC", "2025-01-01"); err == nil {
		t.Fatal("损坏归档不应缓存")
	}
}
