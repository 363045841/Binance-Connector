// 本文件验证真实 ZIP/CSV 通路、SHA256 拒绝和并发缓存复用。
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

// archiveBytes 将官方文档的现货原始成交行封装为其公开归档格式。
func archiveBytes(t *testing.T) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := zip.NewWriter(&buffer)
	csv, err := writer.Create("BNBBTC-aggTrades-2025-01-01.csv")
	if err != nil {
		t.Fatal(err)
	}
	_, err = csv.Write([]byte("51175358,17.80180000,5.69000000,100,105,1735689600010866,True,True\n"))
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

// TestArchiveChecksumStreamingAndReuse 验证不整日加载、并发只下载一次且精度保留。
func TestArchiveChecksumStreamingAndReuse(t *testing.T) {
	data := archiveBytes(t)
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
	s := settingsForTest(t)
	s.ArchiveURL = up.URL
	c := New(s)
	defer c.Close()
	var wg sync.WaitGroup
	paths := make(chan string, 8)
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			path, err := c.Archive(context.Background(), "BNBBTC", "2025-01-01")
			if err != nil {
				errs <- err
			} else {
				paths <- path
			}
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
		err := ReadArchive(context.Background(), path, "2025-01-01", func(trade model.Trade) error {
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

// TestArchiveRejectsChecksumMismatch 验证下载损坏不发布缓存。
func TestArchiveRejectsChecksumMismatch(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, ".CHECKSUM") {
			_, _ = w.Write([]byte(strings.Repeat("0", 64)))
			return
		}
		_, _ = w.Write(archiveBytes(t))
	}))
	defer up.Close()
	s := settingsForTest(t)
	s.ArchiveURL = up.URL
	c := New(s)
	defer c.Close()
	if _, err := c.Archive(context.Background(), "BNBBTC", "2025-01-01"); err == nil {
		t.Fatal("损坏归档不应缓存")
	}
}
