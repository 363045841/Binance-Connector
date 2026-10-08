// 本文件下载校验 Binance 日成交归档，并逐行读取，不将全日成交装入内存。
package binance

import (
	"archive/zip"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"binanceconnector/internal/model"
)

const maxArchiveBytes int64 = 1 << 30
const maxCSVBytes int64 = 8 << 30

// Archive 获取已校验缓存；同日并发下载合并，等待者可以取消。
func (c *Client) Archive(ctx context.Context, symbol, date string) (string, error) {
	day, err := time.Parse("2006-01-02", date)
	if err != nil || !day.Before(time.Now().UTC().Truncate(24*time.Hour)) {
		return "", fmt.Errorf("归档 date 必须是 UTC 已结束日期 YYYY-MM-DD")
	}
	if _, err := model.Symbol(symbol); err != nil {
		return "", err
	}
	name := symbol + "-trades-" + date + ".zip"
	filename := filepath.Join(c.Settings.CacheDir, name)
	for {
		c.archiveMu.Lock()
		if done, ok := c.archiveJobs[name]; ok {
			c.archiveMu.Unlock()
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-done:
				continue
			}
		}
		if info, err := os.Stat(filename); err == nil && info.Size() > 0 {
			c.archiveMu.Unlock()
			return filename, nil
		}
		done := make(chan struct{})
		c.archiveJobs[name] = done
		c.archiveMu.Unlock()
		err := c.downloadArchive(ctx, symbol, date, name, filename)
		c.archiveMu.Lock()
		delete(c.archiveJobs, name)
		close(done)
		c.archiveMu.Unlock()
		return filename, err
	}
}

// archiveRequest 不沿用短 REST 超时，仍复用 Transport 与标准代理。
func (c *Client) archiveRequest(ctx context.Context, path string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(c.Settings.ArchiveURL, "/")+path, nil)
	if err != nil {
		return nil, err
	}
	client := *c.HTTP
	client.Timeout = c.Settings.ArchiveTimeout
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		return nil, &Error{Status: resp.StatusCode, Code: "ARCHIVE_UNAVAILABLE", Message: fmt.Sprintf("历史文件尚未发布或不存在，HTTP %d", resp.StatusCode)}
	}
	return resp, nil
}

// downloadArchive 使用校验和与临时文件原子发布；不接受未完整下载的缓存。
func (c *Client) downloadArchive(ctx context.Context, symbol, date, name, filename string) error {
	base := "/data/spot/daily/trades/" + symbol + "/" + name
	check, err := c.archiveRequest(ctx, base+".CHECKSUM")
	if err != nil {
		return err
	}
	body, err := io.ReadAll(io.LimitReader(check.Body, 4096))
	check.Body.Close()
	if err != nil {
		return err
	}
	fields := strings.Fields(string(body))
	if len(fields) < 1 {
		return fmt.Errorf("空归档校验和")
	}
	expected, err := hex.DecodeString(fields[0])
	if err != nil || len(expected) != sha256.Size {
		return fmt.Errorf("无效归档校验和")
	}
	resp, err := c.archiveRequest(ctx, base)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.ContentLength > maxArchiveBytes {
		return fmt.Errorf("归档压缩文件超过 1 GiB")
	}
	if err := os.MkdirAll(c.Settings.CacheDir, 0755); err != nil {
		return err
	}
	file, err := os.CreateTemp(c.Settings.CacheDir, "archive-*.partial")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	hash := sha256.New()
	written, copyErr := io.Copy(io.MultiWriter(file, hash), io.LimitReader(resp.Body, maxArchiveBytes+1))
	closeErr := file.Close()
	if copyErr != nil {
		return copyErr
	}
	if closeErr != nil {
		return closeErr
	}
	if written > maxArchiveBytes {
		return fmt.Errorf("归档压缩文件超过 1 GiB")
	}
	if hex.EncodeToString(hash.Sum(nil)) != strings.ToLower(fields[0]) {
		return fmt.Errorf("历史归档 SHA256 不匹配")
	}
	z, err := zip.OpenReader(file.Name())
	if err != nil {
		return err
	}
	z.Close()
	return os.Rename(file.Name(), filename)
}

// ReadArchive 依据归档日期确定毫秒/微秒单位，保留微秒精度并流式提交原始成交。
func ReadArchive(ctx context.Context, filename, date string, emit func(model.Trade) error) error {
	z, err := zip.OpenReader(filename)
	if err != nil {
		return err
	}
	defer z.Close()
	if len(z.File) != 1 || !strings.HasSuffix(z.File[0].Name, ".csv") || z.File[0].UncompressedSize64 > uint64(maxCSVBytes) {
		return fmt.Errorf("归档必须包含一个不超过 8 GiB 的 CSV")
	}
	file, err := z.File[0].Open()
	if err != nil {
		return err
	}
	defer file.Close()
	r := csv.NewReader(io.LimitReader(file, maxCSVBytes+1))
	r.FieldsPerRecord = -1
	line := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		row, err := r.Read()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		line++
		if line == 1 && len(row) > 0 && (row[0] == "id" || row[0] == "trade_id") {
			continue
		}
		t, err := ParseArchiveTrade(row, date)
		if err != nil {
			return fmt.Errorf("CSV 第 %d 行: %w", line, err)
		}
		if err := emit(t); err != nil {
			return err
		}
	}
}

// ParseArchiveTrade 解析官方 spot trades 的七列格式，不把 aggTrades 伪装成原始成交。
func ParseArchiveTrade(row []string, date string) (model.Trade, error) {
	if len(row) != 7 {
		return model.Trade{}, fmt.Errorf("原始成交应为七列")
	}
	day, parseErr := time.Parse("2006-01-02", date)
	if parseErr != nil {
		return model.Trade{}, parseErr
	}
	id, err := strconv.ParseInt(row[0], 10, 64)
	if err != nil {
		return model.Trade{}, err
	}
	timestamp, err := strconv.ParseInt(row[4], 10, 64)
	if err != nil {
		return model.Trade{}, err
	}
	maker, err := strconv.ParseBool(row[5])
	if err != nil {
		return model.Trade{}, err
	}
	micros := timestamp * 1000
	if date >= "2025-01-01" {
		micros = timestamp
		timestamp /= 1000
	}
	if timestamp < day.UnixMilli() || timestamp >= day.Add(24*time.Hour).UnixMilli() {
		return model.Trade{}, fmt.Errorf("成交时间不在所请求 UTC 日期内")
	}
	t, err := model.NewTrade(id, timestamp, row[1], row[2], row[3], maker, "raw")
	t.TimestampMicros = strconv.FormatInt(micros, 10)
	return t, err
}
