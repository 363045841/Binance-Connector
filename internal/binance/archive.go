// 本文件下载校验 Binance 日聚合成交归档，并逐行读取，不将全日成交装入内存。
// 现货与永续的归档路径、表头、列数与时间单位都不同，全部按市场显式声明，不做猜测。
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

// archiveLayout 是单个市场归档的静态格式声明：
// path 是 data.binance.vision 下的目录，header 表示 CSV 是否带表头，
// columns 是期望的列数，microsSince 是 transact_time 切换为微秒的起始日期（空串表示始终毫秒）。
type archiveLayout struct {
	path        string
	header      bool
	columns     int
	microsSince string
}

// archiveLayouts 是市场到归档格式的唯一定义处。
var archiveLayouts = map[string]archiveLayout{
	// 现货：无表头八列，2025-01-01 起 transact_time 为微秒，此前是毫秒。
	model.MarketSpot: {path: "/data/spot/daily/aggTrades/", header: false, columns: 8, microsSince: "2025-01-01"},
	// 永续：带表头七列，transact_time 始终为毫秒，聚合成交 ID 与实时 a 字段同一空间。
	model.MarketPerp: {path: "/data/futures/um/daily/aggTrades/", header: true, columns: 7},
}

// micros 判断该布局在给定日期下是否使用微秒时间戳。
func (l archiveLayout) micros(date string) bool {
	return l.microsSince != "" && date >= l.microsSince
}

// archive 返回市场的归档布局；未知市场返回错误。
func archiveLayoutFor(market string) (archiveLayout, error) {
	layout, ok := archiveLayouts[market]
	if !ok {
		return archiveLayout{}, fmt.Errorf("不支持的市场: %s", market)
	}
	return layout, nil
}

// Archive 获取已校验缓存；同日并发下载合并，等待者可以取消。
// 缓存文件名带市场前缀，同一天两个市场的归档互不覆盖。
func (c *Client) Archive(ctx context.Context, market, symbol, date string) (string, error) {
	day, err := time.Parse("2006-01-02", date)
	if err != nil || !day.Before(time.Now().UTC().Truncate(24*time.Hour)) {
		return "", fmt.Errorf("归档 date 必须是 UTC 已结束日期 YYYY-MM-DD")
	}
	if _, err := archiveLayoutFor(market); err != nil {
		return "", err
	}
	if _, err := model.Symbol(symbol); err != nil {
		return "", err
	}
	name := market + "-" + symbol + "-aggTrades-" + date + ".zip"
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
		err := c.downloadArchive(ctx, market, symbol, date, name, filename)
		c.archiveMu.Lock()
		delete(c.archiveJobs, name)
		close(done)
		c.archiveMu.Unlock()
		return filename, err
	}
}

// archiveRequest 不沿用短 REST 超时，仍复用 Transport 与标准代理。
func (c *Client) archiveRequest(ctx context.Context, market, path string) (*http.Response, error) {
	settings, ok := c.Settings.Markets[market]
	if !ok {
		return nil, fmt.Errorf("不支持的市场: %s", market)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(settings.Archive, "/")+path, nil)
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
func (c *Client) downloadArchive(ctx context.Context, market, symbol, date, name, filename string) error {
	layout, err := archiveLayoutFor(market)
	if err != nil {
		return err
	}
	base := layout.path + symbol + "/" + name
	check, err := c.archiveRequest(ctx, market, base+".CHECKSUM")
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
	resp, err := c.archiveRequest(ctx, market, base)
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

// ReadArchive 依据市场的归档布局跳过表头并保留微秒精度，流式提交原始成交。
func ReadArchive(ctx context.Context, filename, market, date string, emit func(model.Trade) error) error {
	layout, err := archiveLayoutFor(market)
	if err != nil {
		return err
	}
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
		if line == 1 && layout.header {
			continue
		}
		t, err := parseArchiveTrade(row, date, layout)
		if err != nil {
			return fmt.Errorf("CSV 第 %d 行: %w", line, err)
		}
		if err := emit(t); err != nil {
			return err
		}
	}
}

// ParseArchiveTrade 按现货归档格式解析八列聚合成交，聚合成交身份独立于原始 tradeId。
// 兼容未来可能出现的额外尾部列，只要求至少八列。
func ParseArchiveTrade(row []string, date string) (model.Trade, error) {
	layout, ok := archiveLayouts[model.MarketSpot]
	if !ok {
		return model.Trade{}, fmt.Errorf("缺少现货归档布局")
	}
	return parseArchiveTrade(row, date, layout)
}

// parseArchiveTrade 按给定布局解析一行；列数与时间单位都由布局决定，不做跨市场猜测。
func parseArchiveTrade(row []string, date string, layout archiveLayout) (model.Trade, error) {
	if len(row) < layout.columns {
		return model.Trade{}, fmt.Errorf("聚合成交应为 %d 列，实际 %d 列", layout.columns, len(row))
	}
	day, parseErr := time.Parse("2006-01-02", date)
	if parseErr != nil {
		return model.Trade{}, parseErr
	}
	id, err := strconv.ParseInt(row[0], 10, 64)
	if err != nil {
		return model.Trade{}, err
	}
	raw, err := strconv.ParseInt(row[5], 10, 64)
	if err != nil {
		return model.Trade{}, err
	}
	maker, err := strconv.ParseBool(row[6])
	if err != nil {
		return model.Trade{}, err
	}
	millis, micros := raw, raw*1000
	if layout.micros(date) {
		millis, micros = raw/1000, raw
	}
	if millis < day.UnixMilli() || millis >= day.Add(24*time.Hour).UnixMilli() {
		return model.Trade{}, fmt.Errorf("成交时间不在所请求 UTC 日期内")
	}
	t, err := model.NewTrade(id, millis, row[1], row[2], "", maker, "aggregated")
	if err != nil {
		return model.Trade{}, err
	}
	t.TimestampMicros = strconv.FormatInt(micros, 10)
	t.FirstTradeID = row[3]
	t.LastTradeID = row[4]
	return t, nil
}
