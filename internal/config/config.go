// Package config 集中读取 Connector 配置，业务模块不直接访问环境变量。
package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"time"
)

// Settings 定义 HTTP、上游访问、流缓存及历史文件参数。
type Settings struct {
	Address, RESTURL, HistoricalURL, WSURL, ArchiveURL, CacheDir, APIKey, Proxy                     string
	HTTPTimeout, ArchiveTimeout, WriteTimeout, KeepAlive, IdleGrace, ReconnectMin, ReconnectMax     time.Duration
	WeightPerMinute, BufferSize, ReplaySize, MaxStreams, SnapshotLimit, CatchupPages, MaxBookLevels int
}

// ProxyOff 表示显式直连；BINANCE_PROXY 设为该值时忽略默认代理。
const ProxyOff = "direct"

// Load 从环境读取配置，缺省仅监听本机；代理默认对齐同仓库本地转发端口。
func Load() (Settings, error) {
	s := Settings{
		Address:       text("BINANCE_LISTEN", "127.0.0.1:8091"),
		RESTURL:       text("BINANCE_REST_URL", "https://data-api.binance.vision"),
		HistoricalURL: text("BINANCE_HISTORY_URL", "https://api.binance.com"),
		WSURL:         text("BINANCE_WS_URL", "wss://data-stream.binance.vision"),
		ArchiveURL:    text("BINANCE_ARCHIVE_URL", "https://data.binance.vision"),
		CacheDir:      text("BINANCE_CACHE_DIR", "data/archives"), APIKey: os.Getenv("BINANCE_API_KEY"),
		Proxy: text("BINANCE_PROXY", text("HTTPS_PROXY", text("HTTP_PROXY", "http://127.0.0.1:6666"))),
	}
	for _, item := range []struct {
		key      string
		dst      *time.Duration
		fallback time.Duration
	}{
		{"BINANCE_HTTP_TIMEOUT", &s.HTTPTimeout, 20 * time.Second},
		{"BINANCE_ARCHIVE_TIMEOUT", &s.ArchiveTimeout, 5 * time.Minute},
		{"BINANCE_WRITE_TIMEOUT", &s.WriteTimeout, 15 * time.Second},
		{"BINANCE_KEEPALIVE", &s.KeepAlive, 15 * time.Second},
		{"BINANCE_IDLE_GRACE", &s.IdleGrace, 30 * time.Second},
		{"BINANCE_RECONNECT_MIN", &s.ReconnectMin, time.Second},
		{"BINANCE_RECONNECT_MAX", &s.ReconnectMax, 30 * time.Second},
	} {
		value, err := time.ParseDuration(text(item.key, item.fallback.String()))
		if err != nil || value <= 0 {
			return s, fmt.Errorf("%s 必须是正时长", item.key)
		}
		*item.dst = value
	}
	for _, item := range []struct {
		key      string
		dst      *int
		fallback int
	}{
		{"BINANCE_WEIGHT_PER_MINUTE", &s.WeightPerMinute, 1200},
		{"BINANCE_STREAM_BUFFER", &s.BufferSize, 512},
		{"BINANCE_REPLAY_SIZE", &s.ReplaySize, 2048},
		{"BINANCE_MAX_STREAMS", &s.MaxStreams, 64},
		{"BINANCE_DEPTH_SNAPSHOT_LIMIT", &s.SnapshotLimit, 1000},
		{"BINANCE_MAX_BOOK_LEVELS", &s.MaxBookLevels, 50000},
		{"BINANCE_CATCHUP_PAGES", &s.CatchupPages, 20},
	} {
		value, err := strconv.Atoi(text(item.key, strconv.Itoa(item.fallback)))
		if err != nil || value <= 0 {
			return s, fmt.Errorf("%s 必须是正整数", item.key)
		}
		*item.dst = value
	}
	for _, item := range []struct {
		value     string
		websocket bool
	}{
		{s.RESTURL, false}, {s.HistoricalURL, false}, {s.ArchiveURL, false}, {s.WSURL, true},
	} {
		u, err := url.Parse(item.value)
		if err != nil || u.Host == "" || u.RawQuery != "" || u.Fragment != "" || u.User != nil {
			return s, fmt.Errorf("无效上游 URL")
		}
		if item.websocket {
			if u.Scheme != "ws" && u.Scheme != "wss" {
				return s, fmt.Errorf("WebSocket URL 必须使用 ws/wss")
			}
		} else if u.Scheme != "http" && u.Scheme != "https" {
			return s, fmt.Errorf("REST URL 必须使用 http/https")
		}
	}
	if s.SnapshotLimit > 5000 || s.MaxBookLevels < 2*s.SnapshotLimit || s.ReconnectMax < s.ReconnectMin {
		return s, fmt.Errorf("盘口快照最多 5000 档，重连最大间隔不能小于最小间隔")
	}
	if s.Proxy != "" && s.Proxy != ProxyOff {
		u, err := url.Parse(s.Proxy)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https" && u.Scheme != "socks5") {
			return s, fmt.Errorf("BINANCE_PROXY 必须是可用的 http/https/socks5 代理，或 %s", ProxyOff)
		}
	}
	return s, nil
}

// text 返回显式环境值或默认值。
func text(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
