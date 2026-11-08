// Package config 集中读取 Connector 配置，业务模块不直接访问环境变量。
package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"binanceconnector/internal/model"
)

// Endpoint 是市场内可能分属不同宿主的端点族；现货的历史成交与归档在独立域名。
type Endpoint int

const (
	// EndpointREST 是市场主 REST 宿主，承载目录、K 线、盘口与聚合成交。
	EndpointREST Endpoint = iota
	// EndpointHistory 是历史原始成交宿主；现货需独立域名并携带 API Key，永续同域。
	EndpointHistory
)

// MarketSettings 描述单个市场的上游地址组与独立权重预算。
// 现货与永续的 REST/WS/归档宿主完全不同，交易所又按 IP 分市场计权重，
// 因此地址与预算必须成组配置，不能跨市场共用。
type MarketSettings struct {
	REST, Historical, WS, Archive string
	WeightPerMinute               int
}

// Base 返回指定端点族的基地址。
func (m MarketSettings) Base(endpoint Endpoint) string {
	if endpoint == EndpointHistory {
		return m.Historical
	}
	return m.REST
}

// Settings 定义 HTTP、上游访问、流缓存及历史文件参数。
type Settings struct {
	Address                 string
	Markets                 map[string]MarketSettings
	CacheDir, APIKey, Proxy string
	HTTPTimeout             time.Duration
	ArchiveTimeout          time.Duration
	WriteTimeout            time.Duration
	KeepAlive               time.Duration
	IdleGrace               time.Duration
	ReconnectMin            time.Duration
	ReconnectMax            time.Duration
	BufferSize              int
	ReplaySize              int
	MaxStreams              int
	SnapshotLimit           int
	CatchupPages            int
	MaxBookLevels           int
	// AllowedOrigins 是 loopback 之外额外放行的浏览器来源；loopback 始终允许。
	AllowedOrigins []string
}

// ProxyOff 表示显式直连；BINANCE_PROXY 设为该值时忽略默认代理。
const ProxyOff = "direct"

// Load 从环境读取配置，缺省仅监听本机；代理默认对齐同仓库本地转发端口。
func Load() (Settings, error) {
	s := Settings{
		Address: text("BINANCE_LISTEN", "127.0.0.1:8091"),
		Markets: map[string]MarketSettings{
			model.MarketSpot: {
				REST:            text("BINANCE_REST_URL", "https://data-api.binance.vision"),
				Historical:      text("BINANCE_HISTORY_URL", "https://api.binance.com"),
				WS:              text("BINANCE_WS_URL", "wss://data-stream.binance.vision"),
				Archive:         text("BINANCE_ARCHIVE_URL", "https://data.binance.vision"),
				WeightPerMinute: 1200,
			},
			model.MarketPerp: {
				REST:            text("BINANCE_PERP_REST_URL", "https://fapi.binance.com"),
				Historical:      text("BINANCE_PERP_HISTORY_URL", "https://fapi.binance.com"),
				WS:              text("BINANCE_PERP_WS_URL", "wss://fstream.binance.com"),
				Archive:         text("BINANCE_PERP_ARCHIVE_URL", "https://data.binance.vision"),
				WeightPerMinute: 2400,
			},
		},
		CacheDir: text("BINANCE_CACHE_DIR", "data/archives"), APIKey: os.Getenv("BINANCE_API_KEY"),
		Proxy: text("BINANCE_PROXY", text("HTTPS_PROXY", text("HTTP_PROXY", "http://127.0.0.1:6666"))),
	}
	origins, err := parseOrigins("BINANCE_ALLOWED_ORIGINS")
	if err != nil {
		return s, err
	}
	s.AllowedOrigins = origins
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
		key     string
		market  string
		fallback int
	}{
		{"BINANCE_WEIGHT_PER_MINUTE", model.MarketSpot, 1200},
		{"BINANCE_PERP_WEIGHT_PER_MINUTE", model.MarketPerp, 2400},
	} {
		value, err := strconv.Atoi(text(item.key, strconv.Itoa(item.fallback)))
		if err != nil || value <= 0 {
			return s, fmt.Errorf("%s 必须是正整数", item.key)
		}
		settings := s.Markets[item.market]
		settings.WeightPerMinute = value
		s.Markets[item.market] = settings
	}
	for _, market := range model.Markets() {
		settings, ok := s.Markets[market]
		if !ok {
			return s, fmt.Errorf("缺少 %s 市场配置", market)
		}
		for _, item := range []struct {
			value     string
			websocket bool
		}{
			{settings.REST, false}, {settings.Historical, false}, {settings.Archive, false}, {settings.WS, true},
		} {
			u, err := url.Parse(item.value)
			if err != nil || u.Host == "" || u.RawQuery != "" || u.Fragment != "" || u.User != nil {
				return s, fmt.Errorf("%s 市场上游 URL 无效", market)
			}
			if item.websocket {
				if u.Scheme != "ws" && u.Scheme != "wss" {
					return s, fmt.Errorf("%s 市场 WebSocket URL 必须使用 ws/wss", market)
				}
			} else if u.Scheme != "http" && u.Scheme != "https" {
				return s, fmt.Errorf("%s 市场 REST URL 必须使用 http/https", market)
			}
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

// parseOrigins 解析逗号分隔的浏览器来源白名单，只接受 http/https 的纯 origin。
func parseOrigins(key string) ([]string, error) {
	var origins []string
	for _, item := range strings.Split(os.Getenv(key), ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		u, err := url.Parse(item)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" ||
			u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
			return nil, fmt.Errorf("%s 中的 %q 不是合法来源，需形如 https://host[:port]", key, item)
		}
		origins = append(origins, strings.ToLower(u.Scheme+"://"+u.Host))
	}
	return origins, nil
}

// text 返回显式环境值或默认值。
func text(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
