// Package binance 封装 Binance REST 与 WebSocket 适配，现货与 USDT-M 永续共用一套协议实现。
// 限流、冷却、错误状态在此统一处理；市场差异由 Market 参数与配置中的地址组表达。
package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"binanceconnector/internal/config"
	"binanceconnector/internal/model"
)

// Error 保留上游 HTTP 状态及重试时间，不把 429/451 隐藏成空行情。
type Error struct {
	Status        int
	Code, Message string
	RetryAfter    time.Duration
}

func (e *Error) Error() string { return fmt.Sprintf("%s: %s", e.Code, e.Message) }

// budget 是单个市场的分钟权重窗口；交易所按 IP 分市场计权重，不能合并计算。
type budget struct {
	mu           sync.Mutex
	window       time.Time
	used         int
	blockedUntil time.Time
}

// Client 共用 HTTP Transport，按市场各自维护权重预算与目录缓存。
// 目录、历史文件与文档缓存都有独立生命周期，互不干扰。
type Client struct {
	Settings config.Settings
	HTTP     *http.Client
	budgets  map[string]*budget
	cacheMu  sync.Mutex
	cache    map[string]*directory
	archiveMu sync.Mutex
	archiveJobs map[string]chan struct{}
}

// directory 是单个市场的品种目录缓存，永续与现货互不覆盖。
type directory struct {
	mu    sync.Mutex
	bytes []byte
	at    time.Time
}

// New 创建支持配置代理的客户端；默认代理由 Settings.Proxy 决定。
func New(s config.Settings) *Client {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConnsPerHost = 16
	t.Proxy = ProxyFunc(s)
	c := &Client{Settings: s, HTTP: &http.Client{Transport: t, Timeout: s.HTTPTimeout}, budgets: make(map[string]*budget), cache: make(map[string]*directory), archiveJobs: make(map[string]chan struct{})}
	for _, market := range model.Markets() {
		c.budgets[market] = &budget{}
		c.cache[market] = &directory{}
	}
	return c
}

// ProxyFunc 返回 HTTP 与 WebSocket 共用的代理选择函数；ProxyOff 表示直连。
func ProxyFunc(s config.Settings) func(*http.Request) (*url.URL, error) {
	if s.Proxy == "" || s.Proxy == config.ProxyOff {
		return nil
	}
	u, err := url.Parse(s.Proxy)
	if err != nil {
		return nil
	}
	return http.ProxyURL(u)
}

// Close 释放空闲连接。
func (c *Client) Close() { c.HTTP.CloseIdleConnections() }

// market 返回市场的地址组与权重预算；未知市场返回错误。
func (c *Client) market(market string) (config.MarketSettings, *budget, error) {
	settings, ok := c.Settings.Markets[market]
	if !ok || !model.ValidMarket(market) {
		return config.MarketSettings{}, nil, fmt.Errorf("不支持的市场: %s", market)
	}
	limit, ok := c.budgets[market]
	if !ok {
		return config.MarketSettings{}, nil, fmt.Errorf("缺少市场权重预算: %s", market)
	}
	return settings, limit, nil
}

// marketDirectory 返回市场对应的目录缓存槽。
func (c *Client) marketDirectory(market string) (*directory, error) {
	if !model.ValidMarket(market) {
		return nil, fmt.Errorf("不支持的市场: %s", market)
	}
	slot, ok := c.cache[market]
	if !ok {
		return nil, fmt.Errorf("缺少市场目录缓存: %s", market)
	}
	return slot, nil
}

// wait 在所属市场的权重窗口内预留预算，429/418 冷却期间不继续访问上游。
func (b *budget) wait(ctx context.Context, weight, perMinute int) error {
	if weight > perMinute {
		return fmt.Errorf("请求权重大于配置的分钟预算")
	}
	for {
		b.mu.Lock()
		now := time.Now()
		if now.Sub(b.window) >= time.Minute {
			b.window = now
			b.used = 0
		}
		until := b.blockedUntil
		if b.used+weight > perMinute && b.window.Add(time.Minute).After(until) {
			until = b.window.Add(time.Minute)
		}
		if !until.After(now) {
			b.used += weight
			b.mu.Unlock()
			return nil
		}
		b.mu.Unlock()
		if err := Sleep(ctx, time.Until(until)); err != nil {
			return err
		}
	}
}

// Sleep 提供可取消等待，关闭服务不遗留限频或重连 goroutine。
func Sleep(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// request 获取响应并吸收上游出口权重；幂等 GET 在本地代理连接抖动时最多重试一次，
// 不做无限重试，也不掩盖 4xx/5xx 上游状态。
func (c *Client) request(ctx context.Context, market string, endpoint config.Endpoint, path string, query url.Values, weight int, key bool) (*http.Response, error) {
	settings, limit, err := c.market(market)
	if err != nil {
		return nil, err
	}
	if err := limit.wait(ctx, weight, settings.WeightPerMinute); err != nil {
		return nil, err
	}
	target := strings.TrimRight(settings.Base(endpoint), "/") + path
	if len(query) > 0 {
		target += "?" + query.Encode()
	}
	var resp *http.Response
	for attempt := 0; ; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
		if err != nil {
			return nil, err
		}
		if key && c.Settings.APIKey != "" {
			req.Header.Set("X-MBX-APIKEY", c.Settings.APIKey)
		}
		resp, err = c.HTTP.Do(req)
		if err == nil {
			break
		}
		if attempt >= 1 || ctx.Err() != nil {
			return nil, err
		}
		if err := Sleep(ctx, 250*time.Millisecond); err != nil {
			return nil, err
		}
	}
	limit.mu.Lock()
	if n, err := strconv.Atoi(resp.Header.Get("X-MBX-USED-WEIGHT-1M")); err == nil && n > limit.used {
		limit.used = n
	}
	limit.mu.Unlock()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return resp, nil
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	code := "UPSTREAM_UNAVAILABLE"
	if resp.StatusCode == 429 || resp.StatusCode == 418 {
		code = "RATE_LIMITED"
	}
	if resp.StatusCode == 451 || resp.StatusCode == 403 {
		code = "ACCESS_DENIED"
	}
	if resp.StatusCode == 400 {
		code = "INVALID_REQUEST"
	}
	delay := time.Duration(0)
	if raw := resp.Header.Get("Retry-After"); raw != "" {
		if sec, err := strconv.Atoi(raw); err == nil {
			delay = time.Duration(sec) * time.Second
		} else if at, err := http.ParseTime(raw); err == nil {
			delay = time.Until(at)
		}
	}
	if code == "RATE_LIMITED" {
		if delay <= 0 {
			delay = time.Minute
		}
		limit.mu.Lock()
		until := time.Now().Add(delay)
		if until.After(limit.blockedUntil) {
			limit.blockedUntil = until
		}
		limit.mu.Unlock()
	}
	return nil, &Error{Status: resp.StatusCode, Code: code, Message: strings.TrimSpace(string(body)), RetryAfter: delay}
}

// get 解码有大小上限的 REST JSON，失败返回协议错误而非部分结果。
func (c *Client) get(ctx context.Context, market string, endpoint config.Endpoint, path string, query url.Values, weight int, key bool, out any) error {
	resp, err := c.request(ctx, market, endpoint, path, query, weight, key)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(io.LimitReader(resp.Body, 16<<20)).Decode(out); err != nil {
		return fmt.Errorf("解析 Binance JSON: %w", err)
	}
	return nil
}
