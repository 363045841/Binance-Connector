// Package binance 封装 Binance 现货 REST，限流、冷却和错误状态在此统一处理。
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
)

// Error 保留上游 HTTP 状态及重试时间，不把 429/451 隐藏成空行情。
type Error struct {
	Status        int
	Code, Message string
	RetryAfter    time.Duration
}

func (e *Error) Error() string { return fmt.Sprintf("%s: %s", e.Code, e.Message) }

// Client 共用 HTTP Transport 与出口权重预算，目录缓存与历史文件有独立生命周期。
type Client struct {
	Settings     config.Settings
	HTTP         *http.Client
	mu           sync.Mutex
	window       time.Time
	used         int
	blockedUntil time.Time
	directoryMu  sync.Mutex
	directory    []byte
	directoryAt  time.Time
	archiveMu    sync.Mutex
	archiveJobs  map[string]chan struct{}
}

// New 创建支持配置代理的客户端；默认代理由 Settings.Proxy 决定。
func New(s config.Settings) *Client {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConnsPerHost = 16
	t.Proxy = ProxyFunc(s)
	return &Client{Settings: s, HTTP: &http.Client{Transport: t, Timeout: s.HTTPTimeout}, archiveJobs: make(map[string]chan struct{})}
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

// wait 在共享权重窗口内预留预算，429/418 冷却期间不继续访问上游。
func (c *Client) wait(ctx context.Context, weight int) error {
	if weight > c.Settings.WeightPerMinute {
		return fmt.Errorf("请求权重大于配置的分钟预算")
	}
	for {
		c.mu.Lock()
		now := time.Now()
		if now.Sub(c.window) >= time.Minute {
			c.window = now
			c.used = 0
		}
		until := c.blockedUntil
		if c.used+weight > c.Settings.WeightPerMinute && c.window.Add(time.Minute).After(until) {
			until = c.window.Add(time.Minute)
		}
		if !until.After(now) {
			c.used += weight
			c.mu.Unlock()
			return nil
		}
		c.mu.Unlock()
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
func (c *Client) request(ctx context.Context, base, path string, query url.Values, weight int, key bool) (*http.Response, error) {
	if err := c.wait(ctx, weight); err != nil {
		return nil, err
	}
	endpoint := strings.TrimRight(base, "/") + path
	if len(query) > 0 {
		endpoint += "?" + query.Encode()
	}
	var resp *http.Response
	for attempt := 0; ; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
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
	c.mu.Lock()
	if n, err := strconv.Atoi(resp.Header.Get("X-MBX-USED-WEIGHT-1M")); err == nil && n > c.used {
		c.used = n
	}
	c.mu.Unlock()
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
		c.mu.Lock()
		until := time.Now().Add(delay)
		if until.After(c.blockedUntil) {
			c.blockedUntil = until
		}
		c.mu.Unlock()
	}
	return nil, &Error{Status: resp.StatusCode, Code: code, Message: strings.TrimSpace(string(body)), RetryAfter: delay}
}

// get 解码有大小上限的 REST JSON，失败返回协议错误而非部分结果。
func (c *Client) get(ctx context.Context, base, path string, query url.Values, weight int, key bool, out any) error {
	resp, err := c.request(ctx, base, path, query, weight, key)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(io.LimitReader(resp.Body, 16<<20)).Decode(out); err != nil {
		return fmt.Errorf("解析 Binance JSON: %w", err)
	}
	return nil
}
