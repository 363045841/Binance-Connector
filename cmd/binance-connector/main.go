// Binance Connector 启动入口，统一处理 HTTP 服务和上游任务的优雅关闭。
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"binanceconnector/internal/binance"
	"binanceconnector/internal/config"
	"binanceconnector/internal/server"
)

// main 读取配置并监听，收到中断后先关闭流再等待 HTTP 请求退出。
func main() {
	settings, err := config.Load()
	if err != nil {
		slog.Error("配置无效", "error", err)
		os.Exit(1)
	}
	api := server.New(binance.New(settings), settings)
	httpServer := &http.Server{Addr: settings.Address, Handler: api, ReadTimeout: settings.HTTPTimeout, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 1 << 20}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	finished := make(chan error, 1)
	go func() {
		slog.Info("Binance Connector 启动", "address", settings.Address, "market", "spot")
		finished <- httpServer.ListenAndServe()
	}()
	select {
	case err := <-finished:
		api.Close()
		if err != http.ErrServerClosed {
			slog.Error("HTTP 服务失败", "error", err)
			os.Exit(1)
		}
		return
	case <-ctx.Done():
	}
	api.Close()
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdown); err != nil {
		slog.Error("HTTP 关闭超时", "error", err)
		_ = httpServer.Close()
	}
}
