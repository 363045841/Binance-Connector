// 本文件验证浏览器来源白名单的解析与校验。
package config

import "testing"

// TestAllowedOrigins 验证逗号分隔解析、大小写归一与非法值拒绝。
func TestAllowedOrigins(t *testing.T) {
	t.Setenv("BINANCE_ALLOWED_ORIGINS", " https://Demo.Example:8443 ,, ")
	s, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(s.AllowedOrigins) != 1 || s.AllowedOrigins[0] != "https://demo.example:8443" {
		t.Fatalf("白名单解析结果: %v", s.AllowedOrigins)
	}

	t.Setenv("BINANCE_ALLOWED_ORIGINS", "https://demo.example/path")
	if _, err := Load(); err == nil {
		t.Fatal("带路径的来源应被拒绝")
	}
}
