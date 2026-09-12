package server

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"unified-api-gateway/internal/config"
	"unified-api-gateway/internal/store"
)

func adminBoundaryGateway(t *testing.T) *Server {
	t.Helper()
	gateway := testGateway(t, func(cfg *config.Config) {
		cfg.DefaultsProfile = "preview"
		cfg.Profiles["alternate"] = config.Profile{Adapter: "transparent", TrafficMode: "dry-run"}
	})
	t.Cleanup(gateway.Close)
	return gateway
}

func TestAdminWriteOriginBoundary(t *testing.T) {
	t.Setenv("GATEWAY_ADMIN_TOKEN", "")
	for _, tc := range []struct {
		name, host, origin, configured, forwarded string
		status                                    int
	}{
		{name: "CLI without Origin", host: "127.0.0.1:8781", status: 200},
		{name: "same origin", host: "127.0.0.1:8781", origin: "http://127.0.0.1:8781", status: 200},
		{name: "different port", host: "127.0.0.1:8781", origin: "http://127.0.0.1:3000", status: 403},
		{name: "different scheme", host: "127.0.0.1:8781", origin: "https://127.0.0.1:8781", status: 403},
		{name: "null origin", host: "127.0.0.1:8781", origin: "null", status: 403},
		{name: "origin has path", host: "127.0.0.1:8781", origin: "http://127.0.0.1:8781/", status: 403},
		{name: "origin has empty query", host: "127.0.0.1:8781", origin: "http://127.0.0.1:8781?", status: 403},
		{name: "explicit TLS proxy", host: "admin.example.test", origin: "https://admin.example.test", configured: "https://admin.example.test", status: 200},
		{name: "proxy explicit default port", host: "admin.example.test:443", origin: "https://admin.example.test", configured: "https://admin.example.test:443", status: 200},
		{name: "proxy must preserve host", host: "127.0.0.1:8781", origin: "https://admin.example.test", configured: "https://admin.example.test", status: 403},
		{name: "proxy CLI without Origin", host: "127.0.0.1:8781", configured: "https://admin.example.test", status: 200},
		{name: "forwarded proto not trusted", host: "admin.example.test", origin: "https://admin.example.test", forwarded: "https", status: 403},
		{name: "invalid configured scheme", host: "127.0.0.1:8781", configured: "file://admin.example.test", status: 503},
		{name: "invalid configured userinfo", host: "127.0.0.1:8781", configured: "https://user@admin.example.test", status: 503},
		{name: "invalid configured path", host: "127.0.0.1:8781", configured: "https://admin.example.test/", status: 503},
		{name: "invalid configured query", host: "127.0.0.1:8781", configured: "https://admin.example.test?", status: 503},
		{name: "invalid configured fragment", host: "127.0.0.1:8781", configured: "https://admin.example.test#", status: 503},
		{name: "invalid configured port", host: "127.0.0.1:8781", configured: "https://admin.example.test:65536", status: 503},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("GATEWAY_ADMIN_ORIGIN", tc.configured)
			gateway := adminBoundaryGateway(t)
			request := httptest.NewRequest(http.MethodPost, "http://"+tc.host+"/api/switch", strings.NewReader(`{"profile":"alternate"}`))
			request.RemoteAddr = "127.0.0.1:45678"
			request.Header.Set("Content-Type", "application/json")
			if tc.origin != "" {
				request.Header.Set("Origin", tc.origin)
			}
			if tc.forwarded != "" {
				request.Header.Set("X-Forwarded-Proto", tc.forwarded)
				request.Header.Set("Forwarded", "proto=https;host=admin.example.test")
			}
			response := httptest.NewRecorder()
			gateway.ServeAdminHTTP(response, request)
			if response.Code != tc.status {
				t.Fatalf("status=%d want=%d body=%s", response.Code, tc.status, response.Body)
			}
			changed := gateway.Snapshot().Config.DefaultsProfile == "alternate"
			if changed != (tc.status == http.StatusOK) {
				t.Fatalf("configuration changed=%v for status %d", changed, tc.status)
			}
		})
	}
}

func TestAdminRejectsCrossOriginNativeForm(t *testing.T) {
	t.Setenv("GATEWAY_ADMIN_TOKEN", "")
	t.Setenv("GATEWAY_ADMIN_ORIGIN", "")
	gateway := adminBoundaryGateway(t)
	request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:8781/api/switch", strings.NewReader("{\"profile\":\"alternate\"}=\r\n"))
	request.RemoteAddr = "127.0.0.1:45678"
	request.Header.Set("Origin", "http://localhost:3000")
	request.Header.Set("Content-Type", "text/plain")
	request.Header.Set("Sec-Fetch-Site", "cross-site")
	response := httptest.NewRecorder()
	gateway.ServeAdminHTTP(response, request)
	if response.Code != http.StatusForbidden || gateway.Snapshot().Config.DefaultsProfile != "preview" {
		t.Fatalf("cross-origin form: status=%d active=%s", response.Code, gateway.Snapshot().Config.DefaultsProfile)
	}
}

func TestAdminMethodsAndJSONBoundary(t *testing.T) {
	t.Setenv("GATEWAY_ADMIN_TOKEN", "test-admin")
	t.Setenv("GATEWAY_ADMIN_ORIGIN", "")
	for _, tc := range []struct {
		name, method, path, contentType, body string
		status                                int
	}{
		{"GET cannot switch", "GET", "/api/switch", "application/json", `{"profile":"alternate"}`, 405},
		{"HEAD cannot switch", "HEAD", "/api/switch", "application/json", `{"profile":"alternate"}`, 405},
		{"DELETE cannot activate", "DELETE", "/admin/v1/config/activate", "application/json", `{}`, 405},
		{"GET cannot rollback", "GET", "/admin/v1/config/rollback", "application/json", `{"id":"absent"}`, 405},
		{"GET cannot preview", "GET", "/api/preview", "application/json", `{}`, 405},
		{"POST cannot read status", "POST", "/api/status", "application/json", `{}`, 405},
		{"text plain", "POST", "/api/switch", "text/plain", `{"profile":"alternate"}`, 415},
		{"missing media type", "POST", "/api/switch", "", `{"profile":"alternate"}`, 415},
		{"invalid media type", "POST", "/api/switch", "application/json; charset", `{"profile":"alternate"}`, 415},
		{"second document", "POST", "/api/switch", "application/json", `{"profile":"alternate"}{}`, 400},
		{"form suffix", "POST", "/api/switch", "application/json", "{\"profile\":\"alternate\"}=\r\n", 400},
		{"null document", "POST", "/api/activate", "application/json", `null`, 400},
		{"unknown property", "POST", "/api/activate", "application/json", `{"yamll":"version: 1"}`, 400},
		{"valid JSON parameters", "POST", "/api/switch", "application/json; charset=utf-8", "{\"profile\":\"alternate\"}\n \t", 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gateway := adminBoundaryGateway(t)
			request := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			request.Header.Set("X-Admin-Token", "test-admin")
			if tc.contentType != "" {
				request.Header.Set("Content-Type", tc.contentType)
			}
			response := httptest.NewRecorder()
			gateway.ServeAdminHTTP(response, request)
			if response.Code != tc.status {
				t.Fatalf("status=%d want=%d body=%s", response.Code, tc.status, response.Body)
			}
			if tc.status == 405 && response.Header().Get("Allow") == "" {
				t.Fatal("405 response omitted Allow")
			}
			if tc.status != 200 && gateway.Snapshot().Config.DefaultsProfile != "preview" {
				t.Fatal("rejected request changed active configuration")
			}
		})
	}
}

type adminCountingReader struct {
	io.Reader
	bytes int
}

func (reader *adminCountingReader) Read(data []byte) (int, error) {
	n, err := reader.Reader.Read(data)
	reader.bytes += n
	return n, err
}

func TestAdminJSONBodyLimitsIncludeRollbackAndTrailingWhitespace(t *testing.T) {
	t.Setenv("GATEWAY_ADMIN_TOKEN", "test-admin")
	t.Setenv("GATEWAY_ADMIN_ORIGIN", "")
	history, err := store.Open(filepath.Join(t.TempDir(), "gateway.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = history.Close() })
	initial := adminBoundaryGateway(t)
	gateway, err := New(Options{Snapshot: initial.Snapshot(), Store: history})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(gateway.Close)
	for _, tc := range []struct {
		path, prefix string
		limit        int
	}{
		{"/api/rollback", `{"id":"missing"}`, 1 << 20},
		{"/api/switch", `{"profile":"alternate"}`, 1 << 20},
		{"/api/preview", `{"profile":"preview"}`, 4 << 20},
		{"/admin/v1/config/validate", `{"yaml":"version: 1"}`, 16 << 20},
	} {
		for _, lengthKnown := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/content-length=%v", tc.path, lengthKnown), func(t *testing.T) {
				body := &adminCountingReader{Reader: io.MultiReader(strings.NewReader(tc.prefix), strings.NewReader(strings.Repeat(" ", tc.limit)))}
				request := httptest.NewRequest(http.MethodPost, tc.path, body)
				if lengthKnown {
					request.ContentLength = int64(len(tc.prefix) + tc.limit)
				}
				request.Header.Set("X-Admin-Token", "test-admin")
				request.Header.Set("Content-Type", "application/json")
				response := httptest.NewRecorder()
				gateway.ServeAdminHTTP(response, request)
				if response.Code != http.StatusRequestEntityTooLarge {
					t.Fatalf("status=%d bytes_read=%d body=%s", response.Code, body.bytes, response.Body)
				}
				if body.bytes > tc.limit+1 {
					t.Fatalf("read %d bytes beyond budget %d", body.bytes, tc.limit)
				}
				if gateway.Snapshot().Config.DefaultsProfile != "preview" {
					t.Fatal("oversized JSON changed configuration")
				}
			})
		}
	}
}
