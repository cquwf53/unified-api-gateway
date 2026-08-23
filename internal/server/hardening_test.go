package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"unified-api-gateway/internal/config"
)

func testGateway(t *testing.T, configure func(*config.Config)) *Server {
	t.Helper()
	cfg := &config.Config{Version: 1, Profiles: map[string]config.Profile{
		"preview": {Name: "preview", Adapter: "transparent", TrafficMode: "dry-run"},
	}}
	if configure != nil {
		configure(cfg)
	}
	snapshot, err := config.NewSnapshot(cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := New(Options{Snapshot: snapshot})
	if err != nil {
		t.Fatal(err)
	}
	return gateway
}

func TestEmbeddedConsoleServedWithCSPAndETag(t *testing.T) {
	t.Setenv("GATEWAY_ADMIN_TOKEN", "test-admin")
	gateway := testGateway(t, nil)
	// The console is served on the admin plane only; the data plane answers
	// with a pointer response instead.
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set("X-Admin-Token", "test-admin")
	gateway.ServeAdminHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d", recorder.Code)
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "Unified API Gateway") {
		t.Fatalf("console not served: %s", body[:min(len(body), 200)])
	}
	if recorder.Header().Get("Content-Security-Policy") == "" {
		t.Fatal("CSP header missing")
	}
	etag := recorder.Header().Get("ETag")
	if etag == "" {
		t.Fatal("ETag missing")
	}
	conditional := httptest.NewRequest(http.MethodGet, "/", nil)
	conditional.Header.Set("X-Admin-Token", "test-admin")
	conditional.Header.Set("If-None-Match", etag)
	second := httptest.NewRecorder()
	gateway.ServeAdminHTTP(second, conditional)
	if second.Code != http.StatusNotModified {
		t.Fatalf("conditional status=%d", second.Code)
	}
	pointer := httptest.NewRecorder()
	gateway.ServeDataHTTP(pointer, httptest.NewRequest(http.MethodGet, "/", nil))
	if !strings.Contains(pointer.Body.String(), "admin_console") {
		t.Fatalf("data plane should point at the admin console, got %s", pointer.Body.String())
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func TestInboundRequestIDValidated(t *testing.T) {
	gateway := testGateway(t, nil)
	cfg := gateway.Snapshot().Config
	cases := []struct {
		name  string
		value string
		valid bool
	}{
		{"accepted", "abcdefgh", true},
		{"uuid-like", "123e4567-e89b-12d3-a456-426614174000", true},
		{"too short", "short", false},
		{"newline injection", "abc\ndefgh", false},
		{"empty", "", false},
	}
	for _, testCase := range cases {
		request := httptest.NewRequest(http.MethodPost, "/anything", nil)
		if testCase.value != "" {
			request.Header.Set("X-Request-Id", testCase.value)
		}
		got := gateway.inboundRequestID(cfg, request)
		if testCase.valid && got != testCase.value {
			t.Errorf("%s: got %q want %q", testCase.name, got, testCase.value)
		}
		if !testCase.valid && got == testCase.value {
			t.Errorf("%s: invalid id %q was echoed back", testCase.name, testCase.value)
		}
	}
}

func TestCustomRequestIDHeaderHonoured(t *testing.T) {
	gateway := testGateway(t, func(cfg *config.Config) {
		cfg.Observability.RequestIDHeader = "X-Correlation-Id"
	})
	request := httptest.NewRequest(http.MethodPost, "/anything", nil)
	request.Header.Set("X-Correlation-Id", "corr-12345")
	if got := gateway.inboundRequestID(gateway.Snapshot().Config, request); got != "corr-12345" {
		t.Fatalf("got %q", got)
	}
	// The legacy header still works as a fallback.
	request = httptest.NewRequest(http.MethodPost, "/anything", nil)
	request.Header.Set("X-Request-Id", "legacy-123")
	if got := gateway.inboundRequestID(gateway.Snapshot().Config, request); got != "legacy-123" {
		t.Fatalf("fallback got %q", got)
	}
}

func TestConfiguredAuditRedactionsHideExtraHeaders(t *testing.T) {
	headers := http.Header{"X-Trace-Id": []string{"t-1"}, "Accept": []string{"application/json"}}
	cfg := &config.Config{Version: 1, Policies: map[string]any{
		"audit": map[string]any{"enabled": true, "redact": []any{"x-trace-id", "response_body"}},
	}}
	redacted := redactHeadersWith(headers, configuredRedactions(cfg))
	if got := redacted["X-Trace-Id"]; len(got) != 1 || got[0] != "[REDACTED]" {
		t.Fatalf("X-Trace-Id=%v", got)
	}
	if got := redacted["Accept"]; len(got) != 1 || got[0] != "application/json" {
		t.Fatalf("Accept=%v", got)
	}
	if !auditEnabled(cfg) {
		t.Fatal("audit should be enabled")
	}
}
