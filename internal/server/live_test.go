package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"unified-api-gateway/internal/config"
)

// TestContentLengthMatchesRewrittenBody drives a rewritten response through a
// real http.Server and a real client. ResponseRecorder cannot catch a stale
// Content-Length, but a genuine client aborts with "unexpected EOF" when the
// declared length does not match the rewritten body.
func TestContentLengthMatchesRewrittenBody(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The Go server sets Content-Length for the upstream body
		// automatically; the gateway rewrites the body to a different length.
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":42,"displayName":"Alice","extra":"this field name gets renamed downstream"}`)
	}))
	defer upstream.Close()
	cfg := &config.Config{Version: 1, Upstreams: map[string]config.Upstream{"u": {BaseURL: upstream.URL}}, Profiles: map[string]config.Profile{
		"rewrite": {Name: "rewrite", Adapter: "transparent", TrafficMode: "live", Upstream: "u",
			Response: transformSpec(`
body:
  rename:
    id: user_id
    displayName: name
`)},
	}, Routes: []config.Route{{ID: "users", Match: config.RouteMatch{Path: "/api/v1/users", Methods: []string{"POST"}}, Profile: "rewrite"}}}
	snapshot, err := config.NewSnapshot(cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := New(Options{Snapshot: snapshot})
	if err != nil {
		t.Fatal(err)
	}
	gatewayServer := httptest.NewServer(http.HandlerFunc(gateway.ServeDataHTTP))
	defer gatewayServer.Close()

	response, err := http.Post(gatewayServer.URL+"/api/v1/users", "application/json", strings.NewReader(`{"user_id":7,"name":"Alice"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, readErr := io.ReadAll(response.Body)
	if readErr != nil {
		t.Fatalf("client failed to read rewritten body (stale Content-Length?): %v", readErr)
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("body is not JSON: %s", body)
	}
	if payload["user_id"] != float64(42) || payload["name"] != "Alice" {
		t.Fatalf("rewritten body=%s", body)
	}
}

// TestSameOrigin blocks cross-site browser origins on the WebSocket upgrade
// while allowing non-browser clients that send no Origin at all.
func TestSameOrigin(t *testing.T) {
	cases := []struct {
		origin, host string
		allowed      bool
	}{
		{"", "api.example.com", true},
		{"https://api.example.com", "api.example.com", true},
		{"https://api.example.com:8780", "api.example.com:8780", true},
		{"https://evil.example.net", "api.example.com", false},
		{"https://evil.example.net", "api.example.com:8780", false},
		{"::not a url::", "api.example.com", false},
	}
	for _, testCase := range cases {
		if got := sameOrigin(testCase.origin, testCase.host); got != testCase.allowed {
			t.Errorf("sameOrigin(%q, %q) = %v, want %v", testCase.origin, testCase.host, got, testCase.allowed)
		}
	}
}

// TestUnknownConfigKeysRejected pins the strict-decoding behaviour for the
// typos that used to be silently dropped.
func TestUnknownConfigKeysRejected(t *testing.T) {
	cases := []struct {
		name   string
		config string
		key    string
	}{
		{"top-level", "version: 1\ndefaults_profil: x\n", "defaults_profil"},
		{"profile field", "version: 1\nprofiles:\n  p:\n    stream_sfe: true\n", "stream_sfe"},
		{"allowed methods", "version: 1\nprofiles:\n  p:\n    allowed_method: [POST]\n", "allowed_method"},
		{"upstream field", "version: 1\nupstreams:\n  u:\n    base_ur: http://x.test\n", "base_ur"},
		{"observability", "version: 1\nobservability:\n  log_leve: debug\n", "log_leve"},
		{"transform body key", "version: 1\nprofiles:\n  p:\n    request:\n      body:\n        renmae:\n          a: b\n", "renmae"},
	}
	for _, testCase := range cases {
		_, err := config.Parse([]byte(testCase.config))
		if err == nil {
			t.Errorf("%s: unknown key %q was accepted", testCase.name, testCase.key)
			continue
		}
		if !strings.Contains(err.Error(), testCase.key) {
			t.Errorf("%s: error should name the unknown key %q, got: %v", testCase.name, testCase.key, err)
		}
	}
}
