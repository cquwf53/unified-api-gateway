package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"unified-api-gateway/internal/config"
	"unified-api-gateway/internal/telemetry"
)

// TestWebSocketProxyRoundTrip drives a real WebSocket client through the
// gateway into a real echoing upstream. Before the Sec-WebSocket-* headers
// were stripped from the outbound request every upgrade failed with 502
// because gorilla refuses a caller-supplied handshake.
func TestWebSocketProxyRoundTrip(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			messageType, message, err := conn.ReadMessage()
			if err != nil {
				return
			}
			if err := conn.WriteMessage(messageType, message); err != nil {
				return
			}
		}
	}))
	defer upstream.Close()
	cfg := &config.Config{Version: 1, Upstreams: map[string]config.Upstream{"u": {BaseURL: upstream.URL}}, Profiles: map[string]config.Profile{
		"ws": {Name: "ws", Adapter: "transparent", TrafficMode: "live", Upstream: "u"},
	}, Routes: []config.Route{{ID: "ws", Match: config.RouteMatch{PathPrefix: "/"}, Profile: "ws"}}}
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

	wsURL := "ws" + strings.TrimPrefix(gatewayServer.URL, "http") + "/socket"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, http.Header{"Sec-WebSocket-Protocol": {"chat.v1"}})
	if err != nil {
		t.Fatalf("upgrade through gateway failed: %v", err)
	}
	defer conn.Close()
	if err := conn.WriteMessage(websocket.TextMessage, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, message, err := conn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if string(message) != "hello" {
		t.Fatalf("echo mismatch: %s", message)
	}
	_ = conn.Close()

	// The upgrade (101) must be counted as a successful request, not a
	// failure. Observe runs when the handler exits, so poll briefly.
	deadline := time.Now().Add(2 * time.Second)
	for {
		snapshot := gateway.Metrics()
		if snapshot.RequestsOK == 1 && snapshot.RequestsFailed == 0 && snapshot.RequestsTotal == 1 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("metrics=%+v", snapshot)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestWebSocketUpstreamFailureCountsAsFailure ensures a failed dial is not
// recorded as a successful 101 upgrade.
func TestWebSocketUpstreamFailureCountsAsFailure(t *testing.T) {
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	deadURL := dead.URL
	dead.Close()
	cfg := &config.Config{Version: 1, Upstreams: map[string]config.Upstream{"u": {BaseURL: deadURL}}, Profiles: map[string]config.Profile{
		"ws": {Name: "ws", Adapter: "transparent", TrafficMode: "live", Upstream: "u"},
	}, Routes: []config.Route{{ID: "ws", Match: config.RouteMatch{PathPrefix: "/"}, Profile: "ws"}}}
	snapshot, err := config.NewSnapshot(cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := New(Options{Snapshot: snapshot})
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/socket", nil)
	request.Header.Set("Upgrade", "websocket")
	request.Header.Set("Connection", "Upgrade")
	request.Header.Set("Sec-WebSocket-Version", "13")
	request.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	gateway.ServeDataHTTP(recorder, request)
	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("status=%d", recorder.Code)
	}
	metrics := gateway.Metrics()
	if metrics.RequestsFailed != 1 || metrics.RequestsTotal != 1 {
		t.Fatalf("metrics=%+v", metrics)
	}
}

// TestMetricsTotalEqualsOkPlusFailed fires early-rejected and successful
// requests and pins the counter invariant (every Begin is paired with an
// Observe).
func TestMetricsTotalEqualsOkPlusFailed(t *testing.T) {
	cfg := &config.Config{Version: 1, Profiles: map[string]config.Profile{
		"p": {Name: "p", Adapter: "transparent", TrafficMode: "dry-run", AllowedMethods: []string{"POST"}},
	}, Routes: []config.Route{{ID: "r", Match: config.RouteMatch{Path: "/thing"}, Profile: "p"}}}
	snapshot, err := config.NewSnapshot(cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := New(Options{Snapshot: snapshot})
	if err != nil {
		t.Fatal(err)
	}
	requests := []struct {
		method, path string
	}{
		{http.MethodDelete, "/thing"}, // 405
		{http.MethodPost, "/nope"},    // 404
		{http.MethodPost, "/thing"},   // 200 (dry-run)
		{http.MethodGet, "/nope"},     // 404
	}
	for _, request := range requests {
		recorder := httptest.NewRecorder()
		gateway.ServeDataHTTP(recorder, httptest.NewRequest(request.method, request.path, nil))
	}
	var metrics telemetry.Snapshot
	deadline := time.Now().Add(2 * time.Second)
	for {
		metrics = gateway.Metrics()
		if metrics.RequestsTotal == uint64(len(requests)) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("metrics=%+v", metrics)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if metrics.RequestsTotal != metrics.RequestsOK+metrics.RequestsFailed {
		t.Fatalf("total(%d) != ok(%d)+failed(%d)", metrics.RequestsTotal, metrics.RequestsOK, metrics.RequestsFailed)
	}
	if metrics.RequestsOK != 1 || metrics.RequestsFailed != 3 {
		t.Fatalf("ok=%d failed=%d, want 1/3", metrics.RequestsOK, metrics.RequestsFailed)
	}
}

// TestGrpcMetricsRecordUpstreamFailure pins that the gRPC plane reports the
// real upstream status instead of a frozen http.StatusOK.
func TestGrpcMetricsRecordUpstreamFailure(t *testing.T) {
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	deadURL := dead.URL
	dead.Close()
	cfg := &config.Config{Version: 1, Upstreams: map[string]config.Upstream{"u": {BaseURL: deadURL}}, Profiles: map[string]config.Profile{
		"p": {Name: "p", Adapter: "transparent", TrafficMode: "live", Upstream: "u"},
	}, Routes: []config.Route{{ID: "r", Match: config.RouteMatch{PathPrefix: "/"}, Profile: "p"}}}
	snapshot, err := config.NewSnapshot(cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := New(Options{Snapshot: snapshot})
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/hellow.Greeter/SayHello", nil)
	request.Header.Set("Content-Type", "application/grpc")
	gateway.ServeGRPCHTTP(recorder, request)
	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("status=%d", recorder.Code)
	}
	if metrics := gateway.Metrics(); metrics.RequestsTotal != 1 || metrics.RequestsFailed != 1 {
		t.Fatalf("metrics=%+v, want one recorded failure", metrics)
	}
}
