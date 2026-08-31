package server

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
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
	}, Routes: []config.Route{{ID: "ws", Match: config.RouteMatch{PathPrefix: "/"}, Profile: "ws"}}, Policies: map[string]any{
		"defaults": map[string]any{"circuit_breaker": map[string]any{"failure_threshold": 1, "reset_timeout_ms": 60000}},
	}}
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

func TestWebSocketUpstreamClientHandshakeErrorDoesNotTripBreaker(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer upstream.Close()
	cfg := &config.Config{
		Version:   1,
		Upstreams: map[string]config.Upstream{"u": {BaseURL: upstream.URL}},
		Profiles:  map[string]config.Profile{"runtime-key": {Name: "Display Name", Adapter: "transparent", TrafficMode: "live", Upstream: "u"}},
		Routes:    []config.Route{{ID: "ws", Match: config.RouteMatch{Path: "/socket"}, Profile: "runtime-key"}},
		Policies:  map[string]any{"defaults": map[string]any{"circuit_breaker": map[string]any{"failure_threshold": 1, "reset_timeout_ms": 60000}}},
	}
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
	for attempt := 0; attempt < 2; attempt++ {
		_, response, dialErr := websocket.DefaultDialer.Dial(wsURL, nil)
		if dialErr == nil {
			t.Fatal("4xx upstream unexpectedly completed websocket handshake")
		}
		if response == nil || response.StatusCode != http.StatusBadGateway {
			t.Fatalf("attempt %d response=%v err=%v", attempt+1, response, dialErr)
		}
		response.Body.Close()
	}
	breaker := gateway.runtimeFor("runtime-key").breaker
	breaker.mu.Lock()
	failures, state := breaker.failures, breaker.state
	breaker.mu.Unlock()
	if failures != 0 || state == breakerOpen {
		t.Fatalf("4xx handshake tripped breaker: failures=%d state=%d", failures, state)
	}
}

func TestWebSocketUpstreamServerHandshakeErrorTripsBreaker(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer upstream.Close()
	cfg := &config.Config{
		Version:   1,
		Upstreams: map[string]config.Upstream{"u": {BaseURL: upstream.URL}},
		Profiles:  map[string]config.Profile{"ws": {Name: "ws", Adapter: "transparent", TrafficMode: "live", Upstream: "u"}},
		Routes:    []config.Route{{ID: "ws", Match: config.RouteMatch{Path: "/socket"}, Profile: "ws"}},
		Policies:  map[string]any{"defaults": map[string]any{"circuit_breaker": map[string]any{"failure_threshold": 1, "reset_timeout_ms": 60000}}},
	}
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
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	breaker := gateway.runtimeFor("ws").breaker
	breaker.mu.Lock()
	failures, state := breaker.failures, breaker.state
	breaker.mu.Unlock()
	if failures != 1 || state != breakerOpen {
		t.Fatalf("5xx handshake did not trip breaker: failures=%d state=%d", failures, state)
	}
}

func TestWebSocketWrappedProfileForbiddenReturns403(t *testing.T) {
	cfg := &config.Config{
		Version:   1,
		Upstreams: map[string]config.Upstream{"u": {BaseURL: "http://127.0.0.1:1"}},
		Profiles: map[string]config.Profile{
			"one": {Name: "one", Adapter: "transparent", TrafficMode: "live", Upstream: "u"},
			"two": {Name: "two", Adapter: "transparent", TrafficMode: "live", Upstream: "u"},
		},
		Clients: map[string]config.ClientCredential{
			"client": {Key: "secret", Profiles: []string{"one"}, Upstreams: []string{"u"}},
		},
		Routes: []config.Route{{ID: "ws", Match: config.RouteMatch{Path: "/socket"}, Profile: "one"}},
	}
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
	request.Header.Set("X-API-Key", "secret")
	request.Header.Set("X-Gateway-Profile", "two")
	gateway.ServeDataHTTP(recorder, request)
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestWebSocketConcurrentPingAndDataWrites(t *testing.T) {
	upstreamPong := make(chan struct{}, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		var writerMu sync.Mutex
		conn.SetPongHandler(func(string) error {
			select {
			case upstreamPong <- struct{}{}:
			default:
			}
			return nil
		})
		stopPing := make(chan struct{})
		defer close(stopPing)
		go func() {
			for i := 0; i < 50; i++ {
				select {
				case <-stopPing:
					return
				default:
				}
				writerMu.Lock()
				_ = conn.WriteControl(websocket.PingMessage, []byte("upstream"), time.Now().Add(time.Second))
				writerMu.Unlock()
				time.Sleep(time.Millisecond)
			}
		}()
		for {
			messageType, payload, readErr := conn.ReadMessage()
			if readErr != nil {
				return
			}
			writerMu.Lock()
			writeErr := conn.WriteMessage(messageType, payload)
			writerMu.Unlock()
			if writeErr != nil {
				return
			}
		}
	}))
	defer upstream.Close()
	cfg := &config.Config{
		Version:   1,
		Upstreams: map[string]config.Upstream{"u": {BaseURL: upstream.URL}},
		Profiles:  map[string]config.Profile{"ws": {Name: "ws", Adapter: "transparent", TrafficMode: "live", Upstream: "u"}},
		Routes:    []config.Route{{ID: "ws", Match: config.RouteMatch{Path: "/socket"}, Profile: "ws"}},
	}
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
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(gatewayServer.URL, "http")+"/socket", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	clientPong := make(chan struct{}, 1)
	conn.SetPongHandler(func(string) error {
		select {
		case clientPong <- struct{}{}:
		default:
		}
		return nil
	})
	for i := 0; i < 50; i++ {
		if err := conn.WriteControl(websocket.PingMessage, []byte("client"), time.Now().Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		want := fmt.Sprintf("message-%d", i)
		if err := conn.WriteMessage(websocket.TextMessage, []byte(want)); err != nil {
			t.Fatal(err)
		}
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, payload, err := conn.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
		if string(payload) != want {
			t.Fatalf("echo=%q want=%q", payload, want)
		}
	}
	select {
	case <-clientPong:
	case <-time.After(2 * time.Second):
		t.Fatal("client did not receive a pong through the gateway")
	}
	select {
	case <-upstreamPong:
	case <-time.After(2 * time.Second):
		t.Fatal("upstream did not receive a pong through the gateway")
	}
}

func TestWebSocketAbnormalUpstreamClosureTripsBreaker(t *testing.T) {
	drop := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		<-drop
		_ = conn.UnderlyingConn().Close()
	}))
	defer upstream.Close()
	cfg := &config.Config{
		Version:   1,
		Upstreams: map[string]config.Upstream{"u": {BaseURL: upstream.URL}},
		Profiles:  map[string]config.Profile{"ws": {Name: "ws", Adapter: "transparent", TrafficMode: "live", Upstream: "u"}},
		Routes:    []config.Route{{ID: "ws", Match: config.RouteMatch{Path: "/socket"}, Profile: "ws"}},
		Policies: map[string]any{
			"defaults": map[string]any{
				"circuit_breaker": map[string]any{"failure_threshold": 1, "reset_timeout_ms": 60000},
			},
		},
	}
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
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	close(drop)
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, _, _ = conn.ReadMessage()
	_ = conn.Close()

	deadline := time.Now().Add(2 * time.Second)
	for {
		breaker := gateway.runtimeFor("ws").breaker
		breaker.mu.Lock()
		state := breaker.state
		breaker.mu.Unlock()
		if state == breakerOpen {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("abnormal upstream closure did not open the breaker")
		}
		time.Sleep(10 * time.Millisecond)
	}
	_, response, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err == nil {
		t.Fatal("open breaker unexpectedly allowed another websocket")
	}
	if response == nil || response.StatusCode != http.StatusServiceUnavailable {
		if response != nil {
			defer response.Body.Close()
		}
		t.Fatalf("breaker response=%v err=%v", response, err)
	}
	response.Body.Close()
}

func TestWebSocketAbnormalClosuresAccumulatePastOne(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
		_ = conn.UnderlyingConn().Close()
	}))
	defer upstream.Close()
	cfg := &config.Config{
		Version:   1,
		Upstreams: map[string]config.Upstream{"u": {BaseURL: upstream.URL}},
		Profiles:  map[string]config.Profile{"ws": {Name: "ws", Adapter: "transparent", TrafficMode: "live", Upstream: "u"}},
		Routes:    []config.Route{{ID: "ws", Match: config.RouteMatch{Path: "/socket"}, Profile: "ws"}},
		Policies: map[string]any{
			"defaults": map[string]any{
				"circuit_breaker": map[string]any{"failure_threshold": 2, "reset_timeout_ms": 60000},
			},
		},
	}
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
	for attempt := 1; attempt <= 2; attempt++ {
		conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			t.Fatalf("attempt %d handshake: %v", attempt, err)
		}
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, _, _ = conn.ReadMessage()
		_ = conn.Close()
		deadline := time.Now().Add(2 * time.Second)
		for {
			breaker := gateway.runtimeFor("ws").breaker
			breaker.mu.Lock()
			failures, state := breaker.failures, breaker.state
			breaker.mu.Unlock()
			if failures >= attempt || state == breakerOpen {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("attempt %d was not counted: failures=%d state=%d", attempt, failures, state)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	_, response, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err == nil {
		t.Fatal("two consecutive abnormal sessions did not open the breaker")
	}
	if response == nil || response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("breaker response=%v err=%v", response, err)
	}
	response.Body.Close()
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
