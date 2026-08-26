package server

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"unified-api-gateway/internal/config"
)

func grpcGateway(t *testing.T, upstreamURL string, maxBody int64, policy map[string]any) *Server {
	t.Helper()
	cfg := &config.Config{
		Version:   1,
		Upstreams: map[string]config.Upstream{"u": {BaseURL: upstreamURL}},
		Profiles: map[string]config.Profile{
			"p": {Name: "p", Adapter: "transparent", TrafficMode: "live", Upstream: "u", MaxBodyBytes: maxBody},
		},
		Routes:   []config.Route{{ID: "grpc", Match: config.RouteMatch{PathPrefix: "/"}, Profile: "p"}},
		Policies: policy,
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

func TestGRPCProfileLimitAppliesToChunkedRequestAndResponse(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/grpc")
		_, _ = w.Write([]byte("response-too-large"))
	}))
	defer upstream.Close()
	gateway := grpcGateway(t, upstream.URL, 8, nil)

	request := httptest.NewRequest(http.MethodPost, "/pkg.Service/Call", bytes.NewReader([]byte("request-too-large")))
	request.ContentLength = -1
	request.Header.Set("Content-Type", "application/grpc")
	recorder := httptest.NewRecorder()
	gateway.ServeGRPCHTTP(recorder, request)
	if recorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("chunked request status=%d body=%s", recorder.Code, recorder.Body.String())
	}

	request = httptest.NewRequest(http.MethodPost, "/pkg.Service/Call", strings.NewReader("ok"))
	request.Header.Set("Content-Type", "application/grpc")
	recorder = httptest.NewRecorder()
	gateway.ServeGRPCHTTP(recorder, request)
	if recorder.Code != http.StatusBadGateway || !strings.Contains(recorder.Body.String(), "response_too_large") {
		t.Fatalf("large response status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestGRPCTrailerStatusForwardedAndTripsBreaker(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/grpc")
		w.Header().Set("Trailer", "Grpc-Status, Grpc-Message")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte{0, 0, 0, 0, 0})
		w.Header().Set("Grpc-Status", "13")
		w.Header().Set("Grpc-Message", "internal")
	}))
	defer upstream.Close()
	gateway := grpcGateway(t, upstream.URL, 1024, map[string]any{
		"defaults": map[string]any{
			"circuit_breaker": map[string]any{"failure_threshold": 1, "reset_timeout_ms": 60000},
		},
	})

	call := func() *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPost, "/pkg.Service/Call", strings.NewReader("ok"))
		request.Header.Set("Content-Type", "application/grpc")
		recorder := httptest.NewRecorder()
		gateway.ServeGRPCHTTP(recorder, request)
		return recorder
	}
	first := call()
	if first.Code != http.StatusOK {
		t.Fatalf("first status=%d body=%s", first.Code, first.Body.String())
	}
	result := first.Result()
	if got := result.Trailer.Get("Grpc-Status"); got != "13" {
		t.Fatalf("grpc-status trailer=%q headers=%v trailers=%v", got, result.Header, result.Trailer)
	}
	second := call()
	if second.Code != http.StatusServiceUnavailable || !strings.Contains(second.Body.String(), "circuit_open") {
		t.Fatalf("breaker status=%d body=%s", second.Code, second.Body.String())
	}
}

func TestGRPCEarlyRejectionIsObserved(t *testing.T) {
	gateway := grpcGateway(t, "http://127.0.0.1:1", 1024, nil)
	recorder := httptest.NewRecorder()
	gateway.ServeGRPCHTTP(recorder, httptest.NewRequest(http.MethodPost, "/pkg.Service/Call", nil))
	if recorder.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("status=%d", recorder.Code)
	}
	metrics := gateway.Metrics()
	if metrics.RequestsTotal != 1 || metrics.RequestsFailed != 1 || metrics.Active != 0 {
		t.Fatalf("metrics=%+v", metrics)
	}
}

func TestGRPCClientStatusDoesNotTripBreaker(t *testing.T) {
	calls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Content-Type", "application/grpc")
		w.Header().Set("Grpc-Status", "3") // INVALID_ARGUMENT: caller fault
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()
	gateway := grpcGateway(t, upstream.URL, 1024, map[string]any{
		"defaults": map[string]any{
			"circuit_breaker": map[string]any{"failure_threshold": 1, "reset_timeout_ms": 60000},
		},
	})
	for i := 0; i < 2; i++ {
		request := httptest.NewRequest(http.MethodPost, "/pkg.Service/Call", strings.NewReader("bad request"))
		request.Header.Set("Content-Type", "application/grpc")
		recorder := httptest.NewRecorder()
		gateway.ServeGRPCHTTP(recorder, request)
		if recorder.Code != http.StatusOK || strings.Contains(recorder.Body.String(), "circuit_open") {
			t.Fatalf("call %d status=%d body=%s", i, recorder.Code, recorder.Body.String())
		}
	}
	if calls != 2 {
		t.Fatalf("client error tripped shared breaker, upstream calls=%d", calls)
	}
}

func TestGRPCBreakerStatusClassification(t *testing.T) {
	for _, status := range []string{"", "0", "1", "3", "5", "6", "7", "8", "9", "10", "11", "12", "16"} {
		if grpcStatusTripsBreaker(status) {
			t.Errorf("caller/business gRPC status %q must not trip the shared breaker", status)
		}
	}
	for _, status := range []string{"2", "4", "13", "14", "15"} {
		if !grpcStatusTripsBreaker(status) {
			t.Errorf("upstream availability gRPC status %q must trip the breaker", status)
		}
	}
}
