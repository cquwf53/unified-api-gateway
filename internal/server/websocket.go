package server

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"unified-api-gateway/internal/config"
	"unified-api-gateway/internal/route"
)

func isWebSocketUpgrade(r *http.Request) bool {
	return strings.EqualFold(r.Header.Get("Upgrade"), "websocket") && strings.Contains(strings.ToLower(r.Header.Get("Connection")), "upgrade")
}

// sameOrigin allows browser connections only when the Origin matches the
// request host, so a web page cannot drive the gateway's WebSocket plane from
// another site. Non-browser clients (no Origin header) are always allowed.
func sameOrigin(origin, host string) bool {
	if origin == "" {
		return true
	}
	parsed, err := url.Parse(origin)
	if err != nil {
		return false
	}
	return strings.EqualFold(parsed.Host, host)
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request, snapshot *config.ConfigSnapshot, requestID string) {
	// handleProxy already opened the metrics span; this handler only adds
	// the terminal observation with the real outcome (a failed dial must
	// not be recorded as a successful upgrade).
	started := time.Now()
	var profile *config.Profile
	var selectedRoute *config.Route
	upstreamName := ""
	status := http.StatusSwitchingProtocols
	defer func() {
		s.observe(requestID, started, r, selectedRoute, profile, upstreamName, status, "", "")
	}()

	cfg := snapshot.Config
	match := route.FromHTTPRequest(r)
	match.ClientID, match.Tenant = s.authenticateClient(cfg, r)
	match.Client = match.ClientID
	if requiresClientAuth(cfg) && match.ClientID == "" {
		status = http.StatusUnauthorized
		s.writeError(w, status, "auth", "client_auth_required", "a valid client key is required", requestID)
		return
	}
	if strings.TrimSpace(r.Header.Get("X-Gateway-Profile")) != "" && !requiresClientAuth(cfg) && match.ClientID == "" {
		match.ClientID = "__anonymous__"
		match.Client = "__anonymous__"
	}
	match.Profile = strings.TrimSpace(r.Header.Get("X-Gateway-Profile"))
	var err error
	profile, selectedRoute, err = route.SelectProfile(cfg, match)
	if err != nil {
		status = http.StatusNotFound
		if err == route.ErrProfileForbidden {
			status = http.StatusForbidden
		}
		s.writeError(w, status, "route", "websocket_profile_failed", err.Error(), requestID)
		return
	}
	if profile == nil || strings.EqualFold(profile.Adapter, "") {
		status = http.StatusNotFound
		s.writeError(w, status, "route", "websocket_profile_missing", "no websocket profile selected", requestID)
		return
	}
	if !strings.EqualFold(profile.Adapter, "transparent") {
		status = http.StatusNotImplemented
		s.writeError(w, status, "adapter", "websocket_adapter_required", "message-level WebSocket transforms require a dedicated adapter", requestID)
		return
	}
	// Reject cross-site browser upgrades before touching the upstream; the
	// gorilla upgrader's own check would only run after a successful dial.
	if !sameOrigin(r.Header.Get("Origin"), r.Host) {
		status = http.StatusForbidden
		s.writeError(w, status, "auth", "origin_rejected", "cross-origin WebSocket upgrades are not allowed", requestID)
		return
	}
	upstream, ok := cfg.Upstreams[profile.Upstream]
	if !ok {
		status = http.StatusBadGateway
		s.writeError(w, status, "upstream", "websocket_upstream_missing", "websocket upstream is not configured", requestID)
		return
	}
	upstreamName = profile.Upstream

	// Long-lived WebSocket connections use a dedicated budget instead of the
	// per-profile HTTP semaphore: a pile of idle sockets must not starve
	// request concurrency. Rate limiting and the breaker still apply at
	// establishment time.
	rt := s.runtimeFor(profile.Name)
	release, rejection := rt.beginWebsocketPolicy(r.Context(), r.Method, r.Header, s.wsSlots())
	if rejection != nil {
		if rejection.retryAfter > 0 {
			w.Header().Set("Retry-After", fmt.Sprintf("%.0f", rejection.retryAfter.Seconds()))
		}
		status = rejection.status
		s.writeError(w, rejection.status, "policy", rejection.code, "request refused by traffic policy: "+rejection.code, requestID)
		return
	}
	defer release()

	base, err := url.Parse(upstream.EffectiveURL())
	if err != nil {
		status = http.StatusBadGateway
		s.writeError(w, status, "upstream", "websocket_upstream_invalid", err.Error(), requestID)
		return
	}
	base.Scheme = websocketScheme(base.Scheme)
	base.Path = joinURLPath(base.Path, r.URL.Path)
	base.RawQuery = r.URL.RawQuery
	requestHeaders := filteredRequestHeaders(r.Header)
	applyUpstreamAuth(requestHeaders, cfg, upstream)
	requestHeaders.Set("X-Gateway-Request-Id", requestID)
	dialer := websocket.Dialer{Subprotocols: requestedSubprotocols(r.Header)}
	upstreamConn, upstreamResponse, err := dialer.DialContext(r.Context(), base.String(), requestHeaders)
	if err != nil {
		message := err.Error()
		if upstreamResponse != nil {
			message = fmt.Sprintf("upstream returned %s", upstreamResponse.Status)
		}
		rt.recordUpstreamResult(false)
		status = http.StatusBadGateway
		s.writeError(w, status, "upstream", "websocket_dial_failed", message, requestID)
		return
	}
	rt.recordUpstreamResult(true)
	defer upstreamConn.Close()
	negotiated := []string{}
	if upstreamResponse != nil {
		negotiated = upstreamResponse.Header.Values("Sec-WebSocket-Protocol")
	}
	upgrader := websocket.Upgrader{
		ReadBufferSize:  32 << 10,
		WriteBufferSize: 32 << 10,
		Subprotocols:    negotiated,
		CheckOrigin:     func(request *http.Request) bool { return sameOrigin(request.Header.Get("Origin"), request.Host) },
	}
	clientConn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		// Upgrade already wrote the error response.
		status = http.StatusInternalServerError
		return
	}
	defer clientConn.Close()
	const wsReadLimit = 8 << 20
	const wsIdleTimeout = 10 * time.Minute
	clientConn.SetReadLimit(wsReadLimit)
	upstreamConn.SetReadLimit(wsReadLimit)
	refreshDeadlines := func() {
		deadline := time.Now().Add(wsIdleTimeout)
		_ = clientConn.SetReadDeadline(deadline)
		_ = upstreamConn.SetReadDeadline(deadline)
	}
	refreshDeadlines()
	clientConn.SetPingHandler(func(string) error { refreshDeadlines(); return nil })
	upstreamConn.SetPingHandler(func(string) error { refreshDeadlines(); return nil })

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	var once sync.Once
	closeBoth := func() { once.Do(func() { _ = clientConn.Close(); _ = upstreamConn.Close(); cancel() }) }
	copyFrames := func(dst, src *websocket.Conn) {
		defer closeBoth()
		for {
			messageType, payload, readErr := src.ReadMessage()
			if readErr != nil {
				return
			}
			refreshDeadlines()
			if writeErr := dst.WriteMessage(messageType, payload); writeErr != nil {
				return
			}
		}
	}
	go copyFrames(upstreamConn, clientConn)
	go copyFrames(clientConn, upstreamConn)
	<-ctx.Done()
}

func websocketScheme(scheme string) string {
	switch strings.ToLower(scheme) {
	case "https", "wss":
		return "wss"
	default:
		return "ws"
	}
}

// requestedSubprotocols collects the client's Sec-WebSocket-Protocol values
// (gorilla expects them on the Dialer rather than in the header map, which
// the outbound sanitizer strips).
func requestedSubprotocols(header http.Header) []string {
	var protocols []string
	for _, value := range header.Values("Sec-WebSocket-Protocol") {
		for _, part := range strings.Split(value, ",") {
			if trimmed := strings.TrimSpace(part); trimmed != "" {
				protocols = append(protocols, trimmed)
			}
		}
	}
	return protocols
}
