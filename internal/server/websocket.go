package server

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/gorilla/websocket"
	"unified-api-gateway/internal/config"
	"unified-api-gateway/internal/route"
)

func isWebSocketUpgrade(r *http.Request) bool {
	return strings.EqualFold(r.Header.Get("Upgrade"), "websocket") && strings.Contains(strings.ToLower(r.Header.Get("Connection")), "upgrade")
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request, snapshot *config.ConfigSnapshot, requestID string) {
	cfg := snapshot.Config
	match := route.FromHTTPRequest(r)
	match.ClientID, match.Tenant = s.authenticateClient(cfg, r)
	match.Client = match.ClientID
	if requiresClientAuth(cfg) && match.ClientID == "" {
		s.writeError(w, http.StatusUnauthorized, "auth", "client_auth_required", "a valid client key is required", requestID)
		return
	}
	if strings.TrimSpace(r.Header.Get("X-Gateway-Profile")) != "" && !requiresClientAuth(cfg) && match.ClientID == "" {
		match.ClientID = "__anonymous__"
		match.Client = "__anonymous__"
	}
	match.Profile = strings.TrimSpace(r.Header.Get("X-Gateway-Profile"))
	profile, selectedRoute, err := route.SelectProfile(cfg, match)
	if err != nil {
		status := http.StatusNotFound
		if err == route.ErrProfileForbidden {
			status = http.StatusForbidden
		}
		s.writeError(w, status, "route", "websocket_profile_failed", err.Error(), requestID)
		return
	}
	if profile == nil || strings.EqualFold(profile.Adapter, "") {
		s.writeError(w, http.StatusNotFound, "route", "websocket_profile_missing", "no websocket profile selected", requestID)
		return
	}
	if !strings.EqualFold(profile.Adapter, "transparent") {
		s.writeError(w, http.StatusNotImplemented, "adapter", "websocket_adapter_required", "message-level WebSocket transforms require a dedicated adapter", requestID)
		return
	}
	upstream, ok := cfg.Upstreams[profile.Upstream]
	if !ok {
		s.writeError(w, http.StatusBadGateway, "upstream", "websocket_upstream_missing", "websocket upstream is not configured", requestID)
		return
	}
	base, err := url.Parse(upstream.EffectiveURL())
	if err != nil {
		s.writeError(w, http.StatusBadGateway, "upstream", "websocket_upstream_invalid", err.Error(), requestID)
		return
	}
	base.Scheme = websocketScheme(base.Scheme)
	base.Path = joinURLPath(base.Path, r.URL.Path)
	base.RawQuery = r.URL.RawQuery
	requestHeaders := filteredRequestHeaders(r.Header)
	for key, value := range upstream.Headers {
		requestHeaders.Set(key, resolveSecretValue(value))
	}
	requestHeaders.Set("X-Gateway-Request-Id", requestID)
	dialer := websocket.Dialer{}
	upstreamConn, upstreamResponse, err := dialer.DialContext(r.Context(), base.String(), requestHeaders)
	if err != nil {
		message := err.Error()
		if upstreamResponse != nil {
			message = fmt.Sprintf("upstream returned %s", upstreamResponse.Status)
		}
		s.writeError(w, http.StatusBadGateway, "upstream", "websocket_dial_failed", message, requestID)
		return
	}
	defer upstreamConn.Close()
	upgrader := websocket.Upgrader{
		ReadBufferSize:  32 << 10,
		WriteBufferSize: 32 << 10,
		CheckOrigin:     func(_ *http.Request) bool { return true },
	}
	clientConn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer clientConn.Close()

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
			if writeErr := dst.WriteMessage(messageType, payload); writeErr != nil {
				return
			}
		}
	}
	go copyFrames(upstreamConn, clientConn)
	go copyFrames(clientConn, upstreamConn)
	<-ctx.Done()
	_ = selectedRoute
}

func websocketScheme(scheme string) string {
	switch strings.ToLower(scheme) {
	case "https", "wss":
		return "wss"
	default:
		return "ws"
	}
}
