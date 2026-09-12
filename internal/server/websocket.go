package server

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
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
// request's scheme, canonical host, and effective port, so a web page cannot
// drive the gateway's WebSocket plane from another origin. Non-browser clients
// (no Origin header) are always allowed. The variadic scheme keeps the helper
// convenient for older unit callers; production callers always pass the
// request scheme derived from TLS, never from forwarded headers.
func sameOrigin(origin, host string, requestScheme ...string) bool {
	if origin == "" {
		return true
	}
	parsed, err := url.Parse(strings.TrimSpace(origin))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.User != nil || parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return false
	}
	scheme := strings.ToLower(strings.TrimSpace(parsed.Scheme))
	if len(requestScheme) > 0 && strings.TrimSpace(requestScheme[0]) != "" {
		expectedScheme := strings.ToLower(strings.TrimSpace(requestScheme[0]))
		if scheme != expectedScheme {
			return false
		}
	} else {
		// Keep the two-argument helper useful for package tests that do not have
		// an HTTP request. The actual handler passes the request scheme above.
		requestScheme = []string{scheme}
	}
	originAuthority, ok := canonicalWebSocketAuthority(parsed.Host, scheme)
	if !ok {
		return false
	}
	requestAuthority, ok := canonicalWebSocketAuthority(host, requestScheme[0])
	if !ok {
		return false
	}
	return originAuthority == requestAuthority
}

type websocketAuthority struct {
	host string
	port int
}

// canonicalWebSocketAuthority parses both URL authorities and net/http Host
// values. It deliberately only considers the supplied authority: forwarded
// headers are not consulted here or by the caller.
func canonicalWebSocketAuthority(rawHost, scheme string) (websocketAuthority, bool) {
	rawHost = strings.TrimSpace(rawHost)
	if rawHost == "" {
		return websocketAuthority{}, false
	}
	host, portText, hasPort := rawHost, "", false
	if parsedHost, parsedPort, err := net.SplitHostPort(rawHost); err == nil {
		host, portText, hasPort = parsedHost, parsedPort, true
	} else if strings.HasPrefix(rawHost, "[") {
		if !strings.HasSuffix(rawHost, "]") {
			return websocketAuthority{}, false
		}
		host = rawHost[1 : len(rawHost)-1]
	}
	canonicalHost, ok := canonicalWebSocketHost(host)
	if !ok {
		return websocketAuthority{}, false
	}
	port := 0
	if hasPort {
		if portText == "" {
			return websocketAuthority{}, false
		}
		for _, digit := range portText {
			if digit < '0' || digit > '9' {
				return websocketAuthority{}, false
			}
			port = port*10 + int(digit-'0')
			if port > 65535 {
				return websocketAuthority{}, false
			}
		}
	} else {
		switch strings.ToLower(strings.TrimSpace(scheme)) {
		case "http":
			port = 80
		case "https":
			port = 443
		default:
			return websocketAuthority{}, false
		}
	}
	return websocketAuthority{host: canonicalHost, port: port}, true
}

func canonicalWebSocketHost(host string) (string, bool) {
	host = strings.TrimSpace(host)
	if host == "" || strings.ContainsAny(host, "[]/\\ \t\r\n") {
		return "", false
	}
	if addr, err := netip.ParseAddr(host); err == nil {
		return strings.ToLower(addr.String()), true
	}
	// A colon is only valid in an IPv6 literal. Reject malformed authorities
	// such as "example.test:not-a-port" instead of treating the whole value as
	// a DNS hostname.
	if strings.Contains(host, ":") {
		return "", false
	}
	return strings.TrimSuffix(strings.ToLower(host), "."), true
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request, pinned *gatewayState, requestID string) {
	// The caller pinned the state for this request; reusing it guarantees
	// the whole connection lifetime sees one configuration version.
	snapshot := pinned.snapshot
	// handleProxy already opened the metrics span; this handler only adds
	// the terminal observation with the real outcome (a failed dial must
	// not be recorded as a successful upgrade).
	started := time.Now()
	var profile *config.Profile
	var selectedRoute *config.Route
	upstreamName := ""
	status := http.StatusSwitchingProtocols
	stage, code := "", ""
	defer func() {
		s.observe(snapshot.Config, requestID, started, r, selectedRoute, profile, upstreamName, status, stage, code)
	}()

	cfg := snapshot.Config
	match := route.FromHTTPRequest(r)
	match.ClientID, match.Tenant = s.authenticateClient(cfg, r)
	match.Client = match.ClientID
	if (requiresClientAuth(cfg) || pinned.policy.requireClientAuth) && match.ClientID == "" {
		status = http.StatusUnauthorized
		s.writeError(w, status, "auth", "client_auth_required", "a valid client key is required", requestID)
		return
	}
	match.Profile = strings.TrimSpace(r.Header.Get("X-Gateway-Profile"))
	if match.Profile != "" && !requiresClientAuth(cfg) && match.ClientID == "" {
		if !pinned.allowsAnonymousOverride() {
			status = http.StatusForbidden
			s.writeError(w, status, "auth", "profile_override_denied", "anonymous profile overrides are disabled by policy", requestID)
			return
		}
		match.ClientID = "__anonymous__"
		match.Client = "__anonymous__"
	}
	var err error
	profile, selectedRoute, err = route.SelectProfile(cfg, match)
	if err != nil {
		status = http.StatusNotFound
		if errors.Is(err, route.ErrProfileForbidden) {
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
	if len(profile.AllowedMethods) > 0 && !methodAllowed(profile.AllowedMethods, r.Method) {
		status = http.StatusMethodNotAllowed
		s.writeError(w, status, "route", "method_not_allowed", "profile "+profile.Name+" does not allow "+r.Method, requestID)
		return
	}
	// Reject cross-site browser upgrades before touching the upstream; the
	// gorilla upgrader's own check would only run after a successful dial.
	if !sameOrigin(r.Header.Get("Origin"), r.Host, requestScheme(r)) {
		status = http.StatusForbidden
		s.writeError(w, status, "auth", "origin_rejected", "cross-origin WebSocket upgrades are not allowed", requestID)
		return
	}
	if rejectionCode, message := transparentPlaneSupported(*profile); rejectionCode != "" {
		status, stage, code = http.StatusNotImplemented, "websocket", rejectionCode
		s.writeError(w, status, stage, code, message, requestID)
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
	rt := pinned.runtimes[profileRuntimeID(*profile)]
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
		s.writeError(w, status, "upstream", "websocket_upstream_invalid", "websocket upstream URL is invalid", requestID)
		return
	}
	base.Scheme = websocketScheme(base.Scheme)
	base.Path = joinURLPath(base.Path, r.URL.Path)
	base.RawQuery = joinRawQuery(base.RawQuery, r.URL.RawQuery)
	requestHeaders := filteredRequestHeaders(r.Header)
	applyUpstreamAuth(requestHeaders, cfg, upstream)
	requestHeaders.Set("X-Gateway-Request-Id", requestID)
	handshakeTimeout := upstream.EffectiveTimeout()
	if handshakeTimeout <= 0 {
		handshakeTimeout = streamResponseHeaderTimeout
	}
	dialer := websocket.Dialer{Subprotocols: requestedSubprotocols(r.Header), HandshakeTimeout: handshakeTimeout}
	if upstream.Insecure {
		dialer.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // explicit opt-in per upstream
	}
	upstreamConn, upstreamResponse, err := dialer.DialContext(r.Context(), base.String(), requestHeaders)
	if err != nil {
		message := "websocket upstream connection failed"
		if upstreamResponse != nil {
			message = fmt.Sprintf("upstream returned %s", upstreamResponse.Status)
			if upstreamResponse.Body != nil {
				_, _ = io.Copy(io.Discard, upstreamResponse.Body)
				_ = upstreamResponse.Body.Close()
			}
		}
		rt.recordUpstreamResult(websocketHandshakeSucceeded(upstreamResponse))
		status = http.StatusBadGateway
		stage, code = "upstream", "websocket_dial_failed"
		s.writeError(w, status, "upstream", "websocket_dial_failed", message, requestID)
		return
	}
	defer upstreamConn.Close()
	negotiated := []string{}
	if upstreamResponse != nil {
		negotiated = upstreamResponse.Header.Values("Sec-WebSocket-Protocol")
	}
	upgrader := websocket.Upgrader{
		ReadBufferSize:  32 << 10,
		WriteBufferSize: 32 << 10,
		Subprotocols:    negotiated,
		CheckOrigin: func(request *http.Request) bool {
			return sameOrigin(request.Header.Get("Origin"), request.Host, requestScheme(request))
		},
	}
	clientConn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		// Upgrade already wrote the error response.
		status = http.StatusInternalServerError
		stage, code = "client", "websocket_upgrade_failed"
		return
	}
	defer clientConn.Close()
	// Resolve a half-open probe after both handshakes, but do not clear
	// closed-state failure history until the connection itself ends cleanly.
	rt.breaker.recordProbeSuccess()
	wsReadLimit := defaultMaxBody
	if cfg.Limits.MaxBodyBytes > 0 {
		wsReadLimit = minPositiveLimit(wsReadLimit, cfg.Limits.MaxBodyBytes)
	}
	if profile.MaxBodyBytes > 0 {
		wsReadLimit = minPositiveLimit(wsReadLimit, profile.MaxBodyBytes)
	}
	const wsIdleTimeout = 10 * time.Minute
	clientConn.SetReadLimit(wsReadLimit)
	upstreamConn.SetReadLimit(wsReadLimit)
	// Each connection's read deadline is only touched by its own read loop
	// (and pings on that connection), avoiding cross-connection writes.
	refreshRead := func(conn *websocket.Conn) {
		_ = conn.SetReadDeadline(time.Now().Add(wsIdleTimeout))
	}
	refreshRead(clientConn)
	refreshRead(upstreamConn)
	clientWriter := &lockedWebSocketWriter{conn: clientConn}
	upstreamWriter := &lockedWebSocketWriter{conn: upstreamConn}
	// Ping handlers answer with a pong (peers use it for liveness) and
	// refresh that connection's own idle deadline. Control and data writes
	// share one lock per connection.
	clientConn.SetPongHandler(func(string) error {
		refreshRead(clientConn)
		return nil
	})
	upstreamConn.SetPongHandler(func(string) error {
		refreshRead(upstreamConn)
		return nil
	})
	clientConn.SetPingHandler(func(appData string) error {
		refreshRead(clientConn)
		return clientWriter.writeControl(websocket.PongMessage, []byte(appData))
	})
	upstreamConn.SetPingHandler(func(appData string) error {
		refreshRead(upstreamConn)
		return upstreamWriter.writeControl(websocket.PongMessage, []byte(appData))
	})

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	var once sync.Once
	closeBoth := func() { once.Do(func() { _ = clientConn.Close(); _ = upstreamConn.Close(); cancel() }) }
	type wsCopyResult struct {
		upstreamFault bool
		err           error
	}
	results := make(chan wsCopyResult, 2)
	copyFrames := func(dstWriter *lockedWebSocketWriter, src *websocket.Conn, sourceIsUpstream bool) {
		var result wsCopyResult
		for {
			messageType, payload, readErr := src.ReadMessage()
			if readErr != nil {
				result.err = readErr
				result.upstreamFault = sourceIsUpstream
				break
			}
			refreshRead(src)
			if writeErr := dstWriter.writeMessage(messageType, payload); writeErr != nil {
				result.err = writeErr
				result.upstreamFault = !sourceIsUpstream
				break
			}
		}
		results <- result
		closeBoth()
	}
	go copyFrames(upstreamWriter, clientConn, false)
	go copyFrames(clientWriter, upstreamConn, true)
	result := <-results
	if result.upstreamFault && !normalWSClosure(result.err) {
		rt.recordUpstreamResult(false)
		stage, code = "upstream", "websocket_connection_failed"
	} else if normalWSClosure(result.err) {
		rt.recordUpstreamResult(true)
	}
	<-ctx.Done()
}

// lockedWebSocketWriter serializes data/control writes on one connection.
type lockedWebSocketWriter struct {
	conn *websocket.Conn
	mu   sync.Mutex
}

func (w *lockedWebSocketWriter) writeMessage(messageType int, payload []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.conn.SetWriteDeadline(time.Now().Add(30 * time.Second)); err != nil {
		return err
	}
	return w.conn.WriteMessage(messageType, payload)
}

func (w *lockedWebSocketWriter) writeControl(messageType int, payload []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.conn.WriteControl(messageType, payload, time.Now().Add(30*time.Second))
}

func normalWSClosure(err error) bool {
	if err == nil || errors.Is(err, net.ErrClosed) || errors.Is(err, context.Canceled) {
		return true
	}
	return websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway)
}

// websocketHandshakeSucceeded classifies a failed upstream handshake using
// the same status boundary as the HTTP and gRPC paths. Caller/business errors
// (4xx) are successful from an upstream-availability perspective; 5xx
// responses and missing responses (network, TLS, or dial errors) count as
// failures.
func websocketHandshakeSucceeded(response *http.Response) bool {
	return response != nil && response.StatusCode < http.StatusInternalServerError
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
