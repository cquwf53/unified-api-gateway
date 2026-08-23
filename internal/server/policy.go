package server

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
	"unified-api-gateway/internal/config"
	"unified-api-gateway/internal/transform"
)

// This file implements the traffic policies declared in configuration:
// request rate limiting, per-profile concurrency caps, per-upstream circuit
// breaking and retry for idempotent requests. Everything is dependency-free
// and safe for concurrent use.

// policyRejection describes why a request was refused before reaching the
// upstream so the data plane can answer with the right status and headers.
type policyRejection struct {
	status     int
	code       string
	retryAfter time.Duration
}

// policySet is the decoded, ready-to-use form of policies.defaults plus the
// top-level limits block.
type policySet struct {
	requestTimeout time.Duration
	maxConcurrency int
	ratePerSecond  float64
	burst          int
	retryAttempts  int
	idempotentOnly bool
	breakerCfg     breakerConfig
}

type breakerConfig struct {
	failureThreshold int
	resetTimeout     time.Duration
}

func defaultPolicySet() policySet {
	return policySet{retryAttempts: 1, idempotentOnly: true}
}

// decodePolicySet merges policies.defaults (a plain map in config) with the
// limits block. The YAML round-trip keeps config.Decode free of policy
// internals and tolerates unknown policy keys.
func decodePolicySet(cfg *config.Config) policySet {
	set := defaultPolicySet()
	if cfg == nil {
		return set
	}
	if cfg.Limits.RequestsPerSec > 0 {
		set.ratePerSecond = cfg.Limits.RequestsPerSec
		set.burst = cfg.Limits.Burst
		if set.burst <= 0 {
			set.burst = int(cfg.Limits.RequestsPerSec)
		}
	}
	if cfg.Limits.Concurrency > 0 {
		set.maxConcurrency = cfg.Limits.Concurrency
	}
	if len(cfg.Policies) == 0 {
		return set
	}
	raw, err := yaml.Marshal(cfg.Policies)
	if err != nil {
		return set
	}
	var structured struct {
		Defaults struct {
			RequestTimeoutMS  int     `yaml:"request_timeout_ms"`
			MaxConcurrency    int     `yaml:"max_concurrency"`
			RequestsPerSecond float64 `yaml:"requests_per_second"`
			Burst             int     `yaml:"burst"`
			Retry             struct {
				MaxAttempts    int   `yaml:"max_attempts"`
				IdempotentOnly *bool `yaml:"idempotent_only"`
			} `yaml:"retry"`
			CircuitBreaker struct {
				FailureThreshold int   `yaml:"failure_threshold"`
				ResetTimeoutMS   int64 `yaml:"reset_timeout_ms"`
			} `yaml:"circuit_breaker"`
		} `yaml:"defaults"`
	}
	if err := yaml.Unmarshal(raw, &structured); err != nil {
		return set
	}
	d := structured.Defaults
	if d.RequestTimeoutMS > 0 {
		set.requestTimeout = time.Duration(d.RequestTimeoutMS) * time.Millisecond
	}
	if d.MaxConcurrency > 0 {
		set.maxConcurrency = d.MaxConcurrency
	}
	if d.RequestsPerSecond > 0 {
		set.ratePerSecond = d.RequestsPerSecond
		if d.Burst > 0 {
			set.burst = d.Burst
		} else if set.burst == 0 {
			set.burst = int(d.RequestsPerSecond)
		}
	}
	if d.Retry.MaxAttempts > 0 {
		set.retryAttempts = d.Retry.MaxAttempts
	}
	if d.Retry.IdempotentOnly != nil {
		set.idempotentOnly = *d.Retry.IdempotentOnly
	}
	if d.CircuitBreaker.FailureThreshold > 0 {
		set.breakerCfg.failureThreshold = d.CircuitBreaker.FailureThreshold
	}
	if d.CircuitBreaker.ResetTimeoutMS > 0 {
		set.breakerCfg.resetTimeout = time.Duration(d.CircuitBreaker.ResetTimeoutMS) * time.Millisecond
	}
	return set
}

// tokenBucket is a minimal thread-safe rate limiter.
type tokenBucket struct {
	mu     sync.Mutex
	rate   float64
	burst  float64
	tokens float64
	last   time.Time
}

func newTokenBucket(rate float64, burst int) *tokenBucket {
	if burst <= 0 {
		burst = 1
	}
	return &tokenBucket{rate: rate, burst: float64(burst), tokens: float64(burst), last: time.Now()}
}

func (b *tokenBucket) allow(now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	elapsed := now.Sub(b.last).Seconds()
	if elapsed > 0 {
		b.tokens += elapsed * b.rate
		if b.tokens > b.burst {
			b.tokens = b.burst
		}
		b.last = now
	}
	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}

const (
	breakerClosed = iota
	breakerOpen
	breakerHalfOpen
)

// breaker is a per-upstream circuit breaker. After failureThreshold
// consecutive failures the breaker opens for resetTimeout; the next request
// after that is a half-open probe and either restores or re-opens it.
type breaker struct {
	mu         sync.Mutex
	threshold  int
	resetAfter time.Duration
	failures   int
	state      int
	openedAt   time.Time
	probing    bool
}

func newBreaker(cfg breakerConfig) *breaker {
	if cfg.failureThreshold <= 0 {
		return nil
	}
	resetAfter := cfg.resetTimeout
	if resetAfter <= 0 {
		resetAfter = 30 * time.Second
	}
	return &breaker{threshold: cfg.failureThreshold, resetAfter: resetAfter}
}

func (b *breaker) allow(now time.Time) bool {
	if b == nil {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case breakerOpen:
		if now.Sub(b.openedAt) >= b.resetAfter {
			b.state = breakerHalfOpen
			b.probing = true
			return true
		}
		return false
	case breakerHalfOpen:
		// Only one in-flight probe is allowed; others fail fast.
		if b.probing {
			return false
		}
		b.probing = true
		return true
	default:
		return true
	}
}

func (b *breaker) recordSuccess() {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures = 0
	b.state = breakerClosed
	b.probing = false
}

func (b *breaker) recordFailure(now time.Time) {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures++
	b.probing = false
	if b.state == breakerHalfOpen || b.failures >= b.threshold {
		b.state = breakerOpen
		b.openedAt = now
	}
}

// profileRuntime bundles the mutable enforcement objects attached to one
// profile (concurrency, rate) plus the breaker shared by every profile using
// the same upstream.
type profileRuntime struct {
	policy  policySet
	sem     chan struct{}
	bucket  *tokenBucket
	breaker *breaker
}

// beginPolicy applies admission control for one request. The returned release
// function must be called when the request finishes; a non-nil rejection
// means the request must not proceed to the upstream.
func (rt *profileRuntime) beginPolicy(ctx context.Context, method string, header http.Header, willCallUpstream bool) (func(), *policyRejection) {
	if rt == nil {
		return func() {}, nil
	}
	now := time.Now()
	if willCallUpstream && !rt.breaker.allow(now) {
		return func() {}, &policyRejection{status: http.StatusServiceUnavailable, code: "circuit_open", retryAfter: rt.breaker.retryAfter()}
	}
	if rt.bucket != nil && !rt.bucket.allow(now) {
		return func() {}, &policyRejection{status: http.StatusTooManyRequests, code: "rate_limited", retryAfter: time.Second}
	}
	if rt.sem != nil {
		select {
		case rt.sem <- struct{}{}:
		default:
			return func() {}, &policyRejection{status: http.StatusServiceUnavailable, code: "concurrency_limit", retryAfter: time.Second}
		}
		select {
		case <-ctx.Done():
			<-rt.sem
			return func() {}, &policyRejection{status: http.StatusServiceUnavailable, code: "concurrency_wait_canceled", retryAfter: 0}
		default:
		}
	}
	return func() {
		if rt.sem != nil {
			<-rt.sem
		}
	}, nil
}

func (b *breaker) retryAfter() time.Duration {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == breakerOpen {
		if remaining := b.resetAfter - time.Since(b.openedAt); remaining > 0 {
			return remaining
		}
	}
	return time.Second
}

// retryAttemptsFor decides how many upstream attempts a request may use.
// Retries are limited to idempotent methods (or requests carrying an explicit
// Idempotency-Key) unless idempotent_only is disabled.
func (rt *profileRuntime) retryAttemptsFor(method string, header http.Header) int {
	if rt == nil || rt.policy.retryAttempts <= 1 {
		return 1
	}
	if !rt.policy.idempotentOnly {
		return rt.policy.retryAttempts
	}
	if header.Get("Idempotency-Key") != "" {
		return rt.policy.retryAttempts
	}
	switch strings.ToUpper(method) {
	case http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodTrace, http.MethodPut, http.MethodDelete:
		return rt.policy.retryAttempts
	default:
		return 1
	}
}

func (rt *profileRuntime) recordUpstreamResult(success bool) {
	if rt == nil {
		return
	}
	if success {
		rt.breaker.recordSuccess()
		return
	}
	rt.breaker.recordFailure(time.Now())
}

// buildRuntimes constructs the per-profile enforcement map for a config.
// Breakers live per upstream and survive re-activation so an open breaker is
// not silently reset by an unrelated configuration change.
func (s *Server) buildRuntimes(cfg *config.Config) map[string]*profileRuntime {
	if cfg == nil {
		return map[string]*profileRuntime{}
	}
	policy := decodePolicySet(cfg)
	runtimes := make(map[string]*profileRuntime, len(cfg.Profiles))
	for name := range cfg.Profiles {
		rt := &profileRuntime{policy: policy}
		if policy.maxConcurrency > 0 {
			rt.sem = make(chan struct{}, policy.maxConcurrency)
		}
		if policy.ratePerSecond > 0 {
			rt.bucket = newTokenBucket(policy.ratePerSecond, policy.burst)
		}
		rt.breaker = s.breakerFor(cfg.Profiles[name].Upstream, policy.breakerCfg)
		runtimes[name] = rt
	}
	return runtimes
}

func (s *Server) breakerFor(upstreamName string, cfg breakerConfig) *breaker {
	if cfg.failureThreshold <= 0 || upstreamName == "" {
		return nil
	}
	s.breakerMu.Lock()
	defer s.breakerMu.Unlock()
	if s.breakers == nil {
		s.breakers = map[string]*breaker{}
	}
	if existing, ok := s.breakers[upstreamName]; ok {
		return existing
	}
	created := newBreaker(cfg)
	s.breakers[upstreamName] = created
	return created
}

func (s *Server) runtimeFor(profileName string) *profileRuntime {
	runtimes := s.runtimes.Load()
	if runtimes == nil {
		return nil
	}
	return (*runtimes)[profileName]
}

// executeUpstream performs the buffered upstream call with retry, request
// timeout and circuit-breaker accounting. Streaming paths do their own I/O
// but use the same runtime for admission control and breaker recording.
func (s *Server) executeUpstream(ctx context.Context, cfg *config.Config, upstream config.Upstream, profile config.Profile, in transform.Envelope, requestID string, rt *profileRuntime, method string, header http.Header) (transform.Envelope, error) {
	if rt != nil && rt.policy.requestTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, rt.policy.requestTimeout)
		defer cancel()
	}
	attempts := rt.retryAttemptsFor(method, header)
	if upstream.Retries > 0 {
		attempts = upstream.Retries + 1
	}
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		if attempt > 0 {
			select {
			case <-time.After(time.Duration(attempt) * 100 * time.Millisecond):
			case <-ctx.Done():
				rt.recordUpstreamResult(false)
				return transform.Envelope{}, ctx.Err()
			}
		}
		envelope, err := s.callUpstream(ctx, cfg, upstream, profile, in, requestID)
		if err == nil {
			if envelope.StatusCode >= 500 && attempt+1 < attempts {
				lastErr = upstreamError{status: envelope.StatusCode, code: "upstream_retryable_status", err: errRetryableStatus(envelope.StatusCode)}
				continue
			}
			rt.recordUpstreamResult(envelope.StatusCode < 500)
			return envelope, nil
		}
		lastErr = err
	}
	rt.recordUpstreamResult(false)
	return transform.Envelope{}, lastErr
}

type retryableStatusError struct{ status int }

func (e retryableStatusError) Error() string { return "upstream returned a retryable server error" }

func errRetryableStatus(status int) error { return retryableStatusError{status: status} }

// maxInflightShadow bounds concurrent shadow requests; beyond it shadows are
// dropped with a warning instead of piling up.
const maxInflightShadow = 64

// shadowTimeout caps a shadow request independently of the primary one.
const shadowTimeout = 30 * time.Second

// dispatchShadow mirrors a request to the shadow upstream without blocking or
// failing the primary response. Shadow calls never touch circuit-breaker
// state and are dropped when the shadow budget is exhausted.
func (s *Server) dispatchShadow(cfg *config.Config, upstream config.Upstream, profile config.Profile, in transform.Envelope, requestID string) {
	select {
	case s.shadowSem <- struct{}{}:
	default:
		slog.Warn("shadow request dropped", "request_id", requestID, "reason", "shadow_concurrency_limit")
		return
	}
	go func() {
		defer func() {
			<-s.shadowSem
			if r := recover(); r != nil {
				slog.Error("shadow request panicked", "request_id", requestID, "panic", r)
			}
		}()
		ctx, cancel := context.WithTimeout(context.Background(), shadowTimeout)
		defer cancel()
		started := time.Now()
		_, err := s.callUpstream(ctx, cfg, upstream, profile, in, requestID)
		if err != nil {
			slog.Warn("shadow request failed", "request_id", requestID, "duration", time.Since(started), "error", err)
			return
		}
		slog.Debug("shadow request completed", "request_id", requestID, "duration", time.Since(started))
	}()
}
