package server

import (
	"sync"
	"time"

	"unified-api-gateway/internal/config"
)

// The counter is shared by every snapshot of a profile. Each request applies
// its pinned limit, while requests admitted by older snapshots still count.
type concurrencyGate struct {
	mu     sync.Mutex
	active int
}

func (g *concurrencyGate) acquire(limit int) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if limit > 0 && g.active >= limit {
		return false
	}
	g.active++
	return true
}

func (g *concurrencyGate) release() {
	g.mu.Lock()
	g.active--
	g.mu.Unlock()
}

type profileAdmission struct {
	sem    *concurrencyGate
	bucket *tokenBucket
}

func (s *Server) admissionFor(name string, policy policySet) *profileAdmission {
	s.admissionMu.Lock()
	defer s.admissionMu.Unlock()
	if s.admissions == nil {
		s.admissions = make(map[string]*profileAdmission)
	}
	if existing := s.admissions[name]; existing != nil {
		existing.bucket.configure(policy.ratePerSecond, policy.burst, time.Now())
		return existing
	}
	next := &profileAdmission{sem: &concurrencyGate{}, bucket: newTokenBucket(policy.ratePerSecond, policy.burst)}
	// Keep entries for removed profile IDs as well: an old pinned request
	// may still be using that identity when the profile is later re-added.
	s.admissions[name] = next
	return next
}

func (s *Server) restoreAdmissions(cfg *config.Config) {
	policy := decodePolicySet(cfg)
	for name := range cfg.Profiles {
		s.admissionFor(name, policy)
	}
}

func (b *tokenBucket) configure(rate float64, burst int, now time.Time) {
	if burst <= 0 {
		burst = 1
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.rate > 0 {
		if elapsed := now.Sub(b.last).Seconds(); elapsed > 0 {
			b.tokens += elapsed * b.rate
			if b.tokens > b.burst {
				b.tokens = b.burst
			}
		}
	}
	// Re-enabling an existing limiter must not mint tokens: this path also
	// restores parameters after a failed activation temporarily disabled it.
	b.rate, b.burst, b.last = rate, float64(burst), now
	if b.tokens > b.burst {
		b.tokens = b.burst
	}
}

// Body parsing precedes model-based routing, so it needs its own process-wide
// budget. Admission is released as soon as parsing finishes, not after I/O to
// the upstream. The counter survives config activation.
func (s *Server) beginIngress(pinned *gatewayState) (func(), *policyRejection) {
	limit := int64(128)
	if pinned != nil && pinned.policy.maxConcurrency > 0 {
		limit = int64(pinned.policy.maxConcurrency)
	}
	for {
		active := s.ingressActive.Load()
		if active >= limit {
			return func() {}, &policyRejection{status: 503, code: "ingress_concurrency_limit", retryAfter: time.Second}
		}
		if s.ingressActive.CompareAndSwap(active, active+1) {
			break
		}
	}
	var once sync.Once
	return func() { once.Do(func() { s.ingressActive.Add(-1) }) }, nil
}

func transparentPlaneSupported(profile config.Profile) (string, string) {
	if mode := normalizeMode(profile.TrafficMode); mode != "" && mode != "live" {
		return "traffic_mode_unsupported", "this transparent endpoint only supports live traffic"
	}
	if len(profile.AllRequestTransforms()) != 0 || len(profile.AllResponseTransforms()) != 0 {
		return "profile_transforms_unsupported", "this transparent endpoint cannot apply profile transforms"
	}
	return "", ""
}
