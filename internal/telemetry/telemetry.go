// Package telemetry provides dependency-free counters and a bounded recent
// request list. It is intentionally small; OpenTelemetry export can be added
// around these hooks without changing the data-plane API.
package telemetry

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type RequestRecord struct {
	RequestID  string        `json:"request_id"`
	StartedAt  time.Time     `json:"started_at"`
	Duration   time.Duration `json:"duration_ns"`
	Method     string        `json:"method"`
	Path       string        `json:"path"`
	Route      string        `json:"route"`
	Profile    string        `json:"profile"`
	Mode       string        `json:"mode"`
	Upstream   string        `json:"upstream"`
	Status     int           `json:"status"`
	ErrorStage string        `json:"error_stage,omitempty"`
	ErrorCode  string        `json:"error_code,omitempty"`
}

type Snapshot struct {
	RequestsTotal      uint64          `json:"requests_total"`
	RequestsOK         uint64          `json:"requests_ok"`
	RequestsFailed     uint64          `json:"requests_failed"`
	AuditEventsDropped uint64          `json:"audit_events_dropped"`
	Active             int64           `json:"active_requests"`
	Recent             []RequestRecord `json:"recent"`
}

// RouteStat is one observed route/profile/status-class combination. Route
// and profile identifiers come from the pinned configuration, so label
// cardinality is bounded by the active configuration rather than by traffic.
type RouteStat struct {
	Route   string
	Profile string
	Class   string
	Total   uint64
}

type routeStatKey struct {
	route   string
	profile string
	class   string
}

type Metrics struct {
	total        atomic.Uint64
	ok           atomic.Uint64
	fail         atomic.Uint64
	auditDropped atomic.Uint64
	active       atomic.Int64
	mu           sync.RWMutex
	// recent is a ring buffer: new records overwrite the oldest slot instead
	// of re-allocating the whole slice on every observation.
	recent []RequestRecord
	head   int
	count  int
	limit  int

	labeledMu sync.Mutex
	labeled   map[routeStatKey]uint64
}

func New(limit int) *Metrics {
	if limit <= 0 {
		limit = 50
	}
	return &Metrics{limit: limit, recent: make([]RequestRecord, limit)}
}

func (m *Metrics) Begin() func() {
	m.total.Add(1)
	m.active.Add(1)
	return func() { m.active.Add(-1) }
}

func (m *Metrics) AuditDropped() { m.auditDropped.Add(1) }

func (m *Metrics) Observe(record RequestRecord) {
	// 101 Switching Protocols is the success status of a proxied WebSocket
	// upgrade and must count as ok even though it sits below 2xx.
	success := record.ErrorCode == "" &&
		((record.Status >= 200 && record.Status < 400) || record.Status == http.StatusSwitchingProtocols)
	if success {
		m.ok.Add(1)
	} else {
		m.fail.Add(1)
	}
	m.mu.Lock()
	m.recent[m.head] = record
	m.head = (m.head + 1) % m.limit
	if m.count < m.limit {
		m.count++
	}
	m.mu.Unlock()
	if record.Route != "" || record.Profile != "" {
		m.labeledMu.Lock()
		if m.labeled == nil {
			m.labeled = map[routeStatKey]uint64{}
		}
		m.labeled[routeStatKey{route: record.Route, profile: record.Profile, class: StatusClass(record.Status)}]++
		m.labeledMu.Unlock()
	}
}

// LabeledRequests returns the per-route/profile/status-class totals in
// deterministic (sorted) order.
func (m *Metrics) LabeledRequests() []RouteStat {
	m.labeledMu.Lock()
	out := make([]RouteStat, 0, len(m.labeled))
	for key, total := range m.labeled {
		out = append(out, RouteStat{Route: key.route, Profile: key.profile, Class: key.class, Total: total})
	}
	m.labeledMu.Unlock()
	sort.Slice(out, func(i, j int) bool {
		if out[i].Route != out[j].Route {
			return out[i].Route < out[j].Route
		}
		if out[i].Profile != out[j].Profile {
			return out[i].Profile < out[j].Profile
		}
		return out[i].Class < out[j].Class
	})
	return out
}

func (m *Metrics) Snapshot() Snapshot {
	m.mu.RLock()
	out := make([]RequestRecord, 0, m.count)
	for i := 0; i < m.count; i++ {
		index := (m.head - 1 - i + m.limit) % m.limit
		out = append(out, m.recent[index])
	}
	m.mu.RUnlock()
	return Snapshot{
		RequestsTotal:      m.total.Load(),
		RequestsOK:         m.ok.Load(),
		RequestsFailed:     m.fail.Load(),
		AuditEventsDropped: m.auditDropped.Load(),
		Active:             m.active.Load(),
		Recent:             out,
	}
}

func (m *Metrics) Prometheus() string {
	s := m.Snapshot()
	var out strings.Builder
	fmt.Fprintf(&out, "# TYPE gateway_requests_total counter\ngateway_requests_total %d\n", s.RequestsTotal)
	fmt.Fprintf(&out, "# TYPE gateway_requests_ok counter\ngateway_requests_ok %d\n", s.RequestsOK)
	fmt.Fprintf(&out, "# TYPE gateway_requests_failed counter\ngateway_requests_failed %d\n", s.RequestsFailed)
	fmt.Fprintf(&out, "# TYPE gateway_audit_events_dropped_total counter\ngateway_audit_events_dropped_total %d\n", s.AuditEventsDropped)
	fmt.Fprintf(&out, "# TYPE gateway_active_requests gauge\ngateway_active_requests %d\n", s.Active)
	out.WriteString("# TYPE gateway_requests_by_route_total counter\n")
	for _, stat := range m.LabeledRequests() {
		fmt.Fprintf(&out, "gateway_requests_by_route_total{route=%q,profile=%q,status=%q} %d\n", stat.Route, stat.Profile, stat.Class, stat.Total)
	}
	return out.String()
}

func (m *Metrics) SortRecentByDuration() []RequestRecord {
	s := m.Snapshot().Recent
	sort.SliceStable(s, func(i, j int) bool { return s[i].Duration > s[j].Duration })
	return s
}

func StatusClass(status int) string {
	if status < 100 {
		return "unknown"
	}
	return fmt.Sprintf("%dxx", status/100)
}

func RequestPath(r *http.Request) string {
	if r == nil || r.URL == nil {
		return ""
	}
	return r.URL.Path
}
