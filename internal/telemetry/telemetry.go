// Package telemetry provides dependency-free counters and a bounded recent
// request list. It is intentionally small; OpenTelemetry export can be added
// around these hooks without changing the data-plane API.
package telemetry

import (
	"fmt"
	"net/http"
	"sort"
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
	RequestsTotal  uint64          `json:"requests_total"`
	RequestsOK     uint64          `json:"requests_ok"`
	RequestsFailed uint64          `json:"requests_failed"`
	Active         int64           `json:"active_requests"`
	Recent         []RequestRecord `json:"recent"`
}

type Metrics struct {
	total  atomic.Uint64
	ok     atomic.Uint64
	fail   atomic.Uint64
	active atomic.Int64
	mu     sync.RWMutex
	recent []RequestRecord
	limit  int
}

func New(limit int) *Metrics {
	if limit <= 0 {
		limit = 50
	}
	return &Metrics{limit: limit}
}

func (m *Metrics) Begin() func() {
	m.total.Add(1)
	m.active.Add(1)
	return func() { m.active.Add(-1) }
}

func (m *Metrics) Observe(record RequestRecord) {
	if record.Status >= 200 && record.Status < 400 && record.ErrorCode == "" {
		m.ok.Add(1)
	} else {
		m.fail.Add(1)
	}
	m.mu.Lock()
	m.recent = append([]RequestRecord{record}, m.recent...)
	if len(m.recent) > m.limit {
		m.recent = m.recent[:m.limit]
	}
	m.mu.Unlock()
}

func (m *Metrics) Snapshot() Snapshot {
	m.mu.RLock()
	recent := append([]RequestRecord(nil), m.recent...)
	m.mu.RUnlock()
	return Snapshot{
		RequestsTotal:  m.total.Load(),
		RequestsOK:     m.ok.Load(),
		RequestsFailed: m.fail.Load(),
		Active:         m.active.Load(),
		Recent:         recent,
	}
}

func (m *Metrics) Prometheus() string {
	s := m.Snapshot()
	return fmt.Sprintf("# TYPE gateway_requests_total counter\ngateway_requests_total %d\n# TYPE gateway_requests_ok counter\ngateway_requests_ok %d\n# TYPE gateway_requests_failed counter\ngateway_requests_failed %d\n# TYPE gateway_active_requests gauge\ngateway_active_requests %d\n", s.RequestsTotal, s.RequestsOK, s.RequestsFailed, s.Active)
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
