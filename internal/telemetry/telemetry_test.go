package telemetry

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestMetricsSnapshotAndAuditDrop(t *testing.T) {
	metrics := New(2)
	finish := metrics.Begin()
	metrics.Observe(RequestRecord{RequestID: "ok", Status: 200, StartedAt: time.Now()})
	finish()
	metrics.AuditDropped()
	metrics.AuditDropped()
	snapshot := metrics.Snapshot()
	if snapshot.RequestsTotal != 1 || snapshot.RequestsOK != 1 || snapshot.RequestsFailed != 0 || snapshot.Active != 0 {
		t.Fatalf("snapshot=%+v", snapshot)
	}
	if snapshot.AuditEventsDropped != 2 {
		t.Fatalf("audit drops=%d", snapshot.AuditEventsDropped)
	}
	if prometheus := metrics.Prometheus(); !strings.Contains(prometheus, "gateway_audit_events_dropped_total 2") {
		t.Fatalf("prometheus=%s", prometheus)
	}
}

func TestRecentRingIsBoundedAndNewestFirst(t *testing.T) {
	metrics := New(2)
	for _, id := range []string{"one", "two", "three"} {
		metrics.Begin()()
		metrics.Observe(RequestRecord{RequestID: id, Status: 500, ErrorCode: "failed"})
	}
	recent := metrics.Snapshot().Recent
	if len(recent) != 2 || recent[0].RequestID != "three" || recent[1].RequestID != "two" {
		t.Fatalf("recent=%+v", recent)
	}
}

func TestLabeledRouteCounters(t *testing.T) {
	metrics := New(4)
	metrics.Observe(RequestRecord{Status: 200, Route: "chat", Profile: "p1"})
	metrics.Observe(RequestRecord{Status: 200, Route: "chat", Profile: "p1"})
	metrics.Observe(RequestRecord{Status: 404, Route: "chat", Profile: "p1"})
	metrics.Observe(RequestRecord{Status: 500, Route: "other", Profile: "p2", ErrorCode: "x"})
	// Requests observed before route selection carry no labels and must not
	// create a bucket.
	metrics.Observe(RequestRecord{Status: 200})

	stats := metrics.LabeledRequests()
	if len(stats) != 3 {
		t.Fatalf("stats=%+v", stats)
	}
	if stats[0].Route != "chat" || stats[0].Profile != "p1" || stats[0].Class != "2xx" || stats[0].Total != 2 {
		t.Fatalf("first stat=%+v", stats[0])
	}
	if stats[1].Class != "4xx" || stats[2].Route != "other" || stats[2].Class != "5xx" {
		t.Fatalf("stats=%+v", stats)
	}

	prometheus := metrics.Prometheus()
	if !strings.Contains(prometheus, "# TYPE gateway_requests_by_route_total counter") {
		t.Fatalf("prometheus missing type line: %s", prometheus)
	}
	if !strings.Contains(prometheus, `gateway_requests_by_route_total{route="chat",profile="p1",status="2xx"} 2`) {
		t.Fatalf("prometheus=%s", prometheus)
	}
}

func TestRouteCountersBoundCardinalityAndPreserveTotals(t *testing.T) {
	const budget = 1024
	metrics := New(1)
	for i := 0; i < budget+10; i++ {
		metrics.Observe(RequestRecord{Status: 200, Route: fmt.Sprintf("route-%04d", i), Profile: "profile"})
	}
	// Existing counters must remain monotonic after the budget is exhausted.
	metrics.Observe(RequestRecord{Status: 200, Route: "route-0000", Profile: "profile"})
	metrics.Observe(RequestRecord{Status: 500, Route: "overflow", Profile: "profile"})
	metrics.Observe(RequestRecord{Status: 999, Route: "overflow", Profile: "profile"})
	stats := metrics.LabeledRequests()
	if len(stats) != budget {
		t.Fatalf("route series=%d, budget=%d", len(stats), budget)
	}
	if stats[0].Route != "route-0000" || stats[0].Total != 2 {
		t.Fatalf("original counter changed identity or reset: %+v", stats[0])
	}
	var retained uint64
	for _, stat := range stats {
		retained += stat.Total
	}
	if retained != budget+1 {
		t.Fatalf("retained requests=%d", retained)
	}
	prometheus := metrics.Prometheus()
	for _, sample := range []string{
		"gateway_requests_route_overflow_total{status=\"2xx\"} 10",
		"gateway_requests_route_overflow_total{status=\"5xx\"} 1",
		"gateway_requests_route_overflow_total{status=\"unknown\"} 1",
	} {
		if !strings.Contains(prometheus, sample) {
			t.Errorf("missing overflow sample %s", sample)
		}
	}
}

func TestStatusClassHasBoundedValues(t *testing.T) {
	for _, tc := range []struct {
		status int
		want   string
	}{{0, "unknown"}, {99, "unknown"}, {101, "1xx"}, {200, "2xx"}, {599, "5xx"}, {600, "unknown"}, {9999, "unknown"}} {
		if got := StatusClass(tc.status); got != tc.want {
			t.Errorf("StatusClass(%d)=%q, want %q", tc.status, got, tc.want)
		}
	}
}
