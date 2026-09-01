package telemetry

import (
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
