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
