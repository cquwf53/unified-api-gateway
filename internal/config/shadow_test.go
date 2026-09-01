package config

import (
	"strings"
	"testing"
)

func TestShadowedRoutesDetectsCoveredLowerPriorityRoute(t *testing.T) {
	cfg := &Config{Routes: []Route{
		{ID: "narrow", Priority: 100, Match: RouteMatch{Path: "/api/v1/users", Methods: []string{"POST"}}, Profile: "p"},
		{ID: "broad", Priority: 200, Match: RouteMatch{PathPrefix: "/api"}, Profile: "p"},
	}}
	out := cfg.ShadowedRoutes()
	if len(out) != 1 {
		t.Fatalf("expected one shadow warning, got %v", out)
	}
	for _, want := range []string{"narrow", "broad", "200", "100"} {
		if !contains(out[0], want) {
			t.Fatalf("warning %q missing %q", out[0], want)
		}
	}
}

func TestShadowedRoutesDetectsSamePathHigherPriority(t *testing.T) {
	cfg := &Config{Routes: []Route{
		{ID: "a", Priority: 100, Match: RouteMatch{Path: "/v1/chat/completions", Methods: []string{"POST"}}, Profile: "p"},
		{ID: "b", Priority: 300, Match: RouteMatch{Path: "/v1/chat/completions", Methods: []string{"POST"}}, Profile: "p"},
	}}
	if out := cfg.ShadowedRoutes(); len(out) != 1 {
		t.Fatalf("expected one shadow warning, got %v", out)
	}
}

func TestShadowedRoutesKeepsCanaryPatternSilent(t *testing.T) {
	// The example-config canary pattern: the narrower route wins by priority,
	// so nothing is shadowed.
	cfg := &Config{Routes: []Route{
		{ID: "canary", Priority: 210, Match: RouteMatch{Path: "/v1/chat/completions", Methods: []string{"POST"}, Headers: map[string]string{"X-Gateway-Profile": "canary"}}, Profile: "p"},
		{ID: "default", Priority: 200, Match: RouteMatch{Path: "/v1/chat/completions", Methods: []string{"POST"}}, Profile: "p"},
	}}
	if out := cfg.ShadowedRoutes(); len(out) != 0 {
		t.Fatalf("canary pattern must not warn, got %v", out)
	}
}

func TestShadowedRoutesHeaderAndMethodDifferencesPreventShadowing(t *testing.T) {
	cfg := &Config{Routes: []Route{
		{ID: "header-high", Priority: 300, Match: RouteMatch{Path: "/x", Headers: map[string]string{"A": "1"}}, Profile: "p"},
		{ID: "header-low", Priority: 100, Match: RouteMatch{Path: "/x"}, Profile: "p"},
		{ID: "get-high", Priority: 300, Match: RouteMatch{Path: "/y", Methods: []string{"GET"}}, Profile: "p"},
		{ID: "post-low", Priority: 100, Match: RouteMatch{Path: "/y", Methods: []string{"POST"}}, Profile: "p"},
	}}
	if out := cfg.ShadowedRoutes(); len(out) != 0 {
		t.Fatalf("differing constraints must not warn, got %v", out)
	}
}

func TestShadowedRoutesModelRouteNotShadowedByModelRoute(t *testing.T) {
	// Higher-priority route constrained to a model cannot cover a route
	// without that constraint, and vice versa.
	cfg := &Config{Routes: []Route{
		{ID: "by-model", Priority: 230, Match: RouteMatch{Path: "/v1/chat/completions", Methods: []string{"POST"}, Model: "zai/glm-5.2"}, Profile: "p"},
		{ID: "default", Priority: 200, Match: RouteMatch{Path: "/v1/chat/completions", Methods: []string{"POST"}}, Profile: "p"},
	}}
	if out := cfg.ShadowedRoutes(); len(out) != 0 {
		t.Fatalf("model-constrained routes must not warn, got %v", out)
	}
}

func TestShadowedRoutesEqualPriorityIsIgnored(t *testing.T) {
	cfg := &Config{Routes: []Route{
		{ID: "first", Priority: 100, Match: RouteMatch{Path: "/x"}, Profile: "p"},
		{ID: "second", Priority: 100, Match: RouteMatch{Path: "/x"}, Profile: "p"},
	}}
	if out := cfg.ShadowedRoutes(); len(out) != 0 {
		t.Fatalf("equal priority is detectDuplicateMatches' business, got %v", out)
	}
}

func contains(haystack, needle string) bool {
	return strings.Contains(haystack, needle)
}
