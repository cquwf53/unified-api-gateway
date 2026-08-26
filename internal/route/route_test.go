package route

import (
	"errors"
	"testing"

	"unified-api-gateway/internal/config"
)

func TestMatchAndPrioritySelection(t *testing.T) {
	c := &config.Config{
		Version: 1,
		Profiles: map[string]config.Profile{
			"broad":    {Name: "broad"},
			"specific": {Name: "specific"},
		},
		Routes: []config.Route{
			{ID: "broad", Profile: "broad", Priority: 10, Match: config.RouteMatch{PathPrefix: "/v1"}},
			{ID: "specific", Profile: "specific", Priority: 10, Match: config.RouteMatch{Path: "/v1/users", Methods: []string{"POST"}, Headers: map[string]string{"X-Tenant": "acme"}}},
		},
	}
	req := Request{Host: "api.example", Path: "/v1/users", Method: "POST", Headers: map[string][]string{"x-tenant": {"acme"}}}
	if !Match(c.Routes[1], req) {
		t.Fatal("specific route should match")
	}
	name, err := ResolveProfile(c, req)
	if err != nil || name != "specific" {
		t.Fatalf("ResolveProfile() = %q, %v", name, err)
	}
}

func TestMatchHostQueryAndRegex(t *testing.T) {
	r := config.Route{Match: config.RouteMatch{
		Host:    "*.example.com",
		Path:    "/v1/*",
		Method:  "GET",
		Headers: map[string]string{"X-Mode": "~^prod-[0-9]+$"},
		Query:   map[string]string{"format": "json"},
	}}
	req := Request{Host: "api.example.com:443", Path: "/v1/items", Method: "get", Headers: map[string][]string{"x-mode": {"prod-2"}}, Query: map[string][]string{"format": {"json"}}}
	if !Match(r, req) {
		t.Fatal("host/path/header/query route should match")
	}
	req.Query["format"] = []string{"xml"}
	if Match(r, req) {
		t.Fatal("query mismatch should fail")
	}
}

func TestExplicitProfileAuthorization(t *testing.T) {
	allowed := true
	c := &config.Config{
		Version:  1,
		Profiles: map[string]config.Profile{"one": {Name: "one"}, "two": {Name: "two"}},
		Clients:  map[string]config.ClientCredential{"client": {Profiles: []string{"one"}, Enabled: &allowed}},
	}
	if name, err := ResolveProfile(c, Request{ClientID: "client", Profile: "one"}); err != nil || name != "one" {
		t.Fatalf("allowed override = %q, %v", name, err)
	}
	_, err := ResolveProfile(c, Request{ClientID: "client", Profile: "two"})
	if !errors.Is(err, ErrProfileForbidden) {
		t.Fatalf("expected forbidden override, got %v", err)
	}
}

func TestAnonymousOverrideMarker(t *testing.T) {
	c := &config.Config{Version: 1, Profiles: map[string]config.Profile{"one": {Name: "one"}}, Clients: map[string]config.ClientCredential{"local": {KeyRef: "env:NOT_SET"}}}
	if name, err := ResolveProfile(c, Request{ClientID: "__anonymous__", Profile: "one"}); err != nil || name != "one" {
		t.Fatalf("anonymous local override = %q, %v", name, err)
	}
}

func TestClientUpstreamAllowlistIncludesShadow(t *testing.T) {
	c := &config.Config{
		Version:   1,
		Upstreams: map[string]config.Upstream{"primary": {}, "shadow": {}},
		Profiles: map[string]config.Profile{
			"p": {Name: "p", TrafficMode: "shadow", Upstream: "primary", ShadowUpstream: "shadow"},
		},
		Clients: map[string]config.ClientCredential{
			"client": {Profiles: []string{"p"}, Upstreams: []string{"primary"}},
		},
	}
	_, err := ResolveProfile(c, Request{ClientID: "client", Profile: "p"})
	if !errors.Is(err, ErrProfileForbidden) {
		t.Fatalf("shadow upstream outside the allowlist must be forbidden, got %v", err)
	}
	client := c.Clients["client"]
	client.Upstreams = []string{"*"}
	c.Clients["client"] = client
	if name, err := ResolveProfile(c, Request{ClientID: "client", Profile: "p"}); err != nil || name != "p" {
		t.Fatalf("wildcard upstream allowlist should allow shadow profile: %q, %v", name, err)
	}
}

func TestNoRoute(t *testing.T) {
	c := &config.Config{Version: 1, Profiles: map[string]config.Profile{"p": {Name: "p"}}}
	_, err := ResolveProfile(c, Request{Path: "/none"})
	if !errors.Is(err, ErrNoRoute) {
		t.Fatalf("expected ErrNoRoute, got %v", err)
	}
}
