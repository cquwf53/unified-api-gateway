// Package route implements deterministic route matching and profile
// selection.  Matching is intentionally independent from net/http so it can
// be tested with recorded requests and used by WebSocket/gRPC adapters too.
package route

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"sort"
	"strings"

	"unified-api-gateway/internal/config"
)

// Request is the route matcher input.  It is an alias so callers can use the
// same value with config and route packages without conversions.
type Request = config.MatchRequest

var (
	ErrNoRoute          = errors.New("no route matched request")
	ErrNoProfile        = errors.New("route selected no profile")
	ErrProfileNotFound  = errors.New("selected profile does not exist")
	ErrProfileForbidden = errors.New("profile override is not allowed for client")
)

// FromHTTPRequest converts an incoming HTTP request to matcher data.  Model
// and tenant can be filled by an earlier authentication/body-normalisation
// stage.
func FromHTTPRequest(r *http.Request) Request {
	if r == nil {
		return Request{}
	}
	return Request{
		Host:    r.Host,
		Path:    r.URL.Path,
		Method:  r.Method,
		Headers: map[string][]string(r.Header),
		Query:   map[string][]string(r.URL.Query()),
	}
}

// Match reports whether a route matches a request.
func Match(r config.Route, req Request) bool {
	m := r.Match
	if m.Host != "" && !matchString(m.Host, hostWithoutPort(req.Host), true) && !matchString(m.Host, req.Host, true) {
		return false
	}
	if m.Path != "" && !matchPath(m.Path, req.Path) {
		return false
	}
	if m.PathPrefix != "" && !pathPrefix(m.PathPrefix, req.Path) {
		return false
	}
	if m.Method != "" && !strings.EqualFold(strings.TrimSpace(m.Method), req.Method) {
		return false
	}
	if len(m.Methods) > 0 {
		ok := false
		for _, method := range m.Methods {
			if strings.EqualFold(strings.TrimSpace(method), req.Method) {
				ok = true
				break
			}
		}
		if !ok {
			return false
		}
	}
	if !matchMap(m.Headers, req.Headers, true) || !matchMap(m.Query, req.Query, false) {
		return false
	}
	if m.Client != "" && !matchString(m.Client, firstNonEmpty(req.Client, req.ClientID), false) {
		return false
	}
	if m.ClientID != "" && !matchString(m.ClientID, req.ClientID, false) {
		return false
	}
	if m.Tenant != "" && !matchString(m.Tenant, req.Tenant, false) {
		return false
	}
	if m.Model != "" && !matchString(m.Model, req.Model, false) {
		return false
	}
	return true
}

// MatchRoute is a parameter-order-friendly alias.
func MatchRoute(req Request, r config.Route) bool { return Match(r, req) }

// (Matches is intentionally a package function because config.Route has a
// field named Match, which cannot coexist with a method of the same name.)

func matchPath(pattern, candidate string) bool {
	if candidate == "" {
		candidate = "/"
	}
	if pattern == candidate {
		return true
	}
	if strings.HasSuffix(pattern, "*") {
		return strings.HasPrefix(candidate, strings.TrimSuffix(pattern, "*"))
	}
	// A route path may include a query accidentally; compare only pathname.
	if u, err := url.Parse(candidate); err == nil && u.Path != "" {
		candidate = u.Path
	}
	if p, err := path.Match(pattern, candidate); err == nil {
		return p
	}
	return false
}

func pathPrefix(prefix, candidate string) bool {
	prefix = strings.TrimSuffix(prefix, "/")
	if prefix == "" {
		return true
	}
	return candidate == prefix || strings.HasPrefix(candidate, prefix+"/")
}

func matchMap(expect map[string]string, got map[string][]string, header bool) bool {
	for key, pattern := range expect {
		values := lookupValues(got, key, header)
		if len(values) == 0 {
			return false
		}
		matched := false
		for _, value := range values {
			if matchString(pattern, value, false) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

func lookupValues(values map[string][]string, key string, insensitive bool) []string {
	if values == nil {
		return nil
	}
	if v, ok := values[key]; ok {
		return v
	}
	if insensitive {
		for k, v := range values {
			if strings.EqualFold(k, key) {
				return v
			}
		}
	}
	return nil
}

func matchString(pattern, value string, insensitive bool) bool {
	if insensitive {
		pattern, value = strings.ToLower(pattern), strings.ToLower(value)
	}
	if pattern == "*" {
		return true
	}
	if strings.HasPrefix(pattern, "~") {
		re, err := regexp.Compile(strings.TrimPrefix(pattern, "~"))
		return err == nil && re.MatchString(value)
	}
	if strings.ContainsAny(pattern, "*?") {
		if ok, err := path.Match(pattern, value); err == nil {
			return ok
		}
	}
	return pattern == value
}

func hostWithoutPort(host string) string {
	if i := strings.LastIndex(host, ":"); i > -1 && !strings.Contains(host[i+1:], "]") {
		// IPv6 literals are enclosed in brackets; only strip a conventional
		// host:port suffix.
		if strings.Count(host, ":") == 1 {
			return host[:i]
		}
	}
	return strings.Trim(host, "[]")
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

// Specificity provides a stable score used to order routes with equal
// priority.  More constrained routes win over broad catch-alls.
func Specificity(r config.Route) int {
	m := r.Match
	score := 0
	if m.Host != "" {
		score += 16
	}
	if m.Path != "" {
		score += 32
	}
	if m.PathPrefix != "" {
		score += 24
	}
	if m.Method != "" || len(m.Methods) > 0 {
		score += 8
	}
	score += len(m.Headers) * 4
	score += len(m.Query) * 3
	if m.Client != "" || m.ClientID != "" {
		score += 8
	}
	if m.Tenant != "" {
		score += 4
	}
	if m.Model != "" {
		score += 4
	}
	return score
}

type candidate struct {
	index int
	route config.Route
}

func matchingRoutes(cfg *config.Config, req Request) []candidate {
	if cfg == nil {
		return nil
	}
	items := make([]candidate, 0, len(cfg.Routes))
	for i, r := range cfg.Routes {
		if Match(r, req) {
			items = append(items, candidate{index: i, route: r})
		}
	}
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].route.Priority != items[j].route.Priority {
			return items[i].route.Priority > items[j].route.Priority
		}
		if Specificity(items[i].route) != Specificity(items[j].route) {
			return Specificity(items[i].route) > Specificity(items[j].route)
		}
		return items[i].index < items[j].index
	})
	return items
}

// SelectProfile returns the selected profile and matching route.  An explicit
// request override is honored first, but only when the caller's credential
// permits that profile.
func SelectProfile(cfg *config.Config, req Request) (*config.Profile, *config.Route, error) {
	if cfg == nil {
		return nil, nil, errors.New("route: nil config")
	}
	if req.Profile != "" {
		if _, ok := cfg.Profiles[req.Profile]; !ok {
			return nil, nil, fmt.Errorf("%w: %s", ErrProfileNotFound, req.Profile)
		}
		if len(cfg.Clients) > 0 && strings.TrimSpace(firstNonEmpty(req.ClientID, req.Client)) == "" {
			return nil, nil, fmt.Errorf("%w: client authentication is required for profile override", ErrProfileForbidden)
		}
		if !profileAllowed(cfg, req, req.Profile) {
			return nil, nil, fmt.Errorf("%w: %s", ErrProfileForbidden, req.Profile)
		}
		return profilePtr(cfg, req.Profile), nil, nil
	}
	for _, item := range matchingRoutes(cfg, req) {
		if item.route.Profile == "" {
			continue
		}
		if _, ok := cfg.Profiles[item.route.Profile]; !ok {
			continue
		}
		if !profileAllowed(cfg, req, item.route.Profile) {
			continue
		}
		r := item.route
		return profilePtr(cfg, item.route.Profile), &r, nil
	}
	if cfg.DefaultsProfile != "" {
		if _, ok := cfg.Profiles[cfg.DefaultsProfile]; !ok {
			return nil, nil, fmt.Errorf("%w: %s", ErrProfileNotFound, cfg.DefaultsProfile)
		}
		if !profileAllowed(cfg, req, cfg.DefaultsProfile) {
			return nil, nil, fmt.Errorf("%w: %s", ErrProfileForbidden, cfg.DefaultsProfile)
		}
		return profilePtr(cfg, cfg.DefaultsProfile), nil, nil
	}
	return nil, nil, ErrNoRoute
}

// ResolveProfile returns only the selected profile ID for callers that do not
// need route metadata.
func ResolveProfile(cfg *config.Config, req Request) (string, error) {
	p, _, err := SelectProfile(cfg, req)
	if err != nil {
		return "", err
	}
	if p == nil {
		return "", ErrNoProfile
	}
	return p.Name, nil
}

// MatchRoutes returns matching routes in selection order, useful for a route
// explanation endpoint.
func MatchRoutes(cfg *config.Config, req Request) []config.Route {
	items := matchingRoutes(cfg, req)
	out := make([]config.Route, len(items))
	for i := range items {
		out[i] = items[i].route
	}
	return out
}

func profilePtr(cfg *config.Config, id string) *config.Profile {
	p := cfg.Profiles[id]
	if p.Name == "" {
		p.Name = id
	}
	return &p
}

func profileAllowed(cfg *config.Config, req Request, profile string) bool {
	clientID := firstNonEmpty(req.ClientID, req.Client)
	if clientID == "__anonymous__" {
		return true
	}
	if clientID == "" || len(cfg.Clients) == 0 {
		return true
	}
	client, ok := cfg.Clients[clientID]
	if !ok || client.Enabled != nil && !*client.Enabled {
		return false
	}
	if len(client.Profiles) == 0 {
		return true
	}
	for _, allowed := range client.Profiles {
		if allowed == "*" || allowed == profile {
			return true
		}
	}
	return false
}

// Router is a convenience object for servers that retain a current snapshot.
type Router struct {
	Config *config.Config
}

func NewRouter(cfg *config.Config) *Router { return &Router{Config: cfg} }

func NewRouterSnapshot(snapshot *config.ConfigSnapshot) *Router {
	if snapshot == nil {
		return &Router{}
	}
	return &Router{Config: snapshot.Config}
}

func (r *Router) Match(req Request) []config.Route {
	if r == nil {
		return nil
	}
	return MatchRoutes(r.Config, req)
}

func (r *Router) ResolveProfile(req Request) (string, error) {
	if r == nil {
		return "", errors.New("route: nil router")
	}
	return ResolveProfile(r.Config, req)
}

func (r *Router) SelectProfile(req Request) (*config.Profile, *config.Route, error) {
	if r == nil {
		return nil, nil, errors.New("route: nil router")
	}
	return SelectProfile(r.Config, req)
}
