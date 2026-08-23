// Package config defines the versioned gateway configuration and its loader.
// Config values are plain data so they can be shared by the data plane, the
// management API and the CLI without importing either server implementation.
package config

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"unified-api-gateway/internal/transform"

	"gopkg.in/yaml.v3"
)

const CurrentVersion = 1

// Config is the complete user-facing gateway configuration.
type Config struct {
	Version         int                                `yaml:"version,omitempty" json:"version"`
	Listeners       Listeners                          `yaml:"listeners,omitempty" json:"listeners"`
	DefaultsProfile string                             `yaml:"defaults_profile,omitempty" json:"defaults_profile"`
	Upstreams       map[string]Upstream                `yaml:"upstreams,omitempty" json:"upstreams"`
	Profiles        map[string]Profile                 `yaml:"profiles,omitempty" json:"profiles"`
	Routes          []Route                            `yaml:"routes,omitempty" json:"routes"`
	Clients         map[string]ClientCredential        `yaml:"clients,omitempty" json:"clients"`
	Credentials     map[string]Credential              `yaml:"credentials,omitempty" json:"credentials"`
	Transforms      map[string]transform.TransformSpec `yaml:"transforms,omitempty" json:"transforms"`
	Policies        map[string]any                     `yaml:"policies,omitempty" json:"policies"`
	Observability   Observability                      `yaml:"observability,omitempty" json:"observability"`
	Limits          Limits                             `yaml:"limits,omitempty" json:"limits"`
}

// UnmarshalYAML accepts the documented snake_case names and a few harmless
// aliases used by early gateway configs (default_profile and top-level
// listener addresses).
func (c *Config) UnmarshalYAML(node *yaml.Node) error {
	type plain Config
	var p plain
	if err := node.Decode(&p); err != nil {
		return err
	}
	*c = Config(p)
	if c.DefaultsProfile == "" {
		var aux struct {
			DefaultProfile string `yaml:"default_profile"`
			HTTP           string `yaml:"http"`
			Admin          string `yaml:"admin"`
			GRPC           string `yaml:"grpc"`
		}
		if err := node.Decode(&aux); err == nil {
			c.DefaultsProfile = aux.DefaultProfile
			if c.Listeners.HTTP == "" {
				c.Listeners.HTTP = aux.HTTP
			}
			if c.Listeners.Admin == "" {
				c.Listeners.Admin = aux.Admin
			}
			if c.Listeners.GRPC == "" {
				c.Listeners.GRPC = aux.GRPC
			}
		}
	}
	return nil
}

// Listeners contains independent data, administration and optional gRPC
// listener addresses.  Empty GRPC disables the listener.
type Listeners struct {
	HTTP  string `yaml:"http,omitempty" json:"http,omitempty"`
	Admin string `yaml:"admin,omitempty" json:"admin,omitempty"`
	GRPC  string `yaml:"grpc,omitempty" json:"grpc,omitempty"`
}

// Upstream describes one target service.
type Upstream struct {
	Name      string            `yaml:"name,omitempty" json:"name,omitempty"`
	BaseURL   string            `yaml:"base_url,omitempty" json:"base_url,omitempty"`
	URL       string            `yaml:"url,omitempty" json:"url,omitempty"`
	Protocol  string            `yaml:"protocol,omitempty" json:"protocol,omitempty"`
	Headers   map[string]string `yaml:"headers,omitempty" json:"headers,omitempty"`
	AuthRef   string            `yaml:"auth_ref,omitempty" json:"auth_ref,omitempty"`
	TimeoutMS int               `yaml:"timeout_ms,omitempty" json:"timeout_ms,omitempty"`
	Timeout   string            `yaml:"timeout,omitempty" json:"timeout,omitempty"`
	Retries   int               `yaml:"retries,omitempty" json:"retries,omitempty"`
	Weight    int               `yaml:"weight,omitempty" json:"weight,omitempty"`
	Insecure  bool              `yaml:"insecure,omitempty" json:"insecure,omitempty"`
}

// EffectiveURL returns BaseURL, falling back to URL for compatibility.
func (u Upstream) EffectiveURL() string {
	if strings.TrimSpace(u.BaseURL) != "" {
		return strings.TrimSpace(u.BaseURL)
	}
	return strings.TrimSpace(u.URL)
}

// EffectiveTimeout returns a parsed timeout.  Invalid values are reported by
// Validate; this helper returns zero for an absent or malformed value.
func (u Upstream) EffectiveTimeout() time.Duration {
	if u.TimeoutMS > 0 {
		return time.Duration(u.TimeoutMS) * time.Millisecond
	}
	if u.Timeout != "" {
		if d, err := time.ParseDuration(u.Timeout); err == nil {
			return d
		}
	}
	return 0
}

// Profile is a reusable client-to-upstream contract and transformation
// pipeline.  Request and Response accept either one mapping or a YAML list;
// lists are represented by TransformSpec.Steps.
type Profile struct {
	Name               string                    `yaml:"name,omitempty" json:"name,omitempty"`
	Adapter            string                    `yaml:"adapter,omitempty" json:"adapter,omitempty"`
	ClientContract     string                    `yaml:"client_contract,omitempty" json:"client_contract,omitempty"`
	UpstreamContract   string                    `yaml:"upstream_contract,omitempty" json:"upstream_contract,omitempty"`
	TrafficMode        string                    `yaml:"traffic_mode,omitempty" json:"traffic_mode,omitempty"`
	Upstream           string                    `yaml:"upstream,omitempty" json:"upstream,omitempty"`
	Request            transform.TransformSpec   `yaml:"request,omitempty" json:"request,omitempty"`
	Response           transform.TransformSpec   `yaml:"response,omitempty" json:"response,omitempty"`
	RequestTransforms  []transform.TransformSpec `yaml:"request_transforms,omitempty" json:"request_transforms,omitempty"`
	ResponseTransforms []transform.TransformSpec `yaml:"response_transforms,omitempty" json:"response_transforms,omitempty"`
	StreamSafe         bool                      `yaml:"stream_safe,omitempty" json:"stream_safe,omitempty"`
	MaxBodyBytes       int64                     `yaml:"max_body_bytes,omitempty" json:"max_body_bytes,omitempty"`
	AllowedMethods     []string                  `yaml:"allowed_methods,omitempty" json:"allowed_methods,omitempty"`
}

// AllRequestTransforms returns the compact request spec followed by explicit
// chain entries.  It is convenient for server implementations and avoids
// special-casing either YAML style.
func (p Profile) AllRequestTransforms() []transform.TransformSpec {
	out := make([]transform.TransformSpec, 0, 1+len(p.RequestTransforms))
	if !specEmpty(p.Request) {
		out = append(out, p.Request)
	}
	out = append(out, p.RequestTransforms...)
	return out
}

func (p Profile) AllResponseTransforms() []transform.TransformSpec {
	out := make([]transform.TransformSpec, 0, 1+len(p.ResponseTransforms))
	if !specEmpty(p.Response) {
		out = append(out, p.Response)
	}
	out = append(out, p.ResponseTransforms...)
	return out
}

func specEmpty(s transform.TransformSpec) bool {
	return s.Name == "" && s.Method == "" && s.Path == "" && s.PathPrefix == "" &&
		len(s.Query.Set) == 0 && len(s.Query.Add) == 0 && len(s.Query.Rename) == 0 && len(s.Query.Delete) == 0 &&
		len(s.Headers.Set) == 0 && len(s.Headers.Add) == 0 && len(s.Headers.Rename) == 0 && len(s.Headers.Delete) == 0 &&
		len(s.Body.Set) == 0 && len(s.Body.Rename) == 0 && len(s.Body.Delete) == 0 && len(s.Body.Default) == 0 && len(s.Body.Minimum) == 0 &&
		s.Status == nil && len(s.StatusMap) == 0 && !s.StreamSafe && len(s.Steps) == 0
}

// Route selects a profile when Match matches an incoming request.
type Route struct {
	ID       string     `yaml:"id,omitempty" json:"id,omitempty"`
	Name     string     `yaml:"name,omitempty" json:"name,omitempty"`
	Match    RouteMatch `yaml:"match,omitempty" json:"match,omitempty"`
	Profile  string     `yaml:"profile,omitempty" json:"profile,omitempty"`
	Priority int        `yaml:"priority,omitempty" json:"priority,omitempty"`
}

// RouteMatch is deliberately protocol-neutral.  Header and query values are
// exact by default; a value beginning with ~ is treated as a regular
// expression by the route package.
type RouteMatch struct {
	Host       string            `yaml:"host,omitempty" json:"host,omitempty"`
	Path       string            `yaml:"path,omitempty" json:"path,omitempty"`
	PathPrefix string            `yaml:"path_prefix,omitempty" json:"path_prefix,omitempty"`
	Method     string            `yaml:"method,omitempty" json:"method,omitempty"`
	Methods    []string          `yaml:"methods,omitempty" json:"methods,omitempty"`
	Headers    map[string]string `yaml:"headers,omitempty" json:"headers,omitempty"`
	Query      map[string]string `yaml:"query,omitempty" json:"query,omitempty"`
	Client     string            `yaml:"client,omitempty" json:"client,omitempty"`
	ClientID   string            `yaml:"client_id,omitempty" json:"client_id,omitempty"`
	Tenant     string            `yaml:"tenant,omitempty" json:"tenant,omitempty"`
	Model      string            `yaml:"model,omitempty" json:"model,omitempty"`
}

// MatchRequest is the input to route matching.  Header and query maps may be
// either net/http's concrete types or ordinary maps converted by adapters.
type MatchRequest struct {
	Host     string
	Path     string
	Method   string
	Headers  map[string][]string
	Query    map[string][]string
	Client   string
	ClientID string
	Tenant   string
	Model    string
	Profile  string // optional explicit profile override
}

// ClientCredential identifies a caller and limits the profiles it may use.
type ClientCredential struct {
	Name      string   `yaml:"name,omitempty" json:"name,omitempty"`
	Key       string   `yaml:"key,omitempty" json:"key,omitempty"`
	KeyRef    string   `yaml:"key_ref,omitempty" json:"key_ref,omitempty"`
	KeyEnv    string   `yaml:"key_env,omitempty" json:"key_env,omitempty"`
	Tenant    string   `yaml:"tenant,omitempty" json:"tenant,omitempty"`
	Profiles  []string `yaml:"profiles,omitempty" json:"profiles,omitempty"`
	Upstreams []string `yaml:"upstreams,omitempty" json:"upstreams,omitempty"`
	Enabled   *bool    `yaml:"enabled,omitempty" json:"enabled,omitempty"`
}

type Credential struct {
	Value string `yaml:"value,omitempty" json:"value,omitempty"`
	Ref   string `yaml:"ref,omitempty" json:"ref,omitempty"`
	Type  string `yaml:"type,omitempty" json:"type,omitempty"`
	Env   string `yaml:"env,omitempty" json:"env,omitempty"`
}

type Limits struct {
	MaxBodyBytes   int64   `yaml:"max_body_bytes,omitempty" json:"max_body_bytes,omitempty"`
	RequestsPerSec float64 `yaml:"requests_per_second,omitempty" json:"requests_per_second,omitempty"`
	Burst          int     `yaml:"burst,omitempty" json:"burst,omitempty"`
	Concurrency    int     `yaml:"concurrency,omitempty" json:"concurrency,omitempty"`
}

type Observability struct {
	LogLevel        string `yaml:"log_level,omitempty" json:"log_level,omitempty"`
	Trace           bool   `yaml:"trace,omitempty" json:"trace,omitempty"`
	Metrics         bool   `yaml:"metrics,omitempty" json:"metrics,omitempty"`
	RecordBody      bool   `yaml:"record_body,omitempty" json:"record_body,omitempty"`
	RedactPrompts   bool   `yaml:"redact_prompts,omitempty" json:"redact_prompts,omitempty"`
	RequestIDHeader string `yaml:"request_id_header,omitempty" json:"request_id_header,omitempty"`
}

// ConfigSnapshot is an immutable-at-use version of Config.  The loader never
// mutates a snapshot after returning it; Clone can be used before a control
// plane edit.
type ConfigSnapshot struct {
	Config   *Config   `json:"config"`
	Version  int       `json:"version"`
	ID       string    `json:"id"`
	Hash     string    `json:"hash"`
	LoadedAt time.Time `json:"loaded_at"`
	Source   string    `json:"source,omitempty"`
}

// NewDefault returns a usable minimal configuration.
func NewDefault() *Config {
	c := &Config{Version: CurrentVersion}
	c.applyDefaults()
	return c
}

// Parse parses YAML (JSON is valid YAML) and validates it.
func Parse(data []byte) (*Config, error) {
	var c Config
	if len(strings.TrimSpace(string(data))) == 0 {
		c = *NewDefault()
		return &c, nil
	}
	if err := yaml.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("config YAML: %w", err)
	}
	c.applyDefaults()
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

// LoadBytes is an alias for Parse.
func LoadBytes(data []byte) (*Config, error) { return Parse(data) }

// LoadFile loads, normalises and validates a YAML configuration.
func LoadFile(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %q: %w", path, err)
	}
	c, err := Parse(data)
	if err != nil {
		return nil, fmt.Errorf("load config %q: %w", path, err)
	}
	return c, nil
}

// LoadSnapshotFile is the versioned counterpart to LoadFile.
func LoadSnapshotFile(path string) (*ConfigSnapshot, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %q: %w", path, err)
	}
	c, err := Parse(data)
	if err != nil {
		return nil, fmt.Errorf("load config %q: %w", path, err)
	}
	return NewSnapshot(c, path)
}

// NewSnapshot creates a content-addressed snapshot.
func NewSnapshot(c *Config, source string) (*ConfigSnapshot, error) {
	if c == nil {
		return nil, errors.New("config: nil config")
	}
	clone := c.Clone()
	if err := clone.Validate(); err != nil {
		return nil, err
	}
	b, err := json.Marshal(clone)
	if err != nil {
		return nil, fmt.Errorf("snapshot hash: %w", err)
	}
	h := sha256.Sum256(b)
	return &ConfigSnapshot{
		Config:   clone,
		Version:  clone.Version,
		ID:       hex.EncodeToString(h[:8]),
		Hash:     hex.EncodeToString(h[:]),
		LoadedAt: time.Now().UTC(),
		Source:   source,
	}, nil
}

func (s *ConfigSnapshot) Clone() *ConfigSnapshot {
	if s == nil {
		return nil
	}
	out := *s
	if s.Config != nil {
		out.Config = s.Config.Clone()
	}
	return &out
}

// Clone performs a deep copy through JSON.  All public config fields are JSON
// compatible and this keeps map/slice aliasing out of hot-reload snapshots.
func (c *Config) Clone() *Config {
	if c == nil {
		return nil
	}
	b, err := json.Marshal(c)
	if err != nil {
		out := *c
		return &out
	}
	var out Config
	if err := json.Unmarshal(b, &out); err != nil {
		copy := *c
		return &copy
	}
	return &out
}

// Validate returns all discovered errors in one call where possible.
func (c *Config) Validate() error {
	if c == nil {
		return errors.New("config is nil")
	}
	var errs []string
	if c.Version <= 0 {
		errs = append(errs, "version must be positive")
	} else if c.Version != CurrentVersion {
		errs = append(errs, fmt.Sprintf("unsupported version %d (current %d)", c.Version, CurrentVersion))
	}
	for id, u := range c.Upstreams {
		name := id
		if u.Name != "" {
			name = u.Name
		}
		base := u.EffectiveURL()
		if base == "" {
			errs = append(errs, fmt.Sprintf("upstream %q: base_url is required", name))
		} else if parsed, err := url.Parse(base); err != nil || parsed.Scheme == "" || parsed.Host == "" {
			errs = append(errs, fmt.Sprintf("upstream %q: invalid base_url %q", name, base))
		} else if !allowedScheme(parsed.Scheme) {
			errs = append(errs, fmt.Sprintf("upstream %q: unsupported URL scheme %q", name, parsed.Scheme))
		}
		if u.TimeoutMS < 0 {
			errs = append(errs, fmt.Sprintf("upstream %q: timeout_ms must be non-negative", name))
		}
		if u.Timeout != "" {
			if d, err := time.ParseDuration(u.Timeout); err != nil || d < 0 {
				errs = append(errs, fmt.Sprintf("upstream %q: invalid timeout %q", name, u.Timeout))
			}
		}
	}
	for id, p := range c.Profiles {
		name := id
		if p.Name != "" {
			name = p.Name
		}
		mode := strings.ToLower(strings.TrimSpace(p.TrafficMode))
		if mode == "" {
			mode = "live"
		}
		if mode != "live" && mode != "dry-run" && mode != "dry_run" && mode != "shadow" && mode != "mock" && mode != "record" && mode != "replay" {
			errs = append(errs, fmt.Sprintf("profile %q: unsupported traffic_mode %q", name, p.TrafficMode))
		}
		if p.Upstream != "" {
			if _, ok := c.Upstreams[p.Upstream]; !ok && mode != "mock" && mode != "replay" {
				errs = append(errs, fmt.Sprintf("profile %q: upstream %q not found", name, p.Upstream))
			}
		} else if mode == "live" || mode == "shadow" || mode == "record" {
			errs = append(errs, fmt.Sprintf("profile %q: upstream is required for traffic_mode %q", name, mode))
		}
		for i, spec := range p.AllRequestTransforms() {
			if err := spec.Validate(); err != nil {
				errs = append(errs, fmt.Sprintf("profile %q request transform %d: %v", name, i, err))
			}
		}
		for i, spec := range p.AllResponseTransforms() {
			if err := spec.Validate(); err != nil {
				errs = append(errs, fmt.Sprintf("profile %q response transform %d: %v", name, i, err))
			}
		}
		if p.MaxBodyBytes < 0 {
			errs = append(errs, fmt.Sprintf("profile %q: max_body_bytes must be non-negative", name))
		}
	}
	seenRoutes := map[string]bool{}
	for i, r := range c.Routes {
		id := r.ID
		if id == "" {
			id = r.Name
		}
		if id != "" {
			if seenRoutes[id] {
				errs = append(errs, fmt.Sprintf("route %q is duplicated", id))
			}
			seenRoutes[id] = true
		}
		if r.Profile == "" {
			errs = append(errs, fmt.Sprintf("route %d: profile is required", i))
		} else if _, ok := c.Profiles[r.Profile]; !ok {
			errs = append(errs, fmt.Sprintf("route %q: profile %q not found", routeLabel(r, i), r.Profile))
		}
		if r.Match.Method != "" && len(r.Match.Methods) > 0 {
			errs = append(errs, fmt.Sprintf("route %q: specify method or methods, not both", routeLabel(r, i)))
		}
	}
	if c.DefaultsProfile != "" {
		if _, ok := c.Profiles[c.DefaultsProfile]; !ok {
			errs = append(errs, fmt.Sprintf("defaults_profile %q not found", c.DefaultsProfile))
		}
	}
	for id, client := range c.Clients {
		if client.Enabled != nil && !*client.Enabled {
			continue
		}
		for _, p := range client.Profiles {
			if _, ok := c.Profiles[p]; !ok {
				errs = append(errs, fmt.Sprintf("client %q: profile %q not found", id, p))
			}
		}
		for _, u := range client.Upstreams {
			if _, ok := c.Upstreams[u]; !ok {
				errs = append(errs, fmt.Sprintf("client %q: upstream %q not found", id, u))
			}
		}
	}
	for name, spec := range c.Transforms {
		if err := spec.Validate(); err != nil {
			errs = append(errs, fmt.Sprintf("transform %q: %v", name, err))
		}
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

func (c *Config) applyDefaults() {
	if c.Version == 0 {
		c.Version = CurrentVersion
	}
	if c.Listeners.HTTP == "" {
		c.Listeners.HTTP = ":8080"
	}
	if c.Listeners.Admin == "" {
		c.Listeners.Admin = ":9090"
	}
	if c.Upstreams == nil {
		c.Upstreams = map[string]Upstream{}
	}
	if c.Profiles == nil {
		c.Profiles = map[string]Profile{}
	}
	if c.Clients == nil {
		c.Clients = map[string]ClientCredential{}
	}
	if c.Credentials == nil {
		c.Credentials = map[string]Credential{}
	}
	if c.Transforms == nil {
		c.Transforms = map[string]transform.TransformSpec{}
	}
	if c.Policies == nil {
		c.Policies = map[string]any{}
	}
	for id, p := range c.Profiles {
		if p.Name == "" {
			p.Name = id
		}
		if p.Adapter == "" {
			p.Adapter = "transparent"
		}
		if p.TrafficMode == "" {
			p.TrafficMode = "live"
		}
		if !p.StreamSafe {
			// A profile is not assumed streaming-safe when it has body rules.
			p.StreamSafe = streamSafeProfile(p)
		}
		c.Profiles[id] = p
	}
	for i := range c.Routes {
		if c.Routes[i].ID == "" && c.Routes[i].Name == "" {
			c.Routes[i].ID = fmt.Sprintf("route-%d", i+1)
		}
	}
	for id, u := range c.Upstreams {
		if u.Name == "" {
			u.Name = id
		}
		if u.Protocol == "" {
			if parsed, err := url.Parse(u.EffectiveURL()); err == nil {
				u.Protocol = strings.ToLower(parsed.Scheme)
			}
		}
		if u.Headers == nil {
			u.Headers = map[string]string{}
		}
		c.Upstreams[id] = u
	}
}

func streamSafeProfile(p Profile) bool {
	for _, s := range p.AllRequestTransforms() {
		if len(s.Body.Set) > 0 || len(s.Body.Rename) > 0 || len(s.Body.Delete) > 0 || len(s.Body.Default) > 0 || len(s.Body.Minimum) > 0 || len(s.Steps) > 0 {
			return false
		}
	}
	for _, s := range p.AllResponseTransforms() {
		if len(s.Body.Set) > 0 || len(s.Body.Rename) > 0 || len(s.Body.Delete) > 0 || len(s.Body.Default) > 0 || len(s.Body.Minimum) > 0 || len(s.Steps) > 0 {
			return false
		}
	}
	return true
}

func routeLabel(r Route, i int) string {
	if r.ID != "" {
		return r.ID
	}
	if r.Name != "" {
		return r.Name
	}
	return fmt.Sprintf("route-%d", i+1)
}

func allowedScheme(s string) bool {
	switch strings.ToLower(s) {
	case "http", "https", "ws", "wss", "grpc", "grpcs":
		return true
	default:
		return false
	}
}

// ProfileNames returns deterministic profile IDs, useful for management APIs.
func (c *Config) ProfileNames() []string {
	if c == nil {
		return nil
	}
	ids := make([]string, 0, len(c.Profiles))
	for id := range c.Profiles {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func (c *Config) HasProfile(id string) bool {
	if c == nil {
		return false
	}
	_, ok := c.Profiles[id]
	return ok
}
