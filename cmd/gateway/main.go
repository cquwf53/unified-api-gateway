package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"unified-api-gateway/internal/config"

	"gopkg.in/yaml.v3"
	"unified-api-gateway/internal/server"
	"unified-api-gateway/internal/store"
)

const defaultConfigPath = "configs/gateway.yaml"

func main() {
	if err := run(os.Args[1:]); err != nil {
		slog.Error("gateway stopped", "error", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	command := "serve"
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		command = args[0]
		args = args[1:]
	}
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	configPath := flags.String("config", defaultConfigPath, "YAML configuration path")
	dbPath := flags.String("db", "data/gateway.db", "SQLite audit/history path")
	httpAddr := flags.String("http", "", "override data-plane listen address")
	preferFile := flags.Bool("prefer-file", false, "always start from the --config file, ignoring any stored active version")
	adminAddr := flags.String("admin", "", "override management listen address")
	adminURL := flags.String("admin-url", "http://127.0.0.1:8781", "running management API URL for hot activation")
	adminToken := flags.String("admin-token", "", "management API token (defaults to $GATEWAY_ADMIN_TOKEN)")
	versionID := flags.String("id", "", "configuration version id")
	if err := flags.Parse(args); err != nil {
		return err
	}

	switch command {
	case "validate":
		cfg, err := config.LoadFile(*configPath)
		if err != nil {
			return err
		}
		for _, warning := range cfg.ShadowedRoutes() {
			fmt.Printf("warning: %s\n", warning)
		}
		fmt.Printf("valid: version=%d profiles=%d routes=%d upstreams=%d\n", cfg.Version, len(cfg.Profiles), len(cfg.Routes), len(cfg.Upstreams))
		return nil
	case "status":
		return printStatus(*dbPath)
	case "health":
		return health(*adminURL)
	case "version":
		fmt.Println(server.BuildVersion)
		return nil
	case "rollback":
		if *versionID == "" {
			return errors.New("rollback requires --id")
		}
		return rollback(*dbPath, *versionID)
	case "activate":
		return activate(*configPath, *dbPath, *adminURL, *adminToken)
	case "preview":
		return preview(*configPath)
	case "serve":
		return serve(*configPath, *dbPath, *httpAddr, *adminAddr, *preferFile)
	default:
		return fmt.Errorf("unknown command %q (use serve, validate, preview, activate, rollback, status, health, or version)", command)
	}
}

func loadSnapshot(configPath string) (*config.ConfigSnapshot, []byte, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, nil, fmt.Errorf("read config: %w", err)
	}
	cfg, err := config.Parse(data)
	if err != nil {
		return nil, nil, err
	}
	snapshot, err := config.NewSnapshot(cfg, configPath)
	if err != nil {
		return nil, nil, err
	}
	return snapshot, data, nil
}

func serve(configPath, dbPath, httpOverride, adminOverride string, preferFile bool) error {
	baseSnapshot, data, err := loadSnapshot(configPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		// A corrupt file is fatal; a missing file may still be recoverable
		// from the stored active configuration below.
		return err
	}
	fileLoadFailed := err != nil
	storeDB, err := store.Open(dbPath)
	if err != nil {
		return err
	}
	defer storeDB.Close()
	// A CLI activation saved while the management plane was down was stored
	// "for next startup"; honour it instead of silently reverting to the
	// on-disk config file. data must follow the snapshot: saving the file
	// YAML under the restored version's id would overwrite the stored
	// configuration (SaveVersion is INSERT OR REPLACE).
	if active, activeErr := storeDB.Active(context.Background()); activeErr == nil && len(active.YAML) > 0 && !preferFile {
		if baseSnapshot != nil && active.SHA256 != "" && active.SHA256 != baseSnapshot.Hash {
			slog.Warn("stored active configuration differs from the config file; the stored version wins (use --prefer-file to change this)", "stored_id", active.ID, "file_id", baseSnapshot.ID)
		}
		if activeCfg, parseErr := config.Parse(active.YAML); parseErr == nil {
			if activeSnapshot, snapErr := config.NewSnapshot(activeCfg, "store:"+active.ID); snapErr == nil {
				if active.SHA256 != "" && active.SHA256 != activeSnapshot.Hash || active.ID != activeSnapshot.ID {
					slog.Warn("stored active configuration failed its content-address integrity check; using the config file", "stored_id", active.ID, "computed_id", activeSnapshot.ID)
				} else {
					baseSnapshot, data = activeSnapshot, active.YAML
					slog.Info("restored configuration saved by a previous activation", "id", active.ID, "created_by", active.CreatedBy)
				}
			}
		} else {
			slog.Warn("stored active configuration no longer parses; using the config file", "id", active.ID, "error", parseErr)
		}
	}
	if fileLoadFailed && (baseSnapshot == nil || baseSnapshot.Config == nil) {
		return errors.New("config file could not be loaded and no stored active configuration is available")
	}
	// Fail closed: require_client_auth with no resolvable key refuses to serve.
	if policiesDecodeRequireClientAuth(baseSnapshot) && !server.ClientAuthKeysConfigured(baseSnapshot.Config) {
		return errors.New("require_client_auth is enabled but no client credential resolves to a key; refusing to start fail-open")
	}
	// Persist the version exactly as it will run; listener overrides below
	// are process-local and deliberately excluded from the stored YAML.
	if err := baseSnapshot.Config.ValidatePersistable(); err != nil {
		return fmt.Errorf("configuration cannot be persisted: %w", err)
	}
	applyLogLevel(baseSnapshot.Config.Observability.LogLevel)
	slog.Info("gateway starting", "version", server.BuildVersion, "config_id", baseSnapshot.ID)
	if err := storeDB.SaveVersionAndActivate(context.Background(), store.Version{ID: baseSnapshot.ID, CreatedBy: "startup", SHA256: baseSnapshot.Hash, YAML: data}); err != nil {
		slog.Warn("activate startup version in history", "error", err)
	}
	// Command-line listener overrides are re-applied after the restore so
	// they survive a stored active configuration.
	snapshot := baseSnapshot
	if httpOverride != "" {
		snapshot.Config.Listeners.HTTP = httpOverride
	}
	if adminOverride != "" {
		snapshot.Config.Listeners.Admin = adminOverride
	}
	if httpOverride != "" || adminOverride != "" {
		if snapshot, err = config.NewSnapshot(snapshot.Config, configPath+" (listener override)"); err != nil {
			return err
		}
	}
	gateway, err := server.New(server.Options{Snapshot: snapshot, Store: storeDB, ConfigPath: configPath})
	if err != nil {
		return err
	}
	// Deferred in reverse order: flush audit writes before closing the store.
	defer gateway.Close()
	// WriteTimeout stays 0 on the data plane: it also covers streaming
	// response bodies, and long completions routinely exceed any fixed bound.
	// The admin plane has no streaming endpoints and keeps a hard cap.
	dataServer := &http.Server{Addr: snapshot.Config.Listeners.HTTP, Handler: http.HandlerFunc(gateway.ServeDataHTTP), ReadHeaderTimeout: 15 * time.Second, ReadTimeout: 120 * time.Second, WriteTimeout: 0, IdleTimeout: 120 * time.Second}
	adminServer := &http.Server{Addr: snapshot.Config.Listeners.Admin, Handler: http.HandlerFunc(gateway.ServeAdminHTTP), ReadHeaderTimeout: 15 * time.Second, ReadTimeout: 120 * time.Second, WriteTimeout: 120 * time.Second, IdleTimeout: 120 * time.Second}
	var grpcServer *http.Server
	if snapshot.Config.Listeners.GRPC != "" {
		grpcServer = &http.Server{Addr: snapshot.Config.Listeners.GRPC, Handler: http.HandlerFunc(gateway.ServeGRPCHTTP), ReadHeaderTimeout: 15 * time.Second, ReadTimeout: 120 * time.Second, WriteTimeout: 0, IdleTimeout: 120 * time.Second}
	}

	errCh := make(chan error, 3)
	go func() {
		slog.Info("data plane listening", "address", dataServer.Addr)
		errCh <- dataServer.ListenAndServe()
	}()
	go func() {
		slog.Info("management plane listening", "address", adminServer.Addr)
		errCh <- adminServer.ListenAndServe()
	}()
	if grpcServer != nil {
		go func() {
			slog.Info("gRPC transparent listener", "address", grpcServer.Addr)
			errCh <- grpcServer.ListenAndServe()
		}()
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var serveErr error
	select {
	case <-ctx.Done():
		slog.Info("shutting down")
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			serveErr = err
		}
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = dataServer.Shutdown(shutdownCtx)
	_ = adminServer.Shutdown(shutdownCtx)
	if grpcServer != nil {
		_ = grpcServer.Shutdown(shutdownCtx)
	}
	return serveErr
}

func activate(configPath, dbPath, adminURL, adminToken string) error {
	snapshot, data, err := loadSnapshot(configPath)
	if err != nil {
		return err
	}
	if adminToken == "" {
		adminToken = strings.TrimSpace(os.Getenv("GATEWAY_ADMIN_TOKEN"))
	}
	payload, _ := json.Marshal(map[string]any{"yaml": string(data), "description": "CLI activation", "actor": "cli"})
	fallbackReason := "running management API was unavailable"
	request, requestErr := http.NewRequest(http.MethodPost, strings.TrimRight(adminURL, "/")+"/admin/v1/config/activate", bytes.NewReader(payload))
	if requestErr != nil {
		return fmt.Errorf("build management API request: %w", requestErr)
	}
	request.Header.Set("Content-Type", "application/json")
	if adminToken != "" {
		request.Header.Set("X-Admin-Token", adminToken)
	}
	client := &http.Client{
		Timeout: 5 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	if response, doErr := client.Do(request); doErr == nil {
		defer response.Body.Close()
		if response.StatusCode >= 200 && response.StatusCode < 300 {
			fmt.Printf("hot activated %s\n", snapshot.ID)
			return nil
		}
		// Any definitive management-plane reply outside 2xx—including a
		// redirect—must not degrade into a local save that pretends success.
		return fmt.Errorf("management API rejected the activation (HTTP %d); nothing was saved", response.StatusCode)
	}
	db, err := store.Open(dbPath)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := snapshot.Config.ValidatePersistable(); err != nil {
		return fmt.Errorf("configuration cannot be persisted: %w", err)
	}
	if err := db.SaveVersionAndActivate(context.Background(), store.Version{ID: snapshot.ID, CreatedBy: "cli", SHA256: snapshot.Hash, YAML: data}); err != nil {
		return err
	}
	fmt.Printf("saved %s for next startup (%s)\n", snapshot.ID, fallbackReason)
	return nil
}

func rollback(dbPath, id string) error {
	db, err := store.Open(dbPath)
	if err != nil {
		return err
	}
	defer db.Close()
	version, err := db.GetVersion(context.Background(), id)
	if err != nil {
		return err
	}
	cfg, err := config.Parse(version.YAML)
	if err != nil {
		return err
	}
	snapshot, err := config.NewSnapshot(cfg, "rollback:"+id)
	if err != nil {
		return err
	}
	if version.ID != snapshot.ID || version.SHA256 != "" && version.SHA256 != snapshot.Hash {
		return fmt.Errorf("stored version %q failed its content-address integrity check", id)
	}
	if err := db.Activate(context.Background(), id); err != nil {
		return err
	}
	fmt.Printf("rolled back to %s (validated snapshot %s)\n", id, snapshot.ID)
	return nil
}

func preview(configPath string) error {
	snapshot, _, err := loadSnapshot(configPath)
	if err != nil {
		return err
	}
	b, _ := json.MarshalIndent(snapshot, "", "  ")
	fmt.Println(string(b))
	return nil
}

func printStatus(dbPath string) error {
	db, err := store.Open(dbPath)
	if err != nil {
		return err
	}
	defer db.Close()
	items, err := db.ListVersions(context.Background(), 20)
	if err != nil {
		return err
	}
	b, _ := json.MarshalIndent(map[string]any{"gateway_version": server.BuildVersion, "versions": items}, "", "  ")
	fmt.Println(string(b))
	return nil
}

func health(rawURL string) error {
	if strings.TrimSpace(rawURL) == "" {
		rawURL = "http://127.0.0.1:8781"
	}
	rawURL = strings.TrimRight(rawURL, "/")
	if !strings.HasSuffix(rawURL, "/readyz") {
		rawURL += "/readyz"
	}
	client := &http.Client{
		Timeout: 3 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	response, err := client.Get(rawURL)
	if err != nil {
		return fmt.Errorf("health check failed: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("health check failed: HTTP %d", response.StatusCode)
	}
	return nil
}

// policiesDecodeRequireClientAuth reads policies.defaults.require_client_auth
// without importing the server package's runtime policy engine.
func policiesDecodeRequireClientAuth(snapshot *config.ConfigSnapshot) bool {
	if snapshot == nil || snapshot.Config == nil {
		return false
	}
	var spec struct {
		Defaults struct {
			Require *bool `yaml:"require_client_auth"`
		} `yaml:"defaults"`
	}
	raw, err := yaml.Marshal(snapshot.Config.Policies)
	if err != nil {
		return false
	}
	if err := yaml.Unmarshal(raw, &spec); err != nil {
		return false
	}
	return spec.Defaults.Require != nil && *spec.Defaults.Require
}

// applyLogLevel maps observability.log_level onto the default logger.
func applyLogLevel(level string) {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		slog.SetLogLoggerLevel(slog.LevelDebug)
	case "warn", "warning":
		slog.SetLogLoggerLevel(slog.LevelWarn)
	case "error":
		slog.SetLogLoggerLevel(slog.LevelError)
	default:
		slog.SetLogLoggerLevel(slog.LevelInfo)
	}
}
