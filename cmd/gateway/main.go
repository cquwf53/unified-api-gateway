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
	adminAddr := flags.String("admin", "", "override management listen address")
	adminURL := flags.String("admin-url", "http://127.0.0.1:8781", "running management API URL for hot activation")
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
		fmt.Printf("valid: version=%d profiles=%d routes=%d upstreams=%d\n", cfg.Version, len(cfg.Profiles), len(cfg.Routes), len(cfg.Upstreams))
		return nil
	case "status":
		return printStatus(*dbPath)
	case "rollback":
		if *versionID == "" {
			return errors.New("rollback requires --id")
		}
		return rollback(*dbPath, *versionID)
	case "activate":
		return activate(*configPath, *dbPath, *adminURL)
	case "preview":
		return preview(*configPath)
	case "serve":
		return serve(*configPath, *dbPath, *httpAddr, *adminAddr)
	default:
		return fmt.Errorf("unknown command %q (use serve, validate, preview, activate, rollback, or status)", command)
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

func serve(configPath, dbPath, httpOverride, adminOverride string) error {
	snapshot, data, err := loadSnapshot(configPath)
	if err != nil {
		return err
	}
	if httpOverride != "" {
		snapshot.Config.Listeners.HTTP = httpOverride
	}
	if adminOverride != "" {
		snapshot.Config.Listeners.Admin = adminOverride
	}
	if httpOverride != "" || adminOverride != "" {
		snapshot, err = config.NewSnapshot(snapshot.Config, configPath+" (listener override)")
		if err != nil {
			return err
		}
	}
	storeDB, err := store.Open(dbPath)
	if err != nil {
		return err
	}
	defer storeDB.Close()
	applyLogLevel(snapshot.Config.Observability.LogLevel)
	slog.Info("gateway starting", "version", server.BuildVersion, "config_id", snapshot.ID)
	if err := storeDB.SaveVersion(context.Background(), store.Version{ID: snapshot.ID, CreatedBy: "startup", Active: true, SHA256: snapshot.Hash, YAML: data}); err != nil {
		slog.Warn("save startup version", "error", err)
	}
	if err := storeDB.Activate(context.Background(), snapshot.ID); err != nil {
		slog.Warn("activate startup version in history", "error", err)
	}
	gateway, err := server.New(server.Options{Snapshot: snapshot, Store: storeDB, ConfigPath: configPath})
	if err != nil {
		return err
	}
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
	select {
	case <-ctx.Done():
		slog.Info("shutting down")
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = dataServer.Shutdown(shutdownCtx)
	_ = adminServer.Shutdown(shutdownCtx)
	if grpcServer != nil {
		_ = grpcServer.Shutdown(shutdownCtx)
	}
	return nil
}

func activate(configPath, dbPath, adminURL string) error {
	snapshot, data, err := loadSnapshot(configPath)
	if err != nil {
		return err
	}
	payload, _ := json.Marshal(map[string]any{"yaml": string(data), "description": "CLI activation", "actor": "cli"})
	if request, requestErr := http.NewRequest(http.MethodPost, strings.TrimRight(adminURL, "/")+"/admin/v1/config/activate", bytes.NewReader(payload)); requestErr == nil {
		request.Header.Set("Content-Type", "application/json")
		if response, doErr := (&http.Client{Timeout: 5 * time.Second}).Do(request); doErr == nil {
			defer response.Body.Close()
			if response.StatusCode >= 200 && response.StatusCode < 300 {
				fmt.Printf("hot activated %s\n", snapshot.ID)
				return nil
			}
		}
	}
	db, err := store.Open(dbPath)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.SaveVersion(context.Background(), store.Version{ID: snapshot.ID, CreatedBy: "cli", Active: true, SHA256: snapshot.Hash, YAML: data}); err != nil {
		return err
	}
	if err := db.Activate(context.Background(), snapshot.ID); err != nil {
		return err
	}
	fmt.Printf("saved %s for next startup (running management API was unavailable)\n", snapshot.ID)
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
