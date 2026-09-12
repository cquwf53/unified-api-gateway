package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"unified-api-gateway/internal/config"
	"unified-api-gateway/internal/transform"
)

func TestConsoleCanLoadBeforeAdminAuthentication(t *testing.T) {
	t.Setenv("GATEWAY_ADMIN_TOKEN", "test-admin")
	gateway := testGateway(t, nil)
	t.Cleanup(gateway.Close)
	for _, path := range []string{"/", "/index.html", "/app.js", "/styles.css"} {
		t.Run(path, func(t *testing.T) {
			response := httptest.NewRecorder()
			gateway.ServeAdminHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
			if response.Code != http.StatusOK {
				t.Fatalf("initial browser navigation %s returned %d", path, response.Code)
			}
		})
	}
	for _, path := range []string{"/api/status", "/api/config", "/api/activate", "/custom.js"} {
		response := httptest.NewRecorder()
		gateway.ServeAdminHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusUnauthorized {
			t.Errorf("unauthenticated %s returned %d", path, response.Code)
		}
	}
}

func TestExplainUsesSimulatedRequestAndRejectsMalformedJSON(t *testing.T) {
	t.Setenv("GATEWAY_ADMIN_TOKEN", "test-admin")
	gateway := testGateway(t, func(cfg *config.Config) {
		cfg.Routes = []config.Route{{
			ID: "chat", Profile: "preview", Priority: 100,
			Match: config.RouteMatch{Path: "/v1/chat/completions", Methods: []string{http.MethodPost}},
		}}
	})
	t.Cleanup(gateway.Close)
	for _, tc := range []struct {
		name, method, path, body string
		status                   int
	}{
		{"query", http.MethodGet, "/api/explain?path=/v1/chat/completions&method=POST", "", http.StatusOK},
		{"json", http.MethodPost, "/admin/v1/routes/explain", `{"path":"/v1/chat/completions","method":"POST","headers":{"X-Demo":["value"]}}`, http.StatusOK},
		{"invalid headers", http.MethodPost, "/api/explain", `{"path":"/v1/chat/completions","method":"POST","headers":{"X-Demo":"value"}}`, http.StatusBadRequest},
		{"invalid json", http.MethodPost, "/api/explain", `{`, http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("X-Admin-Token", "test-admin")
			response := httptest.NewRecorder()
			gateway.ServeAdminHTTP(response, request)
			if response.Code != tc.status {
				t.Fatalf("status=%d body=%s", response.Code, response.Body)
			}
			if tc.status != http.StatusOK {
				return
			}
			var result struct {
				SelectedRoute *config.Route `json:"selected_route"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
			if result.SelectedRoute == nil || result.SelectedRoute.ID != "chat" {
				t.Fatalf("wrong simulated route: %s", response.Body)
			}
		})
	}
}

func TestAdminResponsesRedactTransformHeaders(t *testing.T) {
	t.Setenv("GATEWAY_ADMIN_TOKEN", "test-admin")
	gateway := testGateway(t, func(cfg *config.Config) {
		profile := cfg.Profiles["preview"]
		profile.Request.Headers.Set = map[string]string{"Authorization": "Bearer test-transform-secret", "X-Custom-Context": "test-custom-secret", "X-Public": "visible"}
		cfg.Profiles["preview"] = profile
		cfg.Transforms = map[string]transform.TransformSpec{"shared": profile.Request}
		cfg.Policies = map[string]any{"audit": map[string]any{"redact": []any{"X-Custom-Context"}}}
		cfg.DefaultsProfile = "preview"
	})
	t.Cleanup(gateway.Close)
	for _, path := range []string{"/api/config", "/api/explain?path=/example&method=POST"} {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("X-Admin-Token", "test-admin")
		response := httptest.NewRecorder()
		gateway.ServeAdminHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("%s status=%d", path, response.Code)
		}
		body := response.Body.String()
		if strings.Contains(body, "test-transform-secret") || strings.Contains(body, "test-custom-secret") {
			t.Errorf("%s returned a configured transform secret", path)
		}
		if !strings.Contains(body, "visible") || !strings.Contains(body, "[REDACTED]") {
			t.Errorf("%s did not preserve safe fields and redact sensitive fields", path)
		}
	}
	if gateway.Snapshot().Config.Profiles["preview"].Request.Headers.Set["Authorization"] != "Bearer test-transform-secret" {
		t.Fatal("display redaction mutated the active configuration")
	}
}
