package server

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

// Opt-in real-browser fixture. Open the printed URL in an isolated browser
// context; both listeners use random loopback ports and only an in-memory config.
func TestAdminBrowserOriginBoundary(t *testing.T) {
	if os.Getenv("UAG_BROWSER_TEST") != "1" {
		t.Skip("set UAG_BROWSER_TEST=1 and open the printed URL to exercise a browser")
	}
	t.Setenv("GATEWAY_ADMIN_TOKEN", "")
	t.Setenv("GATEWAY_ADMIN_ORIGIN", "")
	gateway := adminBoundaryGateway(t)
	type result struct {
		status                            int
		origin, contentType, site, active string
	}
	received := make(chan result, 2)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/browser-same-origin" {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			fmt.Fprint(w, `<!doctype html><title>Gateway browser verification</title><p id="result">Checking same-origin JSON request...</p><script>fetch('/api/switch',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({profile:'alternate'})}).then(async r=>{document.getElementById('result').textContent='Same-origin status: '+r.status;await r.text();window.close()})</script>`)
			return
		}
		response := httptest.NewRecorder()
		gateway.ServeAdminHTTP(response, r)
		for key, values := range response.Header() {
			w.Header()[key] = values
		}
		w.WriteHeader(response.Code)
		_, _ = w.Write(response.Body.Bytes())
		if r.URL.Path == "/api/switch" {
			select {
			case received <- result{response.Code, r.Header.Get("Origin"), r.Header.Get("Content-Type"), r.Header.Get("Sec-Fetch-Site"), gateway.Snapshot().Config.DefaultsProfile}:
			default:
			}
		}
	}))
	defer target.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprintf(w, `<!doctype html><title>Gateway cross-origin verification</title><iframe name="result" id="frame"></iframe><form id="form" method="POST" enctype="text/plain" target="result" action="%s/api/switch"><input name='{"profile":"alternate"}' value=""></form><script>window.addEventListener('load',()=>{document.getElementById('frame').onload=()=>location.replace('%s/browser-same-origin');document.getElementById('form').submit()})</script>`, target.URL, target.URL)
	}))
	defer source.Close()
	t.Logf("BROWSER_SOURCE=%s", strings.Replace(source.URL, "127.0.0.1", "localhost", 1))
	for index, want := range []result{{status: 403, active: "preview"}, {status: 200, active: "alternate"}} {
		select {
		case got := <-received:
			t.Logf("BROWSER_RESULT stage=%d status=%d origin=%q content_type=%q fetch_site=%q active=%q", index, got.status, got.origin, got.contentType, got.site, got.active)
			if got.status != want.status || got.active != want.active {
				t.Fatalf("browser stage=%d got=%+v want=%+v", index, got, want)
			}
		case <-time.After(45 * time.Second):
			t.Fatalf("browser did not complete stage %d", index)
		}
	}
}
