package labui_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/brenokcunha/light-pix/internal/labui"
)

func backend(t *testing.T, name string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, name+" "+r.Method+" "+r.URL.RequestURI())
	}))
	t.Cleanup(srv.Close)
	return srv
}

func get(t *testing.T, url string) (*http.Response, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp, string(body)
}

func TestHandlerServesPageAndProxiesBothPSPs(t *testing.T) {
	a, b := backend(t, "A"), backend(t, "B")
	h, err := labui.Handler(labui.Config{PSPA: a.URL, PSPB: b.URL})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, page := get(t, srv.URL+"/")
	if resp.StatusCode != http.StatusOK || !strings.Contains(page, "<html") {
		t.Fatalf("página: %d", resp.StatusCode)
	}
	if cc := resp.Header.Get("Cache-Control"); cc != "no-store" {
		t.Errorf("Cache-Control = %q", cc)
	}

	for path, want := range map[string]string{
		"/api/a/v1/accounts?x=1":      "A GET /v1/accounts?x=1",
		"/api/b/v1/directory/keys?q=": "B GET /v1/directory/keys?q=",
	} {
		resp, body := get(t, srv.URL+path)
		if resp.StatusCode != http.StatusOK || body != want {
			t.Errorf("%s → %d %q, esperava %q", path, resp.StatusCode, body, want)
		}
		if cc := resp.Header.Get("Cache-Control"); cc != "no-store" {
			t.Errorf("%s: Cache-Control = %q", path, cc)
		}
	}

	if resp, _ := get(t, srv.URL+"/nao-existe"); resp.StatusCode != http.StatusNotFound {
		t.Errorf("rota desconhecida: %d", resp.StatusCode)
	}
}

func TestHandlerRejectsBadURL(t *testing.T) {
	for _, cfg := range []labui.Config{{PSPA: "", PSPB: "http://b:8080"}, {PSPA: "http://a:8080", PSPB: "psp-b"}} {
		if _, err := labui.Handler(cfg); err == nil {
			t.Errorf("%+v: esperava erro", cfg)
		}
	}
}
