package faults_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/brenokcunha/light-pix/internal/faults"
)

func newServer(t *testing.T) (*faults.State, *httptest.Server) {
	t.Helper()
	st, err := faults.NewState()
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/ping", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /api/v1/other", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	st.Routes(mux)
	srv := httptest.NewServer(st.Wrap(mux))
	t.Cleanup(srv.Close)
	return st, srv
}

func get(t *testing.T, url string) int {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

func TestUnavailableSparesAdminAndHealth(t *testing.T) {
	st, srv := newServer(t)
	if err := st.Set(faults.Config{Unavailable: true}); err != nil {
		t.Fatal(err)
	}
	if got := get(t, srv.URL+"/api/v1/ping"); got != http.StatusServiceUnavailable {
		t.Fatalf("rota normal: %d, quer 503", got)
	}
	for _, p := range []string{"/healthz", "/admin/faults"} {
		if got := get(t, srv.URL+p); got != http.StatusOK {
			t.Fatalf("%s: %d, quer 200 (isenta de falhas)", p, got)
		}
	}
}

func TestErrorRate(t *testing.T) {
	st, srv := newServer(t)
	cfg := faults.Config{ErrorRate: 0.3, ErrorStatus: http.StatusBadGateway}
	if err := st.Set(cfg); err != nil {
		t.Fatal(err)
	}
	const n = 1000
	errs := 0
	for range n {
		switch get(t, srv.URL+"/api/v1/ping") {
		case http.StatusBadGateway:
			errs++
		case http.StatusOK:
		default:
			t.Fatal("status inesperado")
		}
	}
	if errs < 220 || errs > 380 {
		t.Fatalf("%d erros em %d requisições, esperava cerca de 30%%", errs, n)
	}
}

func TestPathsRestrictFaults(t *testing.T) {
	st, srv := newServer(t)
	if err := st.Set(faults.Config{Unavailable: true, Paths: []string{"/api/v1/ping"}}); err != nil {
		t.Fatal(err)
	}
	if got := get(t, srv.URL+"/api/v1/ping"); got != http.StatusServiceUnavailable {
		t.Fatalf("rota afetada: %d", got)
	}
	if got := get(t, srv.URL+"/api/v1/other"); got != http.StatusOK {
		t.Fatalf("rota fora do filtro: %d", got)
	}
}

func TestFixedLatency(t *testing.T) {
	st, srv := newServer(t)
	if err := st.Set(faults.Config{Latency: faults.Latency{Mode: faults.LatencyFixed, Ms: 150}}); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if got := get(t, srv.URL+"/api/v1/ping"); got != http.StatusOK {
		t.Fatalf("status %d", got)
	}
	if took := time.Since(start); took < 150*time.Millisecond {
		t.Fatalf("respondeu em %v, quer ≥ 150ms", took)
	}
}

func TestLatencyDistributions(t *testing.T) {
	uniform := faults.Latency{Mode: faults.LatencyUniform, MinMs: 100, MaxMs: 200}
	for range 200 {
		if d := uniform.Sample(); d < 100*time.Millisecond || d > 200*time.Millisecond {
			t.Fatalf("uniforme fora do intervalo: %v", d)
		}
	}

	// Lognormal: a mediana e o p99 amostrados ficam perto dos pedidos.
	ln := faults.Latency{Mode: faults.LatencyLognormal, P50Ms: 100, P99Ms: 1000}
	const n = 20000
	samples := make([]float64, n)
	for i := range samples {
		samples[i] = float64(ln.Sample().Milliseconds())
	}
	below := func(ms float64) float64 {
		c := 0
		for _, s := range samples {
			if s <= ms {
				c++
			}
		}
		return float64(c) / n
	}
	if p := below(100); p < 0.45 || p > 0.55 {
		t.Errorf("%.1f%% abaixo da mediana pedida, quer ~50%%", p*100)
	}
	if p := below(1000); p < 0.97 || p > 0.995 {
		t.Errorf("%.1f%% abaixo do p99 pedido, quer ~99%%", p*100)
	}
}

func TestAdminAPI(t *testing.T) {
	st, srv := newServer(t)
	put := func(body string) (int, faults.Config) {
		req, _ := http.NewRequest(http.MethodPut, srv.URL+"/admin/faults", bytes.NewBufferString(body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var c faults.Config
		_ = json.NewDecoder(resp.Body).Decode(&c)
		return resp.StatusCode, c
	}

	code, c := put(`{"errorRate":0.5,"latency":{"mode":"fixed","ms":10},"dropPacs002":0.2}`)
	if code != http.StatusOK || c.ErrorRate != 0.5 || c.ErrorStatus != 503 || c.DropPacs002 != 0.2 {
		t.Fatalf("PUT válido: %d %+v", code, c)
	}
	if st.Get().ErrorRate != 0.5 {
		t.Fatal("configuração não foi aplicada")
	}

	for _, bad := range []string{
		`{"errorRate":1.5}`,
		`{"errorStatus":200}`,
		`{"latency":{"mode":"fixed","ms":120000}}`,
		`{"latency":{"mode":"lognormal","p50Ms":500,"p99Ms":100}}`,
		`{"latency":{"mode":"inventada"}}`,
		`{"campoDesconhecido":true}`,
	} {
		if code, _ := put(bad); code != http.StatusBadRequest {
			t.Errorf("PUT %s: %d, quer 400", bad, code)
		}
	}
	if st.Get().ErrorRate != 0.5 {
		t.Fatal("configuração inválida alterou o estado")
	}

	req, _ := http.NewRequest(http.MethodDelete, srv.URL+"/admin/faults", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if got := st.Get(); got.ErrorRate != 0 || got.DropPacs002 != 0 || got.Latency.Mode != "" {
		t.Fatalf("DELETE não limpou: %+v", got)
	}
	if got := get(t, srv.URL+"/api/v1/ping"); got != http.StatusOK {
		t.Fatalf("depois de limpar: %d", got)
	}
}

func TestFromEnv(t *testing.T) {
	st, _ := newServer(t)
	if err := st.FromEnv(`{"unavailable":true}`); err != nil || !st.Get().Unavailable {
		t.Fatalf("FromEnv válido: %v %+v", err, st.Get())
	}
	if err := st.FromEnv(`{"errorRate":2}`); err == nil {
		t.Fatal("FromEnv inválido deveria falhar")
	}
	if err := st.FromEnv(""); err != nil {
		t.Fatal(err)
	}
}
