// Package labui serve a interface de testes do laboratório: uma página com uma
// aba para o banco pagador e outra para o recebedor. A página fala só com este
// servidor, que repassa as chamadas para a API de cada PSP (/api/a/ e /api/b/),
// o que evita configurar CORS nos PSPs.
package labui

import (
	"embed"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
)

//go:embed web
var webFS embed.FS

// Config diz onde estão as APIs dos dois PSPs.
type Config struct {
	PSPA string // ex.: http://psp-a-api:8080
	PSPB string
}

// Handler devolve a página e o proxy das APIs.
func Handler(cfg Config) (http.Handler, error) {
	site, err := fs.Sub(webFS, "web")
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.Handle("/", http.FileServerFS(site))
	for prefix, raw := range map[string]string{"/api/a/": cfg.PSPA, "/api/b/": cfg.PSPB} {
		target, err := url.Parse(raw)
		if err != nil || target.Scheme == "" || target.Host == "" {
			return nil, fmt.Errorf("labui: URL inválida para %s: %q", prefix, raw)
		}
		proxy := httputil.NewSingleHostReverseProxy(target)
		mux.Handle(prefix, http.StripPrefix(strings.TrimSuffix(prefix, "/"), proxy))
	}
	return noStore(mux), nil
}

// noStore impede o navegador de guardar respostas: a página mostra saldos e
// estados que mudam a cada segundo.
func noStore(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		next.ServeHTTP(w, r)
	})
}
