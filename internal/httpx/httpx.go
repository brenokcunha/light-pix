// Package httpx reúne o que todo servidor HTTP do laboratório precisa:
// instrumentação OTel, health checks e desligamento gracioso.
package httpx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/trace"
)

// Check é uma verificação de dependência usada no /readyz.
type Check func(context.Context) error

// Instrument envolve o handler com traces, métricas HTTP e log de acesso. O
// /healthz fica de fora para não poluir os sinais com as sondas do
// orquestrador.
func Instrument(service string, h http.Handler) http.Handler {
	return otelhttp.NewHandler(accessLog(h), service,
		otelhttp.WithFilter(func(r *http.Request) bool { return r.URL.Path != "/healthz" }),
	)
}

// accessLog registra cada requisição no contexto do span, o que leva
// trace_id ao log e permite navegar do trace para o log e vice-versa.
func accessLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			next.ServeHTTP(w, r)
			return
		}
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		// O padrão da rota só é conhecido depois do roteamento pelo mux.
		if r.Pattern != "" {
			trace.SpanFromContext(r.Context()).SetName(r.Pattern)
		}
		slog.InfoContext(r.Context(), "http request",
			"method", r.Method,
			"route", r.Pattern,
			"status", rec.status,
			"duration_ms", time.Since(start).Milliseconds(),
		)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }

// HandleHealth registra /healthz (liveness: o processo responde) e /readyz
// (readiness: as dependências respondem).
func HandleHealth(mux *http.ServeMux, checks map[string]Check) {
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		status, result := http.StatusOK, map[string]string{}
		for name, check := range checks {
			if err := check(ctx); err != nil {
				status = http.StatusServiceUnavailable
				result[name] = err.Error()
				slog.WarnContext(ctx, "dependência indisponível", "dependency", name, "err", err)
				continue
			}
			result[name] = "ok"
		}
		WriteJSON(w, status, result)
	})
}

// WriteJSON serializa v como resposta JSON.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// Serve roda o servidor até o contexto ser cancelado e então drena as
// requisições em andamento.
func Serve(ctx context.Context, addr string, h http.Handler) error {
	srv := &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 5 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() {
		slog.InfoContext(ctx, "servidor HTTP no ar", "addr", addr)
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		return fmt.Errorf("httpx: serve: %w", err)
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("httpx: shutdown: %w", err)
	}
	if err := <-errCh; !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("httpx: serve: %w", err)
	}
	return nil
}

// Probe faz GET em url e falha se a resposta não for 2xx. Serve de
// healthcheck em imagens sem curl/wget (distroless).
func Probe(ctx context.Context, url string) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("httpx: probe %s: status %d", url, resp.StatusCode)
	}
	return nil
}
