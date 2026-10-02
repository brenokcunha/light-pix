// Package faults permite injetar falhas num simulador em tempo de execução:
// latência (fixa ou com distribuição), taxa de erros HTTP, indisponibilidade
// total e, no SPI, perda de pacs.002 e recebedor que não responde. A
// configuração vive em memória e muda por /admin/faults, sem reiniciar.
package faults

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"net/http"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/brenokcunha/light-pix/internal/httpx"
)

// Modos de latência.
const (
	LatencyNone      = ""
	LatencyFixed     = "fixed"     // sempre ms
	LatencyUniform   = "uniform"   // entre minMs e maxMs
	LatencyLognormal = "lognormal" // mediana p50Ms e percentil 99 p99Ms: cauda longa, como em serviço real
)

// maxLatency limita a espera injetada, para um erro de digitação não deixar o
// simulador mudo por minutos.
const maxLatency = 60 * time.Second

// Latency descreve a espera injetada antes de responder.
type Latency struct {
	Mode  string  `json:"mode,omitempty"`
	Ms    float64 `json:"ms,omitempty"`    // fixed
	MinMs float64 `json:"minMs,omitempty"` // uniform
	MaxMs float64 `json:"maxMs,omitempty"` // uniform
	P50Ms float64 `json:"p50Ms,omitempty"` // lognormal
	P99Ms float64 `json:"p99Ms,omitempty"` // lognormal
}

// Config é o conjunto de falhas ativas. O zero é "sem falhas".
type Config struct {
	// Latency vale para as requisições afetadas (ver Paths).
	Latency Latency `json:"latency"`
	// ErrorRate é a fração (0 a 1) das requisições respondidas com ErrorStatus.
	ErrorRate float64 `json:"errorRate"`
	// ErrorStatus é o status dos erros injetados (padrão 503).
	ErrorStatus int `json:"errorStatus,omitempty"`
	// Unavailable responde 503 a tudo, como um serviço fora do ar.
	Unavailable bool `json:"unavailable"`
	// Paths restringe latência e erros às rotas com esses prefixos de caminho;
	// vazio afeta todas (exceto /admin, /healthz e /readyz).
	Paths []string `json:"paths,omitempty"`

	// DropPacs002 (só SPI) é a fração das pacs.002 que o SPI nunca entrega.
	DropPacs002 float64 `json:"dropPacs002"`
	// CreditorTimeoutRate (só SPI) é a fração das pacs.008 que o SPI não
	// encaminha ao recebedor, como se ele nunca respondesse: o pagador recebe
	// RJCT AB03 ao fim do prazo.
	CreditorTimeoutRate float64 `json:"creditorTimeoutRate"`
}

// Validate confere os limites e preenche os padrões.
func (c *Config) Validate() error {
	for name, v := range map[string]float64{"errorRate": c.ErrorRate, "dropPacs002": c.DropPacs002, "creditorTimeoutRate": c.CreditorTimeoutRate} {
		if v < 0 || v > 1 || math.IsNaN(v) {
			return fmt.Errorf("%s deve estar entre 0 e 1", name)
		}
	}
	if c.ErrorStatus == 0 {
		c.ErrorStatus = http.StatusServiceUnavailable
	}
	if c.ErrorStatus < 400 || c.ErrorStatus > 599 {
		return errors.New("errorStatus deve ser um status 4xx ou 5xx")
	}
	l := c.Latency
	switch l.Mode {
	case LatencyNone:
	case LatencyFixed:
		if l.Ms < 0 || l.Ms > float64(maxLatency.Milliseconds()) {
			return errors.New("latency.ms deve estar entre 0 e 60000")
		}
	case LatencyUniform:
		if l.MinMs < 0 || l.MaxMs < l.MinMs || l.MaxMs > float64(maxLatency.Milliseconds()) {
			return errors.New("latency: 0 ≤ minMs ≤ maxMs ≤ 60000")
		}
	case LatencyLognormal:
		if l.P50Ms <= 0 || l.P99Ms < l.P50Ms || l.P99Ms > float64(maxLatency.Milliseconds()) {
			return errors.New("latency: 0 < p50Ms ≤ p99Ms ≤ 60000")
		}
	default:
		return fmt.Errorf("latency.mode inválido %q (use fixed, uniform ou lognormal)", l.Mode)
	}
	return nil
}

// Sample sorteia uma espera conforme o modo.
func (l Latency) Sample() time.Duration {
	var ms float64
	switch l.Mode {
	case LatencyFixed:
		ms = l.Ms
	case LatencyUniform:
		ms = l.MinMs + rand.Float64()*(l.MaxMs-l.MinMs)
	case LatencyLognormal:
		// p99 = p50·exp(2,326·σ): resolve σ a partir dos dois percentis.
		sigma := math.Log(l.P99Ms/l.P50Ms) / 2.326
		ms = l.P50Ms * math.Exp(sigma*rand.NormFloat64())
	}
	return min(time.Duration(ms*float64(time.Millisecond)), maxLatency)
}

// State guarda as falhas ativas e conta as injetadas.
type State struct {
	cfg      atomic.Pointer[Config]
	injected metric.Int64Counter
}

// NewState cria o estado, sem falhas, e registra a métrica faults.injected.
func NewState() (*State, error) {
	s := &State{}
	s.cfg.Store(&Config{ErrorStatus: http.StatusServiceUnavailable})
	var err error
	s.injected, err = otel.Meter("github.com/brenokcunha/light-pix/internal/faults").Int64Counter("faults.injected",
		metric.WithDescription("Falhas injetadas, por tipo"))
	return s, err
}

// Get devolve a configuração atual (não modifique o resultado).
func (s *State) Get() Config {
	if s == nil {
		return Config{}
	}
	return *s.cfg.Load()
}

// Set valida e ativa a configuração.
func (s *State) Set(c Config) error {
	if err := c.Validate(); err != nil {
		return err
	}
	s.cfg.Store(&c)
	slog.Warn("falhas injetadas reconfiguradas", "faults", c)
	return nil
}

// Roll sorteia com probabilidade p e, se acertar, conta a falha como kind.
// Serve às falhas específicas de um simulador (ex.: dropPacs002). Em um *State
// nil nunca acerta.
func (s *State) Roll(ctx context.Context, p float64, kind string) bool {
	if s == nil || p <= 0 || rand.Float64() >= p {
		return false
	}
	s.count(ctx, kind)
	return true
}

func (s *State) count(ctx context.Context, kind string) {
	s.injected.Add(ctx, 1, metric.WithAttributes(attribute.String("kind", kind)))
}

// exempt diz se o caminho nunca recebe falhas: a administração (para poder
// desligar as falhas) e as sondas de saúde (para o orquestrador não matar o
// simulador por uma falha de mentira).
func exempt(path string) bool {
	return strings.HasPrefix(path, "/admin") || path == "/healthz" || path == "/readyz"
}

// Wrap devolve um handler que aplica as falhas HTTP antes de repassar a
// requisição ao mux. Deve ficar dentro da instrumentação (para o span existir)
// e recebe o mux para nomear o span da requisição que ele mesmo encerra.
func (s *State) Wrap(mux *http.ServeMux) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cfg := s.cfg.Load()
		if exempt(r.URL.Path) || !affects(cfg, r.URL.Path) {
			mux.ServeHTTP(w, r)
			return
		}
		ctx := r.Context()
		span := trace.SpanFromContext(ctx)

		if cfg.Latency.Mode != LatencyNone {
			if d := cfg.Latency.Sample(); d > 0 {
				s.count(ctx, "latency")
				span.SetAttributes(attribute.Int64("fault.latency_ms", d.Milliseconds()))
				select {
				case <-time.After(d):
				case <-ctx.Done():
					return
				}
			}
		}

		status, kind := 0, ""
		switch {
		case cfg.Unavailable:
			status, kind = http.StatusServiceUnavailable, "unavailable"
		case cfg.ErrorRate > 0 && rand.Float64() < cfg.ErrorRate:
			status, kind = cfg.ErrorStatus, "error"
		}
		if status == 0 {
			mux.ServeHTTP(w, r)
			return
		}
		s.count(ctx, kind)
		span.SetAttributes(attribute.String("fault.injected", kind))
		// A requisição não chega ao mux: o padrão da rota é achado aqui para o
		// span e o log continuarem com nome de rota.
		if _, pattern := mux.Handler(r); pattern != "" {
			span.SetName(pattern)
		}
		httpx.WriteError(w, status, "FAULT_INJECTED", "falha injetada ("+kind+")")
	})
}

func affects(c *Config, path string) bool {
	return len(c.Paths) == 0 || slices.ContainsFunc(c.Paths, func(p string) bool { return strings.HasPrefix(path, p) })
}

// Routes registra a API de administração das falhas:
//
//	GET    /admin/faults   configuração atual
//	PUT    /admin/faults   substitui a configuração (JSON de Config)
//	DELETE /admin/faults   desliga todas as falhas
func (s *State) Routes(mux *http.ServeMux) {
	mux.HandleFunc("GET /admin/faults", func(w http.ResponseWriter, r *http.Request) {
		httpx.WriteJSON(w, http.StatusOK, s.Get())
	})
	mux.HandleFunc("PUT /admin/faults", func(w http.ResponseWriter, r *http.Request) {
		var c Config
		if err := httpx.DecodeJSON(w, r, &c); err != nil {
			httpx.WriteError(w, http.StatusBadRequest, "INVALID_REQUEST", err.Error())
			return
		}
		if err := s.Set(c); err != nil {
			httpx.WriteError(w, http.StatusBadRequest, "INVALID_REQUEST", err.Error())
			return
		}
		httpx.WriteJSON(w, http.StatusOK, s.Get())
	})
	mux.HandleFunc("DELETE /admin/faults", func(w http.ResponseWriter, r *http.Request) {
		_ = s.Set(Config{})
		httpx.WriteJSON(w, http.StatusOK, s.Get())
	})
}

// FromEnv aplica a configuração inicial de uma variável de ambiente com JSON
// (ex.: FAULTS='{"errorRate":0.3}'); vazia não faz nada.
func (s *State) FromEnv(raw string) error {
	if raw == "" {
		return nil
	}
	var c Config
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return fmt.Errorf("FAULTS inválida: %w", err)
	}
	return s.Set(c)
}
