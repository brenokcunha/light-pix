package clients

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/sony/gobreaker/v2"

	"github.com/brenokcunha/light-pix/internal/pix"
	"github.com/brenokcunha/light-pix/internal/resilience"
	"github.com/brenokcunha/light-pix/internal/sim/dict"
)

// ErrDictUnavailable indica que o DICT não respondeu a tempo, esgotou os
// retries ou está com o circuit breaker aberto: falha rápida para o chamador.
var ErrDictUnavailable = errors.New("DICT indisponível")

// DictConfig ajusta o gateway do DICT.
type DictConfig struct {
	Timeout  time.Duration // por tentativa
	Attempts int           // tentativas por consulta (1 = sem retry)
	CacheTTL time.Duration // 0 desliga o cache
}

// Dict é o gateway do DICT: cache curto de consultas bem-sucedidas, timeout
// por tentativa, retry com backoff só para erros transitórios e circuit
// breaker. Respostas 4xx (chave inexistente, rate limit) são respostas
// válidas do DICT: não repetem e não abrem o breaker.
type Dict struct {
	c     httpClient
	ispb  string
	cfg   DictConfig
	cb    *gobreaker.CircuitBreaker[dict.Entry]
	cache *entryCache
}

// NewDict cria o gateway; ispb identifica este PSP nas chamadas.
func NewDict(baseURL, ispb string, cfg DictConfig) *Dict {
	return &Dict{
		c:     newHTTPClient(baseURL, cfg.Timeout),
		ispb:  ispb,
		cfg:   cfg,
		cb:    resilience.NewBreaker[dict.Entry]("dict", func(err error) bool { return err == nil || !serverFailure(err) }),
		cache: newEntryCache(cfg.CacheTTL),
	}
}

// serverFailure diz se o erro é falha do serviço (rede, timeout, 5xx) e não
// uma resposta de negócio.
func serverFailure(err error) bool {
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr.Status >= 500
	}
	return !errors.Is(err, context.Canceled)
}

func (d *Dict) header(payerID string) http.Header {
	h := http.Header{pix.HeaderRequestingParticipant: {d.ispb}}
	if payerID != "" {
		h.Set(pix.HeaderPayerID, payerID)
	}
	return h
}

// Lookup resolve a chave em nome do usuário pagador (CPF/CNPJ). Erros do
// DICT voltam como *APIError (ex.: 404, 429); indisponibilidade volta como
// ErrDictUnavailable.
func (d *Dict) Lookup(ctx context.Context, key, payerID string) (dict.Entry, error) {
	if e, ok := d.cache.get(key); ok {
		return e, nil
	}
	e, err := d.cb.Execute(func() (dict.Entry, error) {
		return resilience.Retry(ctx, max(d.cfg.Attempts, 1), 50*time.Millisecond, 500*time.Millisecond,
			func(err error) bool { return serverFailure(err) && ctx.Err() == nil },
			func(ctx context.Context) (dict.Entry, error) {
				var e dict.Entry
				_, err := d.c.do(ctx, http.MethodGet, "/api/v1/entries/"+url.PathEscape(key), d.header(payerID), nil, &e)
				return e, err
			})
	})
	switch {
	case errors.Is(err, gobreaker.ErrOpenState), errors.Is(err, gobreaker.ErrTooManyRequests):
		return dict.Entry{}, ErrDictUnavailable
	case err != nil && serverFailure(err):
		return dict.Entry{}, errors.Join(ErrDictUnavailable, err)
	case err != nil:
		return dict.Entry{}, err
	}
	d.cache.put(key, e)
	return e, nil
}

// Register vincula uma chave a uma conta deste PSP.
func (d *Dict) Register(ctx context.Context, e dict.Entry) (dict.Entry, error) {
	var out dict.Entry
	_, err := d.c.do(ctx, http.MethodPost, "/api/v1/entries", d.header(""), e, &out)
	return out, err
}

// Remove desfaz o vínculo da chave.
func (d *Dict) Remove(ctx context.Context, key string) error {
	d.cache.drop(key)
	_, err := d.c.do(ctx, http.MethodDelete, "/api/v1/entries/"+url.PathEscape(key), d.header(""), nil, nil)
	return err
}

type cachedEntry struct {
	entry   dict.Entry
	expires time.Time
}

// entryCache guarda consultas bem-sucedidas por pouco tempo.
type entryCache struct {
	ttl time.Duration
	mu  sync.Mutex
	m   map[string]cachedEntry
}

func newEntryCache(ttl time.Duration) *entryCache {
	return &entryCache{ttl: ttl, m: map[string]cachedEntry{}}
}

func (c *entryCache) get(key string) (dict.Entry, bool) {
	if c.ttl <= 0 {
		return dict.Entry{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.m[key]
	if !ok || time.Now().After(e.expires) {
		delete(c.m, key)
		return dict.Entry{}, false
	}
	return e.entry, true
}

func (c *entryCache) put(key string, e dict.Entry) {
	if c.ttl <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.m) >= 10_000 { // teto simples: recomeça do zero
		clear(c.m)
	}
	c.m[key] = cachedEntry{entry: e, expires: time.Now().Add(c.ttl)}
}

func (c *entryCache) drop(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.m, key)
}

// --- MED: relatos de infração -------------------------------------------------

// CreateInfraction abre um relato de infração (PSP do pagador).
func (d *Dict) CreateInfraction(ctx context.Context, req dict.InfractionRequest) (dict.InfractionReport, error) {
	var out dict.InfractionReport
	_, err := d.c.do(ctx, http.MethodPost, "/api/v1/infraction-reports", d.header(""), req, &out)
	return out, err
}

// GetInfraction consulta o relato.
func (d *Dict) GetInfraction(ctx context.Context, id string) (dict.InfractionReport, error) {
	var out dict.InfractionReport
	_, err := d.c.do(ctx, http.MethodGet, "/api/v1/infraction-reports/"+url.PathEscape(id), d.header(""), nil, &out)
	return out, err
}

// ListInfractions lista os relatos do PSP: role "counterparty" (a analisar) ou
// "reporter" (abertos por ele), opcionalmente num estado.
func (d *Dict) ListInfractions(ctx context.Context, role, state string) ([]dict.InfractionReport, error) {
	q := url.Values{"role": {role}}
	if state != "" {
		q.Set("state", state)
	}
	var out []dict.InfractionReport
	_, err := d.c.do(ctx, http.MethodGet, "/api/v1/infraction-reports?"+q.Encode(), d.header(""), nil, &out)
	return out, err
}

// AnalyzeInfraction registra a decisão do recebedor.
func (d *Dict) AnalyzeInfraction(ctx context.Context, id string, a dict.AnalysisRequest) (dict.InfractionReport, error) {
	var out dict.InfractionReport
	_, err := d.c.do(ctx, http.MethodPost, "/api/v1/infraction-reports/"+url.PathEscape(id)+"/analysis", d.header(""), a, &out)
	return out, err
}

// CancelInfraction cancela o relato antes da análise (PSP do pagador).
func (d *Dict) CancelInfraction(ctx context.Context, id string) (dict.InfractionReport, error) {
	var out dict.InfractionReport
	_, err := d.c.do(ctx, http.MethodPost, "/api/v1/infraction-reports/"+url.PathEscape(id)+"/cancel", d.header(""), nil, &out)
	return out, err
}
