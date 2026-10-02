package clients_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/brenokcunha/light-pix/internal/clients"
	"github.com/brenokcunha/light-pix/internal/pix"
	"github.com/brenokcunha/light-pix/internal/sim/dict"
)

const payer = "12345678909"

// fakeDict responde com o status configurado e conta as chamadas.
type fakeDict struct {
	status atomic.Int32
	calls  atomic.Int32
	srv    *httptest.Server
}

func newFakeDict(t *testing.T, status int) *fakeDict {
	t.Helper()
	f := &fakeDict{}
	f.status.Store(int32(status))
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.calls.Add(1)
		if r.Header.Get(pix.HeaderRequestingParticipant) != "11111111" || r.Header.Get(pix.HeaderPayerID) != payer {
			http.Error(w, "headers", http.StatusBadRequest)
			return
		}
		st := int(f.status.Load())
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(st)
		if st == http.StatusOK {
			_ = json.NewEncoder(w).Encode(dict.Entry{Key: "k", KeyType: pix.KeyEmail})
		} else {
			_ = json.NewEncoder(w).Encode(map[string]string{"code": "X", "message": "falhou"})
		}
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func newGateway(f *fakeDict, cfg clients.DictConfig) *clients.Dict {
	if cfg.Timeout == 0 {
		cfg.Timeout = time.Second
	}
	return clients.NewDict(f.srv.URL, "11111111", cfg)
}

func TestDictLookupRetriesServerErrors(t *testing.T) {
	f := newFakeDict(t, http.StatusInternalServerError)
	d := newGateway(f, clients.DictConfig{Attempts: 3})

	_, err := d.Lookup(context.Background(), "k", payer)
	if !errors.Is(err, clients.ErrDictUnavailable) {
		t.Fatalf("err = %v, quer ErrDictUnavailable", err)
	}
	if n := f.calls.Load(); n != 3 {
		t.Fatalf("%d chamadas, quer 3 tentativas", n)
	}
}

// Respostas 404 e 429 são do DICT funcionando: não repetem e não abrem o breaker.
func TestDictClientErrorsAreNotRetriedNorTripTheBreaker(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusTooManyRequests} {
		f := newFakeDict(t, status)
		d := newGateway(f, clients.DictConfig{Attempts: 3})
		for range 12 {
			_, err := d.Lookup(context.Background(), "k", payer)
			var apiErr *clients.APIError
			if !errors.As(err, &apiErr) || apiErr.Status != status {
				t.Fatalf("status %d: err = %v", status, err)
			}
		}
		if n := f.calls.Load(); n != 12 {
			t.Fatalf("status %d: %d chamadas, quer 12 (sem retry)", status, n)
		}
		f.status.Store(http.StatusOK)
		if _, err := d.Lookup(context.Background(), "k", payer); err != nil {
			t.Fatalf("status %d: breaker abriu por respostas 4xx: %v", status, err)
		}
	}
}

func TestDictBreakerOpensAndFailsFast(t *testing.T) {
	f := newFakeDict(t, http.StatusInternalServerError)
	d := newGateway(f, clients.DictConfig{Attempts: 1})

	for range 5 {
		if _, err := d.Lookup(context.Background(), "k", payer); !errors.Is(err, clients.ErrDictUnavailable) {
			t.Fatal(err)
		}
	}
	before := f.calls.Load()
	start := time.Now()
	for range 10 {
		if _, err := d.Lookup(context.Background(), "k", payer); !errors.Is(err, clients.ErrDictUnavailable) {
			t.Fatal(err)
		}
	}
	if f.calls.Load() != before {
		t.Fatalf("com o breaker aberto o DICT recebeu %d chamadas", f.calls.Load()-before)
	}
	if time.Since(start) > 100*time.Millisecond {
		t.Fatalf("falha rápida demorou %v", time.Since(start))
	}
}

func TestDictTimeoutCountsAsUnavailable(t *testing.T) {
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(2 * time.Second):
		}
	}))
	t.Cleanup(slow.Close)
	d := clients.NewDict(slow.URL, "11111111", clients.DictConfig{Timeout: 50 * time.Millisecond, Attempts: 1})
	start := time.Now()
	if _, err := d.Lookup(context.Background(), "k", payer); !errors.Is(err, clients.ErrDictUnavailable) {
		t.Fatalf("err = %v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatalf("timeout por tentativa não respeitado: %v", time.Since(start))
	}
}

func TestDictCache(t *testing.T) {
	f := newFakeDict(t, http.StatusOK)
	d := newGateway(f, clients.DictConfig{Attempts: 1, CacheTTL: 100 * time.Millisecond})

	for range 3 {
		if _, err := d.Lookup(context.Background(), "k", payer); err != nil {
			t.Fatal(err)
		}
	}
	if n := f.calls.Load(); n != 1 {
		t.Fatalf("%d chamadas, quer 1 (cache)", n)
	}
	time.Sleep(150 * time.Millisecond)
	if _, err := d.Lookup(context.Background(), "k", payer); err != nil {
		t.Fatal(err)
	}
	if n := f.calls.Load(); n != 2 {
		t.Fatalf("%d chamadas depois do TTL, quer 2", n)
	}
}
