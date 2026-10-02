package dict_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/brenokcunha/light-pix/internal/pix"
	"github.com/brenokcunha/light-pix/internal/sim/dict"
	"github.com/brenokcunha/light-pix/internal/testutil"
)

const (
	ispbA    = "11111111"
	ispbB    = "22222222"
	cpfAlice = "12345678909"
	cpfBob   = "11144477735"
	cnpjLoja = "11222333000181"
)

type env struct {
	t   *testing.T
	url string
}

func setup(t *testing.T, cfg dict.Config, seed ...dict.Entry) *env {
	t.Helper()
	client, db := testutil.Mongo(t)
	s, err := dict.New(context.Background(), client, db, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Seed(context.Background(), seed); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	s.Routes(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return &env{t: t, url: srv.URL}
}

var defaultCfg = dict.Config{BucketCapacity: 100, RefillPerMinute: 60, NotFoundCost: 20}

func entry(keyType pix.KeyType, key, ispb, account, cpf string) dict.Entry {
	return dict.Entry{
		Key: key, KeyType: keyType,
		Account: dict.Account{Participant: ispb, Branch: "0001", AccountNumber: account, AccountType: "CACC",
			OpeningDate: time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)},
		Owner: dict.Owner{Type: dict.NaturalPerson, TaxIdNumber: cpf, Name: "Titular " + account},
	}
}

func (e *env) do(method, path, participant, payer string, body any) (*http.Response, []byte) {
	e.t.Helper()
	var r *bytes.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		r = bytes.NewReader(b)
	} else {
		r = bytes.NewReader(nil)
	}
	req, _ := http.NewRequest(method, e.url+path, r)
	req.Header.Set(pix.HeaderRequestingParticipant, participant)
	if payer != "" {
		req.Header.Set(pix.HeaderPayerID, payer)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer resp.Body.Close()
	var buf bytes.Buffer
	_, _ = buf.ReadFrom(resp.Body)
	return resp, buf.Bytes()
}

func (e *env) create(participant string, en dict.Entry) (int, dict.Entry, string) {
	e.t.Helper()
	resp, body := e.do(http.MethodPost, "/api/v1/entries", participant, "", en)
	var out dict.Entry
	var apiErr struct{ Code string }
	if resp.StatusCode == http.StatusCreated {
		_ = json.Unmarshal(body, &out)
	} else {
		_ = json.Unmarshal(body, &apiErr)
	}
	return resp.StatusCode, out, apiErr.Code
}

func (e *env) lookup(key, participant, payer string) (*http.Response, dict.Entry) {
	e.t.Helper()
	resp, body := e.do(http.MethodGet, "/api/v1/entries/"+url.PathEscape(key), participant, payer, nil)
	var out dict.Entry
	_ = json.Unmarshal(body, &out)
	return resp, out
}

func TestEntryLifecycle(t *testing.T) {
	e := setup(t, defaultCfg)

	code, created, _ := e.create(ispbA, entry(pix.KeyEmail, "Alice@Example.com", ispbA, "1000001", cpfAlice))
	if code != http.StatusCreated || created.Key != "alice@example.com" {
		t.Fatalf("criação: %d %+v", code, created)
	}

	resp, got := e.lookup("alice@example.com", ispbB, cpfBob)
	if resp.StatusCode != http.StatusOK || got.Account.AccountNumber != "1000001" || got.Owner.Name == "" {
		t.Fatalf("consulta: %d %+v", resp.StatusCode, got)
	}

	if code, _, errCode := e.create(ispbA, entry(pix.KeyEmail, "alice@example.com", ispbA, "1000002", cpfAlice)); code != http.StatusConflict || errCode != "ENTRY_ALREADY_EXISTS" {
		t.Fatalf("chave repetida: %d %s", code, errCode)
	}

	// Só o participante dono remove; para os outros a chave "não existe".
	if resp, _ := e.do(http.MethodDelete, "/api/v1/entries/alice@example.com", ispbB, "", nil); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("remoção por outro participante: %d", resp.StatusCode)
	}
	if resp, _ := e.do(http.MethodDelete, "/api/v1/entries/alice@example.com", ispbA, "", nil); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("remoção: %d", resp.StatusCode)
	}
	if resp, _ := e.lookup("alice@example.com", ispbB, cpfBob); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("consulta após remoção: %d", resp.StatusCode)
	}
}

func TestEVPIsGenerated(t *testing.T) {
	e := setup(t, defaultCfg)
	code, created, _ := e.create(ispbA, entry(pix.KeyEVP, "", ispbA, "1000001", cpfAlice))
	if code != http.StatusCreated || pix.ValidateKey(pix.KeyEVP, created.Key) != nil {
		t.Fatalf("EVP: %d %q", code, created.Key)
	}
}

func TestCreateValidation(t *testing.T) {
	e := setup(t, defaultCfg)
	pj := entry(pix.KeyCNPJ, cnpjLoja, ispbA, "3000001", cnpjLoja)
	pj.Owner.Type = dict.LegalPerson

	cases := []struct {
		name        string
		participant string
		entry       dict.Entry
		code        int
	}{
		{"conta de outro participante", ispbB, entry(pix.KeyCPF, cpfAlice, ispbA, "1000001", cpfAlice), http.StatusForbidden},
		{"CPF de outra pessoa", ispbA, entry(pix.KeyCPF, cpfBob, ispbA, "1000001", cpfAlice), http.StatusBadRequest},
		{"CPF inválido", ispbA, entry(pix.KeyCPF, "12345678900", ispbA, "1000001", "12345678900"), http.StatusBadRequest},
		{"telefone fora do E.164", ispbA, entry(pix.KeyPhone, "11999990000", ispbA, "1000001", cpfAlice), http.StatusBadRequest},
		{"tipo de conta inválido", ispbA, func() dict.Entry {
			en := entry(pix.KeyPhone, "+5511999990000", ispbA, "1000001", cpfAlice)
			en.Account.AccountType = "XPTO"
			return en
		}(), http.StatusBadRequest},
		{"pessoa jurídica com CNPJ", ispbA, pj, http.StatusCreated},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if code, _, errCode := e.create(c.participant, c.entry); code != c.code {
				t.Fatalf("status %d (%s), quer %d", code, errCode, c.code)
			}
		})
	}
}

func TestKeyLimitPerAccount(t *testing.T) {
	e := setup(t, defaultCfg)
	for i := range 5 {
		en := entry(pix.KeyPhone, fmt.Sprintf("+551199999000%d", i), ispbA, "1000001", cpfAlice)
		if code, _, errCode := e.create(ispbA, en); code != http.StatusCreated {
			t.Fatalf("chave %d: %d %s", i, code, errCode)
		}
	}
	code, _, errCode := e.create(ispbA, entry(pix.KeyEVP, "", ispbA, "1000001", cpfAlice))
	if code != http.StatusUnprocessableEntity || errCode != "ENTRY_LIMIT_EXCEEDED" {
		t.Fatalf("sexta chave de pessoa física: %d %s", code, errCode)
	}
	// O limite é por conta: outra conta do mesmo titular ainda pode.
	if code, _, _ := e.create(ispbA, entry(pix.KeyEVP, "", ispbA, "1000009", cpfAlice)); code != http.StatusCreated {
		t.Fatalf("chave em outra conta: %d", code)
	}
}

func TestLookupRequiresHeaders(t *testing.T) {
	e := setup(t, defaultCfg)
	if resp, _ := e.lookup("+5511999990000", ispbA, ""); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("sem PI-PayerId: %d", resp.StatusCode)
	}
	if resp, _ := e.lookup("+5511999990000", "", cpfAlice); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("sem participante: %d", resp.StatusCode)
	}
}

// Consulta a chave inexistente custa mais: varrer chaves esgota o balde
// rápido, e o balde é por usuário pagador.
func TestLookupRateLimit(t *testing.T) {
	seed := entry(pix.KeyCPF, cpfBob, ispbB, "2000001", cpfBob)
	e := setup(t, dict.Config{BucketCapacity: 10, RefillPerMinute: 60, NotFoundCost: 5}, seed)

	for i := range 2 {
		if resp, _ := e.lookup("+5511900000000", ispbA, cpfAlice); resp.StatusCode != http.StatusNotFound {
			t.Fatalf("consulta %d: %d", i, resp.StatusCode)
		}
	}
	resp, _ := e.lookup(cpfBob, ispbA, cpfAlice)
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("após varredura: %d, quer 429", resp.StatusCode)
	}
	if s, err := strconv.Atoi(resp.Header.Get("Retry-After")); err != nil || s < 1 {
		t.Fatalf("Retry-After = %q", resp.Header.Get("Retry-After"))
	}

	// Outro usuário pagador no mesmo participante não é afetado.
	if resp, got := e.lookup(cpfBob, ispbA, cpfBob); resp.StatusCode != http.StatusOK || got.Owner.TaxIdNumber != cpfBob {
		t.Fatalf("outro pagador: %d", resp.StatusCode)
	}

	// Reposição de 1 ficha por segundo libera o primeiro pagador.
	time.Sleep(1100 * time.Millisecond)
	if resp, _ := e.lookup(cpfBob, ispbA, cpfAlice); resp.StatusCode != http.StatusOK {
		t.Fatalf("após reposição: %d", resp.StatusCode)
	}
}
