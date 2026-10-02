package bacen_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/brenokcunha/light-pix/internal/clients"
	"github.com/brenokcunha/light-pix/internal/pix"
	"github.com/brenokcunha/light-pix/internal/sim/bacen"
	"github.com/brenokcunha/light-pix/internal/testutil"
)

const (
	ispbA = "11111111"
	ispbB = "22222222"
)

func setup(t *testing.T, initial map[string]pix.Amount) (*clients.Bacen, string) {
	t.Helper()
	client, db := testutil.Mongo(t)
	ctx := context.Background()
	s, err := bacen.New(ctx, client, db)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Seed(ctx, initial); err != nil {
		t.Fatal(err)
	}
	// Seed é idempotente: repetir não muda o saldo.
	if err := s.Seed(ctx, initial); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	s.Routes(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return clients.NewBacen(srv.URL, 5*time.Second), srv.URL
}

func balance(t *testing.T, c *clients.Bacen, ispb string) pix.Amount {
	t.Helper()
	acc, err := c.Account(context.Background(), ispb)
	if err != nil {
		t.Fatalf("conta %s: %v", ispb, err)
	}
	return acc.Balance
}

func wantCode(t *testing.T, err error, status int, code string) {
	t.Helper()
	var apiErr *clients.APIError
	if !errors.As(err, &apiErr) || apiErr.Status != status || apiErr.Code != code {
		t.Fatalf("erro = %v, quer %d %s", err, status, code)
	}
}

func TestSettlement(t *testing.T) {
	c, _ := setup(t, map[string]pix.Amount{ispbA: 1000_00, ispbB: 0})
	ctx := context.Background()
	e2e := pix.NewEndToEndID(ispbA, time.Now())
	req := bacen.SettlementRequest{EndToEndId: e2e, DebtorISPB: ispbA, CreditorISPB: ispbB, Amount: 100_00}

	first, err := c.Settle(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if a, b := balance(t, c, ispbA), balance(t, c, ispbB); a != 900_00 || b != 100_00 {
		t.Fatalf("saldos = %s / %s, quer 900.00 / 100.00", a, b)
	}

	t.Run("replay devolve a liquidação original", func(t *testing.T) {
		again, err := c.Settle(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		if !again.SettledAt.Equal(first.SettledAt) {
			t.Errorf("settledAt = %v, quer %v", again.SettledAt, first.SettledAt)
		}
		if a := balance(t, c, ispbA); a != 900_00 {
			t.Errorf("replay debitou de novo: saldo %s", a)
		}
	})

	t.Run("mesmo EndToEndId com outro valor é conflito", func(t *testing.T) {
		other := req
		other.Amount = 1_00
		_, err := c.Settle(ctx, other)
		wantCode(t, err, http.StatusConflict, bacen.CodeConflict)
	})

	t.Run("saldo insuficiente é AM04 e não move nada", func(t *testing.T) {
		_, err := c.Settle(ctx, bacen.SettlementRequest{
			EndToEndId: pix.NewEndToEndID(ispbA, time.Now()), DebtorISPB: ispbA, CreditorISPB: ispbB, Amount: 900_01,
		})
		wantCode(t, err, http.StatusUnprocessableEntity, bacen.CodeInsufficientFunds)
		if a, b := balance(t, c, ispbA), balance(t, c, ispbB); a != 900_00 || b != 100_00 {
			t.Fatalf("saldos = %s / %s após AM04", a, b)
		}
	})

	t.Run("conta inexistente aborta a transação inteira", func(t *testing.T) {
		_, err := c.Settle(ctx, bacen.SettlementRequest{
			EndToEndId: pix.NewEndToEndID(ispbA, time.Now()), DebtorISPB: ispbA, CreditorISPB: "33333333", Amount: 1_00,
		})
		wantCode(t, err, http.StatusNotFound, bacen.CodeUnknownAccount)
		if a := balance(t, c, ispbA); a != 900_00 {
			t.Fatalf("débito ficou sem crédito: saldo %s", a)
		}
	})
}

// Liquidações concorrentes não podem deixar a conta PI negativa.
func TestConcurrentSettlementsNeverOverdraw(t *testing.T) {
	c, _ := setup(t, map[string]pix.Amount{ispbA: 1000_00, ispbB: 0})
	var ok, am04 atomic.Int32
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			_, err := c.Settle(context.Background(), bacen.SettlementRequest{
				EndToEndId: pix.NewEndToEndID(ispbA, time.Now()), DebtorISPB: ispbA, CreditorISPB: ispbB, Amount: 100_00,
			})
			var apiErr *clients.APIError
			switch {
			case err == nil:
				ok.Add(1)
			case errors.As(err, &apiErr) && apiErr.Code == bacen.CodeInsufficientFunds:
				am04.Add(1)
			default:
				t.Errorf("erro inesperado: %v", err)
			}
		})
	}
	wg.Wait()
	if ok.Load() != 10 || am04.Load() != 10 {
		t.Fatalf("liquidadas %d, AM04 %d; quer 10 e 10", ok.Load(), am04.Load())
	}
	if a, b := balance(t, c, ispbA), balance(t, c, ispbB); a != 0 || b != 1000_00 {
		t.Fatalf("saldos = %s / %s", a, b)
	}
}

func TestSTRTransfersAndStatement(t *testing.T) {
	c, baseURL := setup(t, map[string]pix.Amount{ispbA: 100_00})
	ctx := context.Background()
	from := time.Now().Add(-time.Minute)

	in := bacen.STRTransfer{TransferId: "str-1", Direction: "IN", Amount: 50_00}
	e, err := c.STRTransfer(ctx, ispbA, in)
	if err != nil {
		t.Fatal(err)
	}
	if e.Kind != bacen.KindSTRIn || e.BalanceAfter != 150_00 {
		t.Fatalf("lançamento = %+v", e)
	}
	if again, err := c.STRTransfer(ctx, ispbA, in); err != nil || again.ID != e.ID {
		t.Fatalf("replay do aporte: %+v, %v", again, err)
	}
	_, err = c.STRTransfer(ctx, ispbA, bacen.STRTransfer{TransferId: "str-1", Direction: "OUT", Amount: 50_00})
	wantCode(t, err, http.StatusConflict, bacen.CodeConflict)
	_, err = c.STRTransfer(ctx, ispbA, bacen.STRTransfer{TransferId: "str-2", Direction: "OUT", Amount: 150_01})
	wantCode(t, err, http.StatusUnprocessableEntity, bacen.CodeInsufficientFunds)
	if _, err := c.STRTransfer(ctx, ispbA, bacen.STRTransfer{TransferId: "str-3", Direction: "OUT", Amount: 30_00}); err != nil {
		t.Fatal(err)
	}

	entries, err := c.Statement(ctx, ispbA, from, time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	// Saldo é a soma do extrato: seed + aporte - retirada.
	var sum pix.Amount
	for _, e := range entries {
		sum += e.Amount
	}
	if len(entries) != 3 || sum != 120_00 || sum != balance(t, c, ispbA) {
		t.Fatalf("extrato com %d lançamentos e soma %s, saldo %s", len(entries), sum, balance(t, c, ispbA))
	}

	t.Run("paginação por cursor", func(t *testing.T) {
		var ids []string
		next := ""
		for range 5 {
			url := baseURL + "/api/v1/pi-accounts/" + ispbA + "/statement?limit=1"
			if next != "" {
				url += "&after=" + next
			}
			resp, err := http.Get(url)
			if err != nil {
				t.Fatal(err)
			}
			var page bacen.Statement
			err = json.NewDecoder(resp.Body).Decode(&page)
			resp.Body.Close()
			if err != nil {
				t.Fatal(err)
			}
			for _, e := range page.Entries {
				ids = append(ids, e.ID.Hex())
			}
			if next = page.Next; next == "" {
				break
			}
		}
		if len(ids) != 3 || ids[0] != entries[0].ID.Hex() || ids[2] != entries[2].ID.Hex() {
			t.Fatalf("páginas = %v", ids)
		}
	})
}
