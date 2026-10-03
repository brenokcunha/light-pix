package psp_test

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/brenokcunha/light-pix/internal/faults"
	"github.com/brenokcunha/light-pix/internal/psp"
)

// ret pede a devolução de um Pix recebido por este PSP.
func (n *node) ret(original, idemKey string, amount json.Number, reason string) response {
	n.w.t.Helper()
	body := map[string]any{"amount": amount}
	if reason != "" {
		body["reason"] = reason
	}
	return n.call(http.MethodPost, "/v1/pix/payments/"+original+"/returns", idemKey, body)
}

func (n *node) getReturn(id string) response {
	n.w.t.Helper()
	return n.call(http.MethodGet, "/v1/pix/returns/"+id, "", nil)
}

// waitReturn espera a devolução chegar ao estado neste PSP.
func (n *node) waitReturn(id, state string) response {
	n.w.t.Helper()
	var last response
	eventually(n.w.t, 15*time.Second, "devolução "+id+" em "+state, func() bool {
		last = n.getReturn(id)
		return last.str("state") == state
	})
	return last
}

// credited faz A pagar a B e espera os dois lados fecharem.
func creditedPix(t *testing.T, w *world, amount json.Number, idem string) string {
	t.Helper()
	a, b := w.nodes[ispbA], w.nodes[ispbB]
	r := a.pay(idem, alice, "bob@example.com", amount)
	if r.status != http.StatusCreated {
		t.Fatalf("pix: %d %v", r.status, r.body)
	}
	id := r.str("endToEndId")
	a.waitState(id, psp.StateConfirmed, 10*time.Second)
	b.waitState(id, psp.StateCredited, 10*time.Second)
	return id
}

// Devolução parcial de ponta a ponta: Bob devolve R$ 50 dos R$ 150 que recebeu.
func TestPartialReturnEndToEnd(t *testing.T) {
	w := newWorld(t, worldOpts{})
	a, b := w.nodes[ispbA], w.nodes[ispbB]
	orig := creditedPix(t, w, "150.00", "k-orig")

	created := b.ret(orig, "r-1", "50.00", "")
	if created.status != http.StatusCreated || created.str("state") != psp.StatePending || created.str("returnOf") != orig {
		t.Fatalf("criação: %d %v", created.status, created.body)
	}
	id := created.str("endToEndId")
	if id[0] != 'D' || len(id) != 32 {
		t.Fatalf("identificador %q não é de devolução", id)
	}
	if av, held := b.balance(bob); av != 10100_00 || held != 50_00 {
		t.Fatalf("reserva de Bob: %s / %s", av, held)
	}

	done := b.waitReturn(id, psp.StateConfirmed)
	if h := history(done); !slices.Equal(h, []string{"PENDING", "SENT", "CONFIRMED"}) {
		t.Fatalf("histórico = %v", h)
	}
	got := a.waitReturn(id, psp.StateCredited)
	if got.str("direction") != psp.DirectionIn || got.str("returnOf") != orig || got.str("returnReason") != "MD06" {
		t.Fatalf("lado de A: %v", got.body)
	}

	if av, held := a.balance(alice); av != 9900_00 || held != 0 { // 10000 − 150 + 50
		t.Fatalf("Alice: %s / %s", av, held)
	}
	if av, held := b.balance(bob); av != 10100_00 || held != 0 { // 10000 + 150 − 50
		t.Fatalf("Bob: %s / %s", av, held)
	}
	if pa, pb := w.piBalance(ispbA), w.piBalance(ispbB); pa != 1_000_000_00-100_00 || pb != 1_000_000_00+100_00 {
		t.Fatalf("contas PI: %s / %s", pa, pb)
	}
	for _, n := range []*node{a, b} {
		list := n.call(http.MethodGet, "/v1/pix/payments/"+orig+"/returns", "", nil)
		if list.status != http.StatusOK {
			t.Fatalf("lista de devoluções: %d", list.status)
		}
		n.checkLedger()
	}
	// O Pix e a devolução entram na conciliação da conta PI, nos dois PSPs.
	from, to := time.Now().Add(-time.Hour), time.Now().Add(time.Hour)
	for _, n := range []*node{a, b} {
		rep, err := n.resolver.Reconcile(context.Background(), from, to, "manual")
		must(t, err)
		if rep.Status != "OK" || rep.PIEntries != 2 {
			t.Fatalf("PSP %s: %s, %d liquidações, divergências %+v", n.ispb, rep.Status, rep.PIEntries, rep.Divergences)
		}
	}
}

func TestReturnCannotExceedThePix(t *testing.T) {
	w := newWorld(t, worldOpts{})
	b := w.nodes[ispbB]
	orig := creditedPix(t, w, "100.00", "k-orig")

	first := b.ret(orig, "r-1", "60.00", "")
	if first.status != http.StatusCreated {
		t.Fatalf("primeira: %d %v", first.status, first.body)
	}
	if r := b.ret(orig, "r-2", "40.01", ""); r.status != http.StatusUnprocessableEntity || r.str("code") != "RETURN_AMOUNT_EXCEEDED" {
		t.Fatalf("acima do resto: %d %v", r.status, r.body)
	}
	last := b.ret(orig, "r-3", "40.00", "")
	if last.status != http.StatusCreated {
		t.Fatalf("no limite: %d %v", last.status, last.body)
	}
	if r := b.ret(orig, "r-4", "0.01", ""); r.status != http.StatusUnprocessableEntity || r.str("code") != "RETURN_AMOUNT_EXCEEDED" {
		t.Fatalf("depois de devolver tudo: %d %v", r.status, r.body)
	}
	b.waitReturn(first.str("endToEndId"), psp.StateConfirmed)
	b.waitReturn(last.str("endToEndId"), psp.StateConfirmed)
	if av, held := b.balance(bob); av != 10000_00 || held != 0 {
		t.Fatalf("Bob devolveu tudo e deveria estar como antes: %s / %s", av, held)
	}
	b.checkLedger()
}

func TestReturnRequestValidation(t *testing.T) {
	w := newWorld(t, worldOpts{})
	a, b := w.nodes[ispbA], w.nodes[ispbB]
	orig := creditedPix(t, w, "100.00", "k-orig")

	cases := []struct {
		name   string
		resp   response
		status int
		code   string
	}{
		{"sem Idempotency-Key", b.ret(orig, "", "1.00", ""), 400, "INVALID_REQUEST"},
		{"valor zero", b.ret(orig, "v1", "0.00", ""), 400, "INVALID_REQUEST"},
		{"motivo não permitido", b.ret(orig, "v2", "1.00", "FR01"), 400, "INVALID_REQUEST"},
		{"Pix inexistente", b.ret("E22222222202601010000AAAAAAAAAAA", "v3", "1.00", ""), 404, "PAYMENT_NOT_FOUND"},
		{"só se devolve Pix recebido", a.ret(orig, "v4", "1.00", ""), 422, "NOT_RETURNABLE"},
	}
	for _, c := range cases {
		if c.resp.status != c.status || c.resp.str("code") != c.code {
			t.Errorf("%s: %d %v, quer %d %s", c.name, c.resp.status, c.resp.body, c.status, c.code)
		}
	}
	if r := b.getReturn("D22222222202601010000AAAAAAAAAAA"); r.status != http.StatusNotFound {
		t.Errorf("devolução inexistente: %d", r.status)
	}
}

func TestReturnIdempotency(t *testing.T) {
	w := newWorld(t, worldOpts{})
	b := w.nodes[ispbB]
	orig := creditedPix(t, w, "100.00", "k-orig")

	first := b.ret(orig, "r-1", "30.00", "")
	again := b.ret(orig, "r-1", "30.00", "")
	if first.status != http.StatusCreated || again.status != http.StatusOK ||
		first.str("endToEndId") != again.str("endToEndId") || again.header.Get("Idempotent-Replayed") != "true" {
		t.Fatalf("repetição: %d %v", again.status, again.body)
	}
	if r := b.ret(orig, "r-1", "31.00", ""); r.status != http.StatusUnprocessableEntity || r.str("code") != "IDEMPOTENCY_KEY_REUSED" {
		t.Fatalf("mesma chave, outro pedido: %d %v", r.status, r.body)
	}
	b.waitReturn(first.str("endToEndId"), psp.StateConfirmed)
	if av, _ := b.balance(bob); av != 10070_00 { // 10000 + 100 − 30, uma vez só
		t.Fatalf("Bob: %s", av)
	}
}

// Sem saldo, o cliente não devolve, e nada fica reservado no Pix original.
func TestReturnNeedsBalance(t *testing.T) {
	w := newWorld(t, worldOpts{})
	b := w.nodes[ispbB]
	orig := creditedPix(t, w, "9000.00", "k-orig")

	// Bob manda quase tudo de volta a Alice, por outro Pix.
	r := b.pay("k-spend", bob, "alice@example.com", "19000.00")
	if r.status != http.StatusCreated {
		t.Fatalf("Pix de Bob: %d %v", r.status, r.body)
	}
	b.waitState(r.str("endToEndId"), psp.StateConfirmed, 10*time.Second)

	if r := b.ret(orig, "r-1", "100.00", ""); r.status != http.StatusUnprocessableEntity || r.str("code") != "INSUFFICIENT_FUNDS" {
		t.Fatalf("sem saldo: %d %v", r.status, r.body)
	}
	if got := b.payment(orig); got.body["returnedAmount"] != nil {
		t.Fatalf("a recusa deixou valor reservado no Pix original: %v", got.body["returnedAmount"])
	}
}

// O pagador original não pode receber (conta bloqueada): a devolução é
// recusada, o valor volta ao saldo de quem devolveu e o Pix continua devolvível.
func TestRejectedReturnReleasesFundsAndAmount(t *testing.T) {
	w := newWorld(t, worldOpts{})
	a, b := w.nodes[ispbA], w.nodes[ispbB]
	orig := creditedPix(t, w, "100.00", "k-orig")
	a.setAccountStatus(alice, psp.AccountBlocked)

	id := b.ret(orig, "r-1", "100.00", "").str("endToEndId")
	done := b.waitReturn(id, psp.StateRejected)
	if done.str("reason") != "AC06" {
		t.Fatalf("motivo = %q, quer AC06", done.str("reason"))
	}
	if av, held := b.balance(bob); av != 10100_00 || held != 0 {
		t.Fatalf("a reserva não voltou: %s / %s", av, held)
	}
	if got := b.payment(orig); got.body["returnedAmount"] != nil && got.body["returnedAmount"] != float64(0) {
		t.Fatalf("o Pix ficou marcado como devolvido: %v", got.body["returnedAmount"])
	}
	rec := a.getReturn(id)
	if rec.str("state") != psp.StateRejected || rec.str("reason") != "AC06" {
		t.Fatalf("registro no pagador original: %v", rec.body)
	}
	b.checkLedger()
	a.checkLedger()

	// Desbloqueada a conta, a devolução do valor cheio passa.
	a.setAccountStatus(alice, psp.AccountActive)
	again := b.ret(orig, "r-2", "100.00", "")
	if again.status != http.StatusCreated {
		t.Fatalf("nova devolução: %d %v", again.status, again.body)
	}
	b.waitReturn(again.str("endToEndId"), psp.StateConfirmed)
}

func TestReturnWindowExpired(t *testing.T) {
	w := newWorld(t, worldOpts{})
	b := w.nodes[ispbB]
	orig := creditedPix(t, w, "10.00", "k-orig")
	b.svc.ReturnWindow = 50 * time.Millisecond
	time.Sleep(100 * time.Millisecond)
	if r := b.ret(orig, "r-1", "1.00", ""); r.status != http.StatusUnprocessableEntity || r.str("code") != "RETURN_WINDOW_EXPIRED" {
		t.Fatalf("%d %v", r.status, r.body)
	}
}

// Devoluções simultâneas nunca somam mais que o Pix, e o dinheiro fecha.
func TestConcurrentReturnsRespectTheCeiling(t *testing.T) {
	w := newWorld(t, worldOpts{})
	a, b := w.nodes[ispbA], w.nodes[ispbB]
	orig := creditedPix(t, w, "100.00", "k-orig")

	type result struct {
		status int
		id     string
		code   string
	}
	results := make(chan result, 8)
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			r := b.ret(orig, "c-"+string(rune('a'+i)), "25.00", "")
			results <- result{r.status, r.str("endToEndId"), r.str("code")}
		})
	}
	wg.Wait()
	close(results)
	var ids []string
	exceeded := 0
	for r := range results {
		switch {
		case r.status == http.StatusCreated:
			ids = append(ids, r.id)
		case r.code == "RETURN_AMOUNT_EXCEEDED":
			exceeded++
		default:
			t.Errorf("resposta inesperada: %d %s", r.status, r.code)
		}
	}
	if len(ids) != 4 || exceeded != 4 {
		t.Fatalf("criadas %d, recusadas %d; quer 4 e 4", len(ids), exceeded)
	}
	for _, id := range ids {
		b.waitReturn(id, psp.StateConfirmed)
		a.waitReturn(id, psp.StateCredited)
	}
	if av, held := a.balance(alice); av != 10000_00 || held != 0 {
		t.Fatalf("Alice recebeu tudo de volta: %s / %s", av, held)
	}
	a.checkLedger()
	b.checkLedger()
}

// A pacs.002 da devolução se perde: o resolver pergunta ao SPI e fecha os dois lados.
func TestLostReturnConfirmationIsResolved(t *testing.T) {
	w := newWorld(t, worldOpts{resolver: true})
	a, b := w.nodes[ispbA], w.nodes[ispbB]
	orig := creditedPix(t, w, "40.00", "k-orig")
	w.setSPIFaults(faults.Config{DropPacs002: 1})

	id := b.ret(orig, "r-1", "40.00", "").str("endToEndId")
	b.waitReturn(id, psp.StateConfirmed)
	a.waitReturn(id, psp.StateCredited)
	if av, held := a.balance(alice); av != 10000_00 || held != 0 {
		t.Fatalf("Alice: %s / %s", av, held)
	}
	a.checkLedger()
	b.checkLedger()
}
