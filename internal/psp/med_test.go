package psp_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/brenokcunha/light-pix/internal/pix"
	"github.com/brenokcunha/light-pix/internal/psp"
)

func (n *node) report(e2e, reason, details string) response {
	n.w.t.Helper()
	return n.call(http.MethodPost, "/v1/pix/payments/"+e2e+"/infraction-reports", "", map[string]any{"reason": reason, "details": details})
}

func (n *node) infraction(id string) response {
	n.w.t.Helper()
	return n.call(http.MethodGet, "/v1/infraction-reports/"+id, "", nil)
}

func (n *node) decide(id, result string) response {
	n.w.t.Helper()
	return n.call(http.MethodPost, "/v1/infraction-reports/"+id+"/decision", "", map[string]any{"result": result, "note": "análise do lab"})
}

func (n *node) waitInfraction(id, state string) response {
	n.w.t.Helper()
	var last response
	eventually(n.w.t, 20*time.Second, "relato "+id+" em "+state+" no PSP "+n.ispb, func() bool {
		last = n.infraction(id)
		return last.str("state") == state
	})
	return last
}

// blockedReport faz A pagar B, Alice relatar a fraude e o PSP B bloquear.
func blockedReport(t *testing.T, w *world, amount json.Number) (e2e, infractionID string) {
	t.Helper()
	a, b := w.nodes[ispbA], w.nodes[ispbB]
	e2e = creditedPix(t, w, amount, "k-orig")
	r := a.report(e2e, "FRAUD", "caí num golpe")
	if r.status != http.StatusCreated || r.str("state") != psp.InfractionOpen || r.str("role") != psp.RoleReporter {
		t.Fatalf("relato: %d %v", r.status, r.body)
	}
	infractionID = r.str("id")
	b.waitInfraction(infractionID, psp.InfractionBlocked)
	return e2e, infractionID
}

// MED completo: Alice relata, B bloqueia o valor, o analista concorda e a
// devolução especial leva o dinheiro de volta.
func TestMEDAgreedReturnsTheMoney(t *testing.T) {
	w := newWorld(t, worldOpts{resolver: true})
	a, b := w.nodes[ispbA], w.nodes[ispbB]
	_, id := blockedReport(t, w, "200.00")

	if bal := b.account(bob); bal.Available != 10000_00 || bal.Blocked != 200_00 || bal.Held != 0 {
		t.Fatalf("bloqueio cautelar: %+v", bal)
	}
	blocked := b.infraction(id)
	if blocked.body["blockedAmount"] != float64(200) || blocked.str("role") != psp.RoleReceiver {
		t.Fatalf("relato no recebedor: %v", blocked.body)
	}

	// Só o PSP recebedor decide.
	if r := a.decide(id, "AGREE"); r.status != http.StatusForbidden {
		t.Fatalf("pagador decidindo: %d %v", r.status, r.body)
	}
	agreed := b.decide(id, "AGREE")
	if agreed.status != http.StatusOK || agreed.str("state") != psp.InfractionAgreed || agreed.str("returnId") == "" {
		t.Fatalf("decisão: %d %v", agreed.status, agreed.body)
	}
	retID := agreed.str("returnId")
	if bal := b.account(bob); bal.Blocked != 0 || bal.Held != 200_00 {
		t.Fatalf("o bloqueio vira reserva da devolução: %+v", bal)
	}
	if r := b.decide(id, "AGREE"); r.status != http.StatusConflict || r.str("code") != "INFRACTION_NOT_BLOCKED" {
		t.Fatalf("segunda decisão: %d %v", r.status, r.body)
	}

	b.waitInfraction(id, psp.InfractionReturned)
	a.waitInfraction(id, psp.InfractionReturned)
	if got := a.getReturn(retID); got.str("returnReason") != "FR01" || got.str("state") != psp.StateCredited {
		t.Fatalf("devolução especial em A: %v", got.body)
	}
	if bal := a.account(alice); bal.Available != 10000_00 || bal.Held != 0 { // pagou 200 e recebeu 200
		t.Fatalf("Alice: %+v", bal)
	}
	if bal := b.account(bob); bal.Available != 10000_00 || bal.Held != 0 || bal.Blocked != 0 {
		t.Fatalf("Bob: %+v", bal)
	}
	if pa, pb := w.piBalance(ispbA), w.piBalance(ispbB); pa != 1_000_000_00 || pb != 1_000_000_00 {
		t.Fatalf("contas PI: %s / %s", pa, pb)
	}
	// O DICT ficou com a decisão e a devolução.
	eventually(t, 10*time.Second, "decisão no DICT", func() bool {
		rep := httpGetJSON(t, w.dict+"/api/v1/infraction-reports/"+id, ispbA)
		return rep["state"] == "AGREED"
	})
	a.checkLedger()
	b.checkLedger()
	from, to := time.Now().Add(-time.Hour), time.Now().Add(time.Hour)
	for _, n := range []*node{a, b} {
		rep, err := n.resolver.Reconcile(context.Background(), from, to, "manual")
		must(t, err)
		if rep.Status != "OK" {
			t.Fatalf("PSP %s: %+v", n.ispb, rep.Divergences)
		}
	}
}

func TestMEDDisagreedUnblocks(t *testing.T) {
	w := newWorld(t, worldOpts{resolver: true})
	a, b := w.nodes[ispbA], w.nodes[ispbB]
	_, id := blockedReport(t, w, "200.00")

	if r := b.decide(id, "DISAGREE"); r.status != http.StatusOK || r.str("state") != psp.InfractionDisapproved {
		t.Fatalf("decisão: %d %v", r.status, r.body)
	}
	if bal := b.account(bob); bal.Available != 10200_00 || bal.Blocked != 0 {
		t.Fatalf("o valor deveria voltar ao saldo de Bob: %+v", bal)
	}
	a.waitInfraction(id, psp.InfractionDisapproved)
	if bal := a.account(alice); bal.Available != 9800_00 {
		t.Fatalf("Alice não recebe nada: %+v", bal)
	}
	a.checkLedger()
	b.checkLedger()
}

// O valor já saiu em parte da conta: o MED bloqueia e devolve só o que sobrou.
func TestMEDBlocksOnlyWhatIsLeft(t *testing.T) {
	w := newWorld(t, worldOpts{resolver: true})
	a, b := w.nodes[ispbA], w.nodes[ispbB]
	e2e := creditedPix(t, w, "300.00", "k-orig")
	// Bob manda quase tudo para Alice, por outro Pix: sobram R$ 50.
	spend := b.pay("k-spend", bob, "alice@example.com", "10250.00")
	if spend.status != http.StatusCreated {
		t.Fatalf("Pix de Bob: %d %v", spend.status, spend.body)
	}
	b.waitState(spend.str("endToEndId"), psp.StateConfirmed, 10*time.Second)

	id := a.report(e2e, "SCAM", "").str("id")
	blocked := b.waitInfraction(id, psp.InfractionBlocked)
	if blocked.body["blockedAmount"] != float64(50) {
		t.Fatalf("bloqueado = %v, quer 50", blocked.body["blockedAmount"])
	}
	if r := b.decide(id, "AGREE"); r.status != http.StatusOK {
		t.Fatalf("decisão: %d %v", r.status, r.body)
	}
	b.waitInfraction(id, psp.InfractionReturned)
	a.waitInfraction(id, psp.InfractionReturned)
	if bal := b.account(bob); bal.Available != 0 || bal.Blocked != 0 || bal.Held != 0 {
		t.Fatalf("Bob: %+v", bal)
	}
	if bal := a.account(alice); bal.Available != 10000_00-300_00+10250_00+50_00 {
		t.Fatalf("Alice: %+v", bal)
	}
	a.checkLedger()
	b.checkLedger()
}

// Sem nada a bloquear, o recebedor ainda pode concordar; o DICT registra e ninguém recebe devolução.
func TestMEDAgreedWithNoFunds(t *testing.T) {
	w := newWorld(t, worldOpts{resolver: true})
	a, b := w.nodes[ispbA], w.nodes[ispbB]
	e2e := creditedPix(t, w, "300.00", "k-orig")
	spend := b.pay("k-spend", bob, "alice@example.com", "10300.00")
	b.waitState(spend.str("endToEndId"), psp.StateConfirmed, 10*time.Second)

	id := a.report(e2e, "FRAUD", "").str("id")
	if got := b.waitInfraction(id, psp.InfractionBlocked); got.body["blockedAmount"] != nil {
		t.Fatalf("nada para bloquear, mas veio %v", got.body["blockedAmount"])
	}
	if r := b.decide(id, "AGREE"); r.status != http.StatusOK || r.str("state") != psp.InfractionNoFunds {
		t.Fatalf("decisão: %d %v", r.status, r.body)
	}
	a.waitInfraction(id, psp.InfractionNoFunds)
	if got := a.call(http.MethodGet, "/v1/pix/payments/"+e2e+"/returns", "", nil); got.status != http.StatusOK {
		t.Fatalf("lista: %d", got.status)
	}
	a.checkLedger()
	b.checkLedger()
}

// O PSP do pagador cancela depois do bloqueio: o valor volta ao recebedor.
func TestMEDCancelUnblocks(t *testing.T) {
	w := newWorld(t, worldOpts{resolver: true})
	a, b := w.nodes[ispbA], w.nodes[ispbB]
	_, id := blockedReport(t, w, "200.00")

	if r := b.call(http.MethodPost, "/v1/infraction-reports/"+id+"/cancel", "", nil); r.status != http.StatusForbidden {
		t.Fatalf("recebedor cancelando: %d", r.status)
	}
	if r := a.call(http.MethodPost, "/v1/infraction-reports/"+id+"/cancel", "", nil); r.status != http.StatusOK || r.str("state") != psp.InfractionCancelled {
		t.Fatalf("cancelamento: %d %v", r.status, r.body)
	}
	b.waitInfraction(id, psp.InfractionCancelled)
	if bal := b.account(bob); bal.Available != 10200_00 || bal.Blocked != 0 {
		t.Fatalf("Bob: %+v", bal)
	}
	a.checkLedger()
	b.checkLedger()
}

func TestMEDReportValidation(t *testing.T) {
	w := newWorld(t, worldOpts{resolver: true})
	a, b := w.nodes[ispbA], w.nodes[ispbB]
	e2e := creditedPix(t, w, "10.00", "k-orig")

	cases := []struct {
		name   string
		resp   response
		status int
		code   string
	}{
		{"Pix inexistente", a.report("E11111111202601010000AAAAAAAAAAA", "FRAUD", ""), 404, "PAYMENT_NOT_FOUND"},
		{"só se relata Pix enviado", b.report(e2e, "FRAUD", ""), 422, "NOT_REPORTABLE"},
		{"motivo inválido", a.report(e2e, "OUTRO", ""), 400, "INVALID_REQUEST"},
	}
	for _, c := range cases {
		if c.resp.status != c.status || c.resp.str("code") != c.code {
			t.Errorf("%s: %d %v, quer %d %s", c.name, c.resp.status, c.resp.body, c.status, c.code)
		}
	}
	first := a.report(e2e, "FRAUD", "")
	if first.status != http.StatusCreated {
		t.Fatalf("relato válido: %d %v", first.status, first.body)
	}
	if r := a.report(e2e, "FRAUD", ""); r.status != http.StatusConflict || r.str("code") != "INFRACTION_ALREADY_REPORTED" {
		t.Errorf("segundo relato: %d %v", r.status, r.body)
	}
	if r := a.infraction("00000000-0000-0000-0000-000000000000"); r.status != http.StatusNotFound {
		t.Errorf("relato inexistente: %d", r.status)
	}
}

// Relato sobre um Pix que o recebedor não conhece é recusado sozinho, sem bloquear nada.
func TestMEDUnknownPixIsDisapproved(t *testing.T) {
	w := newWorld(t, worldOpts{resolver: true})
	b := w.nodes[ispbB]
	e2e := pix.NewEndToEndID(ispbA, time.Now())
	body, _ := json.Marshal(map[string]any{"endToEndId": e2e, "reason": "FRAUD", "counterpartyIspb": ispbB, "amount": json.Number("10.00")})
	req, _ := http.NewRequest(http.MethodPost, w.dict+"/api/v1/infraction-reports", bytesReader(body))
	req.Header.Set(pix.HeaderRequestingParticipant, ispbA)
	resp, err := http.DefaultClient.Do(req)
	must(t, err)
	var rep map[string]any
	must(t, json.NewDecoder(resp.Body).Decode(&rep))
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("relato direto no DICT: %d %v", resp.StatusCode, rep)
	}
	id := rep["id"].(string)
	b.waitInfraction(id, psp.InfractionDisapproved)
	eventually(t, 10*time.Second, "recusa no DICT", func() bool {
		return httpGetJSON(t, w.dict+"/api/v1/infraction-reports/"+id, ispbA)["state"] == "DISAPPROVED"
	})
	if bal := b.account(bob); bal.Blocked != 0 || bal.Available != 10000_00 {
		t.Fatalf("Bob não deveria ser tocado: %+v", bal)
	}
}
