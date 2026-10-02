package psp_test

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/brenokcunha/light-pix/internal/iso20022"
	"github.com/brenokcunha/light-pix/internal/pix"
	"github.com/brenokcunha/light-pix/internal/psp"
)

// O Pix de A chega a B, é aceito e, na liquidação, credita a conta de Bob.
func TestReceivedPixIsCreditedOnSettlement(t *testing.T) {
	w := newWorld(t, worldOpts{})
	a, b := w.nodes[ispbA], w.nodes[ispbB]

	e2e := a.pay("k-1", alice, "bob@example.com", "150.00").str("endToEndId")
	a.waitState(e2e, psp.StateConfirmed, 10*time.Second)
	got := b.waitState(e2e, psp.StateCredited, 10*time.Second)

	if got.str("direction") != psp.DirectionIn {
		t.Fatalf("direction = %q", got.str("direction"))
	}
	if h := history(got); !slices.Equal(h, []string{"RECEIVED", "ACCEPTED", "CREDITED"}) {
		t.Fatalf("histórico = %v", h)
	}
	debtor, _ := got.body["debtor"].(map[string]any)
	creditor, _ := got.body["creditor"].(map[string]any)
	if debtor["name"] != "Alice Andrade" || debtor["ispb"] != ispbA || creditor["name"] != "Bob Barros" || creditor["key"] != "bob@example.com" {
		t.Fatalf("partes: debtor=%v creditor=%v", debtor, creditor)
	}

	if av, held := b.balance(bob); av != 10150_00 || held != 0 {
		t.Fatalf("saldo de Bob: %s / %s", av, held)
	}
	if av, held := a.balance(alice); av != 9850_00 || held != 0 {
		t.Fatalf("saldo de Alice: %s / %s", av, held)
	}
	// O dinheiro que saiu das contas de A é o que entrou nas de B e na conta PI.
	if pa, pb := w.piBalance(ispbA), w.piBalance(ispbB); pa != 1_000_000_00-150_00 || pb != 1_000_000_00+150_00 {
		t.Fatalf("contas PI: %s / %s", pa, pb)
	}
	a.checkLedger()
	b.checkLedger()
}

// Dois sentidos ao mesmo tempo: nada se perde e o total do sistema se conserva.
func TestBidirectionalPaymentsConserveMoney(t *testing.T) {
	w := newWorld(t, worldOpts{})
	a, b := w.nodes[ispbA], w.nodes[ispbB]

	const n = 15
	type result struct {
		node *node
		id   string
	}
	results := make(chan result, 2*n)
	idem := func(prefix string, i int) string { return prefix + string(rune('a'+i)) }
	for i := range n {
		go func() {
			r := a.pay(idem("ab", i), alice, "bob@example.com", "1.00")
			if r.status != http.StatusCreated {
				t.Errorf("A→B %d: %d %v", i, r.status, r.body)
			}
			results <- result{a, r.str("endToEndId")}
		}()
		go func() {
			r := b.pay(idem("ba", i), bob, "alice@example.com", "2.00")
			if r.status != http.StatusCreated {
				t.Errorf("B→A %d: %d %v", i, r.status, r.body)
			}
			results <- result{b, r.str("endToEndId")}
		}()
	}
	for range 2 * n {
		r := <-results
		r.node.waitState(r.id, psp.StateConfirmed, 30*time.Second)
		// O lado recebedor também fecha.
		other := b
		if r.node == b {
			other = a
		}
		other.waitState(r.id, psp.StateCredited, 30*time.Second)
	}

	// A enviou 15 × 1,00 e recebeu 15 × 2,00; B o contrário.
	if av, held := a.balance(alice); av != 10000_00-n*1_00+n*2_00 || held != 0 {
		t.Fatalf("Alice: %s / %s", av, held)
	}
	if av, held := b.balance(bob); av != 10000_00+n*1_00-n*2_00 || held != 0 {
		t.Fatalf("Bob: %s / %s", av, held)
	}
	if pa, pb := w.piBalance(ispbA), w.piBalance(ispbB); pa != 1_000_000_00-n*1_00+n*2_00 || pb != 1_000_000_00+n*1_00-n*2_00 {
		t.Fatalf("contas PI: %s / %s", pa, pb)
	}
	a.checkLedger()
	b.checkLedger()
}

// O recebedor demora além do prazo do SPI. Quando o worker de B finalmente
// roda, a pacs.002 de rejeição pode ser lida antes da pacs.008: ela espera o
// recebimento ser registrado e então o cancela, sem creditar nada.
func TestLateReceiverEndsRejectedWithoutCredit(t *testing.T) {
	w := newWorld(t, worldOpts{
		creditorTimeout: time.Second,
		skip:            map[string]map[string]bool{ispbB: {"worker": true}},
	})
	a, b := w.nodes[ispbA], w.nodes[ispbB]

	e2e := a.pay("k-1", alice, "bob@example.com", "25.00").str("endToEndId")
	a.waitState(e2e, psp.StateRejected, 10*time.Second)

	b.startWorker()
	got := b.waitState(e2e, psp.StateRejected, 10*time.Second)
	if got.str("reason") != "AB03" {
		t.Fatalf("motivo = %q, quer AB03", got.str("reason"))
	}
	if h := history(got); !slices.Equal(h, []string{"RECEIVED", "ACCEPTED", "REJECTED"}) {
		t.Fatalf("histórico = %v", h)
	}
	if av, _ := b.balance(bob); av != 10000_00 {
		t.Fatalf("Bob foi creditado: %s", av)
	}
	b.checkLedger()
}

// Chave que aponta para conta que não existe no PSP do recebedor: o recebedor
// recusa (AC03) e deixa o registro da recusa.
func TestReceivedPixForUnknownAccountIsRejected(t *testing.T) {
	w := newWorld(t, worldOpts{})
	a, b := w.nodes[ispbA], w.nodes[ispbB]

	status := httpDo(t, http.MethodPost, w.dict+"/api/v1/entries", map[string]any{
		"Key": "sumiu@example.com", "KeyType": "EMAIL",
		"Account": map[string]any{"Participant": ispbB, "Branch": "0001", "AccountNumber": "9999999", "AccountType": "CACC"},
		"Owner":   map[string]any{"Type": "NATURAL_PERSON", "TaxIdNumber": "11144477735", "Name": "Conta Sumida"},
	}, ispbB)
	if status != http.StatusCreated {
		t.Fatalf("cadastro da chave: %d", status)
	}

	e2e := a.pay("k-1", alice, "sumiu@example.com", "10.00").str("endToEndId")
	done := a.waitState(e2e, psp.StateRejected, 10*time.Second)
	if done.str("reason") != "AC03" {
		t.Fatalf("motivo no pagador = %q", done.str("reason"))
	}
	got := b.waitState(e2e, psp.StateRejected, 10*time.Second)
	if got.str("reason") != "AC03" || !slices.Equal(history(got), []string{"RECEIVED", "REJECTED"}) {
		t.Fatalf("registro no recebedor: %v", got.body)
	}
}

// Reentrega da pacs.008 e da pacs.002 (at-least-once) não duplica resposta
// nem crédito.
func TestIncomingHandlersAreIdempotent(t *testing.T) {
	w := newWorld(t, worldOpts{skip: map[string]map[string]bool{
		ispbB: {"relay": true, "connector": true, "worker": true},
	}})
	b := w.nodes[ispbB]
	ctx := context.Background()

	now := time.Now()
	e2e := pix.NewEndToEndID(ispbA, now)
	pacs008, _ := json.Marshal(iso20022.NewPacs008Envelope(iso20022.CdtTrfTxInf{
		PmtId:          iso20022.PmtId{EndToEndId: e2e},
		IntrBkSttlmAmt: iso20022.ActiveCurrencyAndAmount{Ccy: "BRL", Value: 70_00},
		AccptncDtTm:    now,
		Dbtr:           iso20022.Party{Nm: "Alice Andrade", Id: "12345678909"},
		DbtrAcct:       iso20022.Account{Issr: "0001", Id: alice, Tp: "CACC"},
		DbtrAgt:        iso20022.Agent{MmbId: ispbA},
		Cdtr:           iso20022.Party{Nm: "Bob Barros", Id: "11144477735"},
		CdtrAcct:       iso20022.Account{Issr: "0001", Id: bob, Tp: "CACC", Prxy: "bob@example.com"},
		CdtrAgt:        iso20022.Agent{MmbId: ispbB},
	}, now))
	pacs002 := func(status string, reason string) amqp.Delivery {
		st := iso20022.TxInfAndSts{OrgnlEndToEndId: e2e, TxSts: status, FctvIntrBkSttlmDt: &now}
		if reason != "" {
			st.StsRsnInf = &iso20022.StsRsnInf{Rsn: reason}
		}
		body, _ := json.Marshal(iso20022.NewPacs002Envelope(pix.BCBISPB, ispbB, "M"+ispbA+"00000000000000000000000", st, now))
		return amqp.Delivery{Body: body}
	}

	for range 2 {
		if err := b.svc.HandleIncoming(ctx, amqp.Delivery{Body: pacs008}); err != nil {
			t.Fatal(err)
		}
	}
	if n := b.outboxCount(bson.D{}); n != 1 {
		t.Fatalf("%d respostas na outbox, quer 1", n)
	}
	if st := b.payment(e2e).str("state"); st != psp.StateAccepted {
		t.Fatalf("estado = %s", st)
	}
	if av, _ := b.balance(bob); av != 10000_00 {
		t.Fatalf("crédito antes da liquidação: %s", av)
	}

	for range 2 {
		if err := b.svc.HandleResult(ctx, pacs002(iso20022.StatusSettled, "")); err != nil {
			t.Fatal(err)
		}
	}
	// Uma rejeição tardia não desfaz o que foi liquidado.
	if err := b.svc.HandleResult(ctx, pacs002(iso20022.StatusRejected, iso20022.ReasonSettlementTimeout)); err != nil {
		t.Fatal(err)
	}
	if st := b.payment(e2e).str("state"); st != psp.StateCredited {
		t.Fatalf("estado = %s", st)
	}
	if av, held := b.balance(bob); av != 10070_00 || held != 0 {
		t.Fatalf("saldo de Bob: %s / %s, quer creditado uma vez", av, held)
	}
	b.checkLedger()

	// Resposta de um Pix recebido que ainda não foi registrado volta à fila...
	other := pix.NewEndToEndID(ispbA, now.Add(time.Minute))
	st := iso20022.TxInfAndSts{OrgnlEndToEndId: other, TxSts: iso20022.StatusSettled}
	body, _ := json.Marshal(iso20022.NewPacs002Envelope(pix.BCBISPB, ispbB, "M"+ispbA+"00000000000000000000001", st, now))
	if err := b.svc.HandleResult(ctx, amqp.Delivery{Body: body}); err == nil {
		t.Fatal("esperava erro para repetir: o recebimento ainda não existe")
	}
	// ...e a de um Pix enviado por este PSP que não existe é descartada.
	mine := pix.NewEndToEndID(ispbB, now)
	st.OrgnlEndToEndId = mine
	body, _ = json.Marshal(iso20022.NewPacs002Envelope(pix.BCBISPB, ispbB, "M"+ispbB+"00000000000000000000001", st, now))
	if err := b.svc.HandleResult(ctx, amqp.Delivery{Body: body}); err != nil {
		t.Fatalf("resultado de Pix próprio inexistente: %v", err)
	}
}
