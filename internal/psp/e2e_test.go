package psp_test

import (
	"context"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/brenokcunha/light-pix/internal/pix"
	"github.com/brenokcunha/light-pix/internal/psp"
)

const (
	alice = "1000001" // PSP A, chave alice@example.com
	bob   = "2000001" // PSP B, chave bob@example.com
)

func history(r response) []string {
	var states []string
	items, _ := r.body["history"].([]any)
	for _, it := range items {
		m, _ := it.(map[string]any)
		states = append(states, m["state"].(string))
	}
	return states
}

// Caminho feliz: o Pix de A para B é confirmado, os saldos e as contas PI
// conferem e todo o fluxo cai num único trace.
func TestPixSentIsConfirmedEndToEnd(t *testing.T) {
	w := newWorld(t, worldOpts{})
	a, b := w.nodes[ispbA], w.nodes[ispbB]

	created := a.pay("k-1", alice, "bob@example.com", "150.00")
	if created.status != http.StatusCreated || created.str("state") != psp.StatePending {
		t.Fatalf("criação: %d %v", created.status, created.body)
	}
	e2e := created.str("endToEndId")
	if owner, err := pix.ParseEndToEndID(e2e); err != nil || owner != ispbA {
		t.Fatalf("EndToEndId %q inválido: %v", e2e, err)
	}
	if av, held := a.balance(alice); av != 9850_00 || held != 150_00 {
		t.Fatalf("saldo logo após a reserva: %s / %s", av, held)
	}

	done := a.waitState(e2e, psp.StateConfirmed, 10*time.Second)
	if got := history(done); !slices.Equal(got, []string{"PENDING", "SENT", "CONFIRMED"}) {
		t.Fatalf("histórico = %v", got)
	}
	if av, held := a.balance(alice); av != 9850_00 || held != 0 {
		t.Fatalf("saldo final do pagador: %s / %s", av, held)
	}
	if pa, pb := w.piBalance(ispbA), w.piBalance(ispbB); pa != 1_000_000_00-150_00 || pb != 1_000_000_00+150_00 {
		t.Fatalf("contas PI: %s / %s", pa, pb)
	}
	a.checkLedger()
	b.checkLedger()
	if n := a.outboxCount(bson.D{{Key: "publishedAt", Value: nil}}); n != 0 {
		t.Fatalf("%d eventos da outbox sem publicar", n)
	}

	assertSingleTrace(t, e2e)
}

// assertSingleTrace confere que todos os spans do Pix, de A e de B, estão no
// mesmo trace do pedido HTTP.
func assertSingleTrace(t *testing.T, e2e string) {
	t.Helper()
	attrOf := func(s sdktrace.ReadOnlySpan, key string) string {
		for _, kv := range s.Attributes() {
			if kv.Key == attribute.Key(key) {
				return kv.Value.AsString()
			}
		}
		return ""
	}
	want := []string{
		"POST /v1/pix/payments", "outbox.publish spi.out", "process spi.out",
		"process pix.incoming", "process pix.results", "spi.deliver pacs.002", "spi.deliver pacs.008",
	}
	var traceID string
	var names map[string]bool
	eventually(t, 5*time.Second, "spans do Pix no mesmo trace", func() bool {
		traceID, names = "", map[string]bool{}
		for _, s := range spans.Ended() {
			if s.Name() == "POST /v1/pix/payments" && attrOf(s, "pix.end_to_end_id") == e2e {
				traceID = s.SpanContext().TraceID().String()
			}
		}
		if traceID == "" {
			return false
		}
		for _, s := range spans.Ended() {
			if s.SpanContext().TraceID().String() == traceID {
				names[s.Name()] = true
			}
		}
		for _, n := range want {
			if !names[n] {
				return false
			}
		}
		return true
	})
	// Nenhum span deste Pix fica fora do trace do pedido.
	for _, s := range spans.Ended() {
		if attrOf(s, "pix.end_to_end_id") == e2e && s.SpanContext().TraceID().String() != traceID {
			t.Errorf("span %q do Pix está em outro trace", s.Name())
		}
	}
	// Spans do Mongo aparecem dentro do trace.
	var db bool
	for n := range names {
		db = db || strings.HasPrefix(n, "insert ") || strings.HasPrefix(n, "update ")
	}
	if !db {
		t.Errorf("sem spans do MongoDB no trace; spans: %v", names)
	}
}

// O recebedor recusa (conta bloqueada): o pagador recebe RJCT AC06 e a
// reserva volta ao saldo.
func TestPixRejectedByCreditorReleasesHold(t *testing.T) {
	w := newWorld(t, worldOpts{})
	a, b := w.nodes[ispbA], w.nodes[ispbB]
	b.setAccountStatus(bob, psp.AccountBlocked)

	e2e := a.pay("k-1", alice, "bob@example.com", "40.00").str("endToEndId")
	done := a.waitState(e2e, psp.StateRejected, 10*time.Second)
	if done.str("reason") != "AC06" {
		t.Fatalf("motivo = %q, quer AC06", done.str("reason"))
	}
	if av, held := a.balance(alice); av != 10000_00 || held != 0 {
		t.Fatalf("saldo após rejeição: %s / %s", av, held)
	}
	if pa := w.piBalance(ispbA); pa != 1_000_000_00 {
		t.Fatalf("conta PI de A mudou: %s", pa)
	}
	a.checkLedger()
}

// Recebedor não responde no prazo: o SPI rejeita com AB03.
func TestPixCreditorTimeout(t *testing.T) {
	w := newWorld(t, worldOpts{
		creditorTimeout: time.Second,
		skip:            map[string]map[string]bool{ispbB: {"worker": true}}, // B nunca responde
	})
	a := w.nodes[ispbA]

	e2e := a.pay("k-1", alice, "bob@example.com", "25.00").str("endToEndId")
	done := a.waitState(e2e, psp.StateRejected, 10*time.Second)
	if done.str("reason") != "AB03" {
		t.Fatalf("motivo = %q, quer AB03", done.str("reason"))
	}
	if av, held := a.balance(alice); av != 10000_00 || held != 0 {
		t.Fatalf("saldo após timeout: %s / %s", av, held)
	}
	a.checkLedger()
}

// O SPI recusa de forma definitiva (participante desconhecido): o conector
// rejeita o pagamento em vez de deixar a reserva presa.
func TestPixRejectedWhenSPIRefuses(t *testing.T) {
	w := newWorld(t, worldOpts{})
	a := w.nodes[ispbA]

	// Chave de um participante que o SPI não conhece.
	resp := httpDo(t, http.MethodPost, w.dict+"/api/v1/entries", map[string]any{
		"Key": "fantasma@example.com", "KeyType": "EMAIL",
		"Account": map[string]any{"Participant": "33333333", "Branch": "0001", "AccountNumber": "9", "AccountType": "CACC"},
		"Owner":   map[string]any{"Type": "NATURAL_PERSON", "TaxIdNumber": "11144477735", "Name": "Fantasma"},
	}, "33333333")
	if resp != http.StatusCreated {
		t.Fatalf("cadastro da chave: %d", resp)
	}

	e2e := a.pay("k-1", alice, "fantasma@example.com", "10.00").str("endToEndId")
	done := a.waitState(e2e, psp.StateRejected, 10*time.Second)
	if done.str("reason") != "AC03" {
		t.Fatalf("motivo = %q, quer AC03", done.str("reason"))
	}
	if av, held := a.balance(alice); av != 10000_00 || held != 0 {
		t.Fatalf("saldo após recusa do SPI: %s / %s", av, held)
	}
}

// Sem saldo, o pedido é recusado na hora e nada é gravado.
func TestPixInsufficientFunds(t *testing.T) {
	w := newWorld(t, worldOpts{})
	a := w.nodes[ispbA]

	r := a.pay("k-1", alice, "bob@example.com", "10000.01")
	if r.status != http.StatusUnprocessableEntity || r.str("code") != "INSUFFICIENT_FUNDS" {
		t.Fatalf("resposta: %d %v", r.status, r.body)
	}
	if av, held := a.balance(alice); av != 10000_00 || held != 0 {
		t.Fatalf("saldo mudou: %s / %s", av, held)
	}
	if n := a.outboxCount(bson.D{}); n != 0 {
		t.Fatalf("%d eventos na outbox", n)
	}
	// A mesma Idempotency-Key pode ser usada de novo: nada foi registrado.
	if r := a.pay("k-1", alice, "bob@example.com", "10.00"); r.status != http.StatusCreated {
		t.Fatalf("nova tentativa com a mesma chave: %d %v", r.status, r.body)
	}
}

func TestPixIdempotency(t *testing.T) {
	w := newWorld(t, worldOpts{})
	a := w.nodes[ispbA]

	first := a.pay("k-1", alice, "bob@example.com", "10.00")
	again := a.pay("k-1", alice, "bob@example.com", "10.00")
	if first.status != http.StatusCreated || again.status != http.StatusOK ||
		first.str("endToEndId") != again.str("endToEndId") || again.header.Get("Idempotent-Replayed") != "true" {
		t.Fatalf("repetição: %d %v", again.status, again.body)
	}
	if other := a.pay("k-1", alice, "bob@example.com", "11.00"); other.status != http.StatusUnprocessableEntity || other.str("code") != "IDEMPOTENCY_KEY_REUSED" {
		t.Fatalf("mesma chave, outro pedido: %d %v", other.status, other.body)
	}
	// A chave é por conta: a conta de Carol pode usar o mesmo valor.
	if c := a.pay("k-1", "1000002", "bob@example.com", "10.00"); c.status != http.StatusCreated {
		t.Fatalf("outra conta, mesma chave: %d %v", c.status, c.body)
	}

	a.waitState(first.str("endToEndId"), psp.StateConfirmed, 10*time.Second)
	if av, _ := a.balance(alice); av != 9990_00 {
		t.Fatalf("a repetição debitou de novo: %s", av)
	}

	// Requisições simultâneas com a mesma chave criam um só pagamento.
	results := make(chan response, 5)
	for range 5 {
		go func() { results <- a.pay("k-race", alice, "bob@example.com", "1.00") }()
	}
	ids := map[string]int{}
	for range 5 {
		r := <-results
		if r.status != http.StatusCreated && r.status != http.StatusOK {
			t.Fatalf("corrida: %d %v", r.status, r.body)
		}
		ids[r.str("endToEndId")]++
	}
	if len(ids) != 1 {
		t.Fatalf("corrida criou %d pagamentos: %v", len(ids), ids)
	}
}

func TestPixRequestValidation(t *testing.T) {
	w := newWorld(t, worldOpts{})
	a := w.nodes[ispbA]

	cases := []struct {
		name   string
		resp   response
		status int
		code   string
	}{
		{"sem Idempotency-Key", a.pay("", alice, "bob@example.com", "1.00"), 400, "INVALID_REQUEST"},
		{"valor zero", a.pay("v1", alice, "bob@example.com", "0.00"), 400, "INVALID_REQUEST"},
		{"conta inexistente", a.pay("v2", "9999999", "bob@example.com", "1.00"), 404, "ACCOUNT_NOT_FOUND"},
		{"chave inexistente", a.pay("v3", alice, "ninguem@example.com", "1.00"), 422, "KEY_NOT_FOUND"},
		{"chave do próprio PSP", a.pay("v4", alice, "carol@example.com", "1.00"), 422, "SAME_PSP_NOT_SUPPORTED"},
	}
	for _, c := range cases {
		if c.resp.status != c.status || c.resp.str("code") != c.code {
			t.Errorf("%s: %d %v, quer %d %s", c.name, c.resp.status, c.resp.body, c.status, c.code)
		}
	}
	if r := a.payment("E11111111202601010000AAAAAAAAAAA"); r.status != http.StatusNotFound {
		t.Errorf("pagamento inexistente: %d", r.status)
	}
	a.setAccountStatus(alice, psp.AccountBlocked)
	if r := a.pay("v5", alice, "bob@example.com", "1.00"); r.status != http.StatusUnprocessableEntity || r.str("code") != "ACCOUNT_NOT_ACTIVE" {
		t.Errorf("conta bloqueada: %d %v", r.status, r.body)
	}
}

// Com o relay fora do ar o pagamento fica PENDING, e a mensagem espera na
// outbox; ao subir, o relay publica o acumulado e o Pix se completa.
func TestOutboxSurvivesRelayDowntime(t *testing.T) {
	w := newWorld(t, worldOpts{skip: map[string]map[string]bool{ispbA: {"relay": true}}})
	a := w.nodes[ispbA]

	e2e := a.pay("k-1", alice, "bob@example.com", "10.00").str("endToEndId")
	time.Sleep(1500 * time.Millisecond)
	if st := a.payment(e2e).str("state"); st != psp.StatePending {
		t.Fatalf("estado sem relay = %s, quer PENDING", st)
	}
	if n := a.outboxCount(bson.D{{Key: "publishedAt", Value: nil}}); n != 1 {
		t.Fatalf("%d eventos pendentes na outbox, quer 1", n)
	}

	a.startRelay()
	a.waitState(e2e, psp.StateConfirmed, 10*time.Second)
	if n := a.outboxCount(bson.D{{Key: "publishedAt", Value: nil}}); n != 0 {
		t.Fatalf("%d eventos ainda pendentes", n)
	}
}

// Várias transações simultâneas: nada se perde nem se duplica, e os saldos
// fecham com o razão e com as contas PI.
func TestManyConcurrentPayments(t *testing.T) {
	w := newWorld(t, worldOpts{})
	a, b := w.nodes[ispbA], w.nodes[ispbB]

	const n = 30
	ids := make(chan string, n)
	for i := range n {
		go func() {
			r := a.pay("c-"+string(rune('a'+i%26))+string(rune('a'+i/26)), alice, "bob@example.com", "1.00")
			if r.status != http.StatusCreated {
				t.Errorf("pedido %d: %d %v", i, r.status, r.body)
			}
			ids <- r.str("endToEndId")
		}()
	}
	for range n {
		a.waitState(<-ids, psp.StateConfirmed, 20*time.Second)
	}
	if av, held := a.balance(alice); av != 10000_00-n*1_00 || held != 0 {
		t.Fatalf("saldo de A: %s / %s", av, held)
	}
	if pa, pb := w.piBalance(ispbA), w.piBalance(ispbB); pa != 1_000_000_00-n*1_00 || pb != 1_000_000_00+n*1_00 {
		t.Fatalf("contas PI: %s / %s", pa, pb)
	}
	a.checkLedger()
	b.checkLedger()
}

// Aplicar o resultado de novo (reentrega da mesma pacs.002) não mexe em nada.
func TestFinishIsIdempotent(t *testing.T) {
	w := newWorld(t, worldOpts{})
	a := w.nodes[ispbA]

	e2e := a.pay("k-1", alice, "bob@example.com", "10.00").str("endToEndId")
	a.waitState(e2e, psp.StateConfirmed, 10*time.Second)

	ctx := context.Background()
	for _, state := range []string{psp.StateConfirmed, psp.StateRejected} {
		if err := a.svc.Finish(ctx, e2e, state, "AB03", nil); err != nil {
			t.Fatal(err)
		}
	}
	if st := a.payment(e2e).str("state"); st != psp.StateConfirmed {
		t.Fatalf("estado = %s", st)
	}
	if av, held := a.balance(alice); av != 9990_00 || held != 0 {
		t.Fatalf("saldo: %s / %s", av, held)
	}
	a.checkLedger()
	// Resultado de um Pix que este PSP nunca enviou é ignorado.
	if err := a.svc.Finish(ctx, "E11111111202601010000ZZZZZZZZZZZ", psp.StateConfirmed, "", nil); err != nil {
		t.Fatal(err)
	}
}
