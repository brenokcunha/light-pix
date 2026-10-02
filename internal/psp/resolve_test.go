package psp_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/brenokcunha/light-pix/internal/broker"
	"github.com/brenokcunha/light-pix/internal/pix"
	"github.com/brenokcunha/light-pix/internal/psp"
)

// O SPI responde 503 às duas primeiras tentativas: as filas de retry
// reenviam sozinhas e o Pix se completa.
func TestSPIFailureIsRetried(t *testing.T) {
	w := newWorld(t, worldOpts{})
	a := w.nodes[ispbA]
	w.proxy.failPosts.Store(2)

	start := time.Now()
	e2e := a.pay("k-1", alice, "bob@example.com", "10.00").str("endToEndId")
	a.waitState(e2e, psp.StateConfirmed, 15*time.Second)
	if took := time.Since(start); took < testDelays[0]+testDelays[1] {
		t.Fatalf("confirmou em %v: as esperas do retry (%v + %v) não foram respeitadas", took, testDelays[0], testDelays[1])
	}
	if w.proxy.failPosts.Load() != 0 {
		t.Fatal("as falhas injetadas não foram consumidas")
	}
	a.checkLedger()
}

// O SPI fica fora do ar além de todas as tentativas: a mensagem vai para a DLQ
// e o resolver a devolve à fila quando o SPI volta.
func TestDLQMessageIsRedrivenAfterOutage(t *testing.T) {
	w := newWorld(t, worldOpts{resolver: true})
	a := w.nodes[ispbA]
	w.proxy.failPosts.Store(int32(1 + len(testDelays))) // tentativa original + todos os retries

	e2e := a.pay("k-1", alice, "bob@example.com", "10.00").str("endToEndId")
	// Enquanto o SPI está fora, o pagamento fica PENDING com a reserva feita.
	eventually(t, 10*time.Second, "mensagem na DLQ", func() bool {
		return w.proxy.failPosts.Load() == 0
	})
	a.waitState(e2e, psp.StateConfirmed, 20*time.Second)
	if av, held := a.balance(alice); av != 9990_00 || held != 0 {
		t.Fatalf("saldo: %s / %s", av, held)
	}
	if n, _ := a.client.Database(a.db).Collection("dead_letters").CountDocuments(context.Background(), bson.D{}); n != 0 {
		t.Fatalf("%d mensagens estacionadas, quer 0", n)
	}
	a.checkLedger()
}

// O SPI nunca volta: depois de MaxRedrives voltas pela DLQ a mensagem é
// estacionada e, como o SPI não conhece a transação, o Pix é rejeitado e a
// reserva volta ao pagador.
func TestUndeliverableMessageIsParkedAndPaymentRefunded(t *testing.T) {
	w := newWorld(t, worldOpts{resolver: true})
	a := w.nodes[ispbA]
	w.proxy.failPosts.Store(1 << 20)

	e2e := a.pay("k-1", alice, "bob@example.com", "10.00").str("endToEndId")
	done := a.waitState(e2e, psp.StateRejected, 40*time.Second)
	if done.str("reason") != "ED05" {
		t.Fatalf("motivo = %q, quer ED05", done.str("reason"))
	}
	if av, held := a.balance(alice); av != 10000_00 || held != 0 {
		t.Fatalf("saldo após estacionar: %s / %s", av, held)
	}
	// O pagamento é resolvido antes de a mensagem ser gravada em dead_letters.
	var parked struct {
		Queue   string
		Headers map[string]string
	}
	eventually(t, 5*time.Second, "mensagem em dead_letters", func() bool {
		return a.client.Database(a.db).Collection("dead_letters").FindOne(context.Background(), bson.D{}).Decode(&parked) == nil
	})
	if parked.Queue != broker.QueueSPIOut || parked.Headers[broker.HeaderLastError] == "" {
		t.Fatalf("dead_letters: %+v", parked)
	}
	if n := a.queueDepth(broker.DLQ(broker.QueueSPIOut)); n != 0 {
		t.Fatalf("%d mensagens ainda na DLQ", n)
	}
	a.checkLedger()
}

// Mensagem malformada nunca passa: vai à DLQ, volta MaxRedrives vezes e fica
// estacionada para análise. Não derruba nem trava o consumidor.
func TestPoisonMessageEndsParked(t *testing.T) {
	w := newWorld(t, worldOpts{resolver: true})
	a := w.nodes[ispbA]

	a.publish(broker.QueueResults, []byte(`isto não é uma pacs.002`))

	dead := a.client.Database(a.db).Collection("dead_letters")
	eventually(t, 30*time.Second, "mensagem estacionada", func() bool {
		n, _ := dead.CountDocuments(context.Background(), bson.D{})
		return n == 1
	})
	var parked struct {
		Body    string
		Headers map[string]string
	}
	must(t, dead.FindOne(context.Background(), bson.D{}).Decode(&parked))
	if parked.Body != "isto não é uma pacs.002" || parked.Headers[broker.HeaderRedrive] != "3" {
		t.Fatalf("estacionada: %+v", parked)
	}
	// O consumidor segue funcionando.
	e2e := a.pay("k-1", alice, "bob@example.com", "5.00").str("endToEndId")
	a.waitState(e2e, psp.StateConfirmed, 10*time.Second)
}

// A pacs.002 de confirmação se perde (fila esvaziada): o resolver pergunta ao
// SPI e fecha o Pix enviado.
func TestLostConfirmationIsResolvedFromSPIStatus(t *testing.T) {
	w := newWorld(t, worldOpts{
		resolver: true,
		skip:     map[string]map[string]bool{ispbA: {"worker": true}},
	})
	a, b := w.nodes[ispbA], w.nodes[ispbB]

	e2e := a.pay("k-1", alice, "bob@example.com", "30.00").str("endToEndId")
	b.waitState(e2e, psp.StateCredited, 10*time.Second)
	eventually(t, 5*time.Second, "ACSC na fila de A", func() bool { return a.queueDepth(broker.QueueResults) >= 1 })
	a.purge(broker.QueueResults) // a mensagem se perde
	if st := a.payment(e2e).str("state"); st != psp.StateSent {
		t.Fatalf("estado antes do resolver = %s, quer SENT", st)
	}

	done := a.waitState(e2e, psp.StateConfirmed, 10*time.Second)
	if h := history(done); !slices.Equal(h, []string{"PENDING", "SENT", "CONFIRMED"}) {
		t.Fatalf("histórico = %v", h)
	}
	if av, held := a.balance(alice); av != 9970_00 || held != 0 {
		t.Fatalf("saldo de Alice: %s / %s", av, held)
	}
	a.checkLedger()
}

// Mesmo no lado recebedor: o ACSC de B se perde e o resolver credita a conta.
func TestLostSettlementOnReceiverIsResolved(t *testing.T) {
	w := newWorld(t, worldOpts{
		resolver: true,
		skip:     map[string]map[string]bool{ispbB: {"worker": true}},
	})
	a, b := w.nodes[ispbA], w.nodes[ispbB]
	b.startIncoming() // B responde às pacs.008, mas ninguém lê pix.results

	e2e := a.pay("k-1", alice, "bob@example.com", "30.00").str("endToEndId")
	a.waitState(e2e, psp.StateConfirmed, 10*time.Second)
	eventually(t, 5*time.Second, "ACSC na fila de B", func() bool { return b.queueDepth(broker.QueueResults) >= 1 })
	b.purge(broker.QueueResults)
	if st := b.payment(e2e).str("state"); st != psp.StateAccepted {
		t.Fatalf("estado antes do resolver = %s, quer ACCEPTED", st)
	}

	b.waitState(e2e, psp.StateCredited, 10*time.Second)
	if av, held := b.balance(bob); av != 10030_00 || held != 0 {
		t.Fatalf("saldo de Bob: %s / %s", av, held)
	}
	b.checkLedger()
}

// A conciliação confere cada liquidação da conta PI com o PSP; sem problema o
// relatório sai OK e com os mesmos totais dos dois lados.
func TestReconciliationCleanRun(t *testing.T) {
	w := newWorld(t, worldOpts{})
	a, b := w.nodes[ispbA], w.nodes[ispbB]
	var ids []string
	for i, n := range []string{"3.00", "4.00", "5.00"} {
		ids = append(ids, a.pay("a"+string(rune('0'+i)), alice, "bob@example.com", json.Number(n)).str("endToEndId"))
	}
	ids = append(ids, b.pay("b0", bob, "alice@example.com", "2.50").str("endToEndId"))
	for _, id := range ids[:3] {
		a.waitState(id, psp.StateConfirmed, 10*time.Second)
		b.waitState(id, psp.StateCredited, 10*time.Second)
	}
	b.waitState(ids[3], psp.StateConfirmed, 10*time.Second)
	a.waitState(ids[3], psp.StateCredited, 10*time.Second)

	from, to := time.Now().Add(-time.Hour), time.Now().Add(time.Hour)
	for _, n := range []*node{a, b} {
		rep, err := n.resolver.Reconcile(context.Background(), from, to, "manual")
		must(t, err)
		if rep.Status != "OK" || len(rep.Divergences) != 0 {
			t.Fatalf("PSP %s: %s com divergências %+v", n.ispb, rep.Status, rep.Divergences)
		}
		if rep.PIEntries != 4 || rep.PIDebits != rep.LocalOut || rep.PICredits != rep.LocalIn {
			t.Fatalf("PSP %s: totais %+v", n.ispb, rep)
		}
	}
	rep, _ := a.resolver.Reconcile(context.Background(), from, to, "manual")
	if rep.PIDebits != 12_00 || rep.PICredits != 2_50 {
		t.Fatalf("totais de A: débitos %s, créditos %s", rep.PIDebits, rep.PICredits)
	}
}

// Cada tipo de divergência é detectado e entra no relatório, que também fica
// disponível pela API administrativa.
func TestReconciliationFindsDivergences(t *testing.T) {
	w := newWorld(t, worldOpts{})
	a := w.nodes[ispbA]
	ctx := context.Background()
	payments := a.client.Database(a.db).Collection("payments")

	lost := a.pay("k-1", alice, "bob@example.com", "10.00").str("endToEndId")
	wrong := a.pay("k-2", alice, "bob@example.com", "20.00").str("endToEndId")
	for _, id := range []string{lost, wrong} {
		a.waitState(id, psp.StateConfirmed, 10*time.Second)
	}

	// 1) o PSP perdeu a confirmação de um pagamento que a conta PI liquidou;
	// 2) o valor registrado no PSP difere do liquidado;
	// 3) o PSP tem um pagamento liquidado que não existe na conta PI.
	_, err := payments.UpdateOne(ctx, bson.D{{Key: "_id", Value: lost}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "state", Value: psp.StateSent}}}})
	must(t, err)
	_, err = payments.UpdateOne(ctx, bson.D{{Key: "_id", Value: wrong}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "amount", Value: 21_00}}}})
	must(t, err)
	ghost := pix.NewEndToEndID(ispbA, time.Now())
	settled := time.Now().UTC()
	_, err = payments.InsertOne(ctx, psp.Payment{EndToEndID: ghost, Direction: psp.DirectionOut, State: psp.StateConfirmed,
		Amount: 7_00, SettledAt: &settled, CreatedAt: settled, UpdatedAt: settled})
	must(t, err)

	handler := http.NewServeMux()
	a.resolver.Routes(handler)
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	from, to := time.Now().Add(-time.Hour), time.Now().Add(time.Hour)
	resp, err := http.Post(srv.URL+"/admin/reconciliation?from="+from.UTC().Format(time.RFC3339)+"&to="+to.UTC().Format(time.RFC3339), "application/json", nil)
	must(t, err)
	defer resp.Body.Close()
	var rep psp.Report
	must(t, json.NewDecoder(resp.Body).Decode(&rep))

	if resp.StatusCode != http.StatusOK || rep.Status != "DIVERGENT" || rep.Trigger != "manual" {
		t.Fatalf("relatório: %d %+v", resp.StatusCode, rep)
	}
	got := map[string]psp.Divergence{}
	for _, d := range rep.Divergences {
		got[d.Type] = d
	}
	if len(rep.Divergences) != 3 {
		t.Fatalf("%d divergências: %+v", len(rep.Divergences), rep.Divergences)
	}
	if d := got[psp.DivPIWithoutLocal]; d.EndToEndID != lost || d.PIAmount != 10_00 || d.LocalState != "OUT/SENT" {
		t.Errorf("PI sem pagamento liquidado: %+v", d)
	}
	if d := got[psp.DivAmountMismatch]; d.EndToEndID != wrong || d.PIAmount != 20_00 || d.LocalAmount != 21_00 {
		t.Errorf("valor diferente: %+v", d)
	}
	if d := got[psp.DivLocalWithoutPI]; d.EndToEndID != ghost || d.LocalAmount != 7_00 {
		t.Errorf("liquidado sem lançamento na conta PI: %+v", d)
	}

	// O último relatório fica disponível na API e na coleção.
	last, err := http.Get(srv.URL + "/admin/reconciliation")
	must(t, err)
	defer last.Body.Close()
	var lastRep psp.Report
	must(t, json.NewDecoder(last.Body).Decode(&lastRep))
	if lastRep.ID != rep.ID {
		t.Errorf("GET devolveu outro relatório")
	}
	if n, _ := a.client.Database(a.db).Collection("reconciliation_reports").CountDocuments(ctx, bson.D{}); n != 1 {
		t.Errorf("%d relatórios gravados, quer 1", n)
	}
}

// O resolver conserta a divergência "o PSP perdeu a confirmação", e a
// conciliação seguinte fecha limpa.
func TestResolverRepairsWhatReconciliationFinds(t *testing.T) {
	w := newWorld(t, worldOpts{})
	a := w.nodes[ispbA]
	ctx := context.Background()

	e2e := a.pay("k-1", alice, "bob@example.com", "10.00").str("endToEndId")
	a.waitState(e2e, psp.StateConfirmed, 10*time.Second)

	// Desfaz a confirmação no PSP (estado, saldo e razão), como se nunca tivesse chegado.
	db := a.client.Database(a.db)
	old := time.Now().Add(-time.Hour).UTC()
	_, err := db.Collection("payments").UpdateOne(ctx, bson.D{{Key: "_id", Value: e2e}}, bson.D{
		{Key: "$set", Value: bson.D{{Key: "state", Value: psp.StateSent}, {Key: "updatedAt", Value: old}}},
		{Key: "$unset", Value: bson.D{{Key: "settledAt", Value: ""}, {Key: "finishedAt", Value: ""}}}})
	must(t, err)
	_, err = db.Collection("accounts").UpdateOne(ctx, bson.D{{Key: "number", Value: alice}},
		bson.D{{Key: "$inc", Value: bson.D{{Key: "balance.held", Value: 10_00}}}})
	must(t, err)
	_, err = db.Collection("ledger_entries").DeleteOne(ctx, bson.D{{Key: "endToEndId", Value: e2e}, {Key: "kind", Value: psp.EntryConfirm}})
	must(t, err)

	from, to := time.Now().Add(-time.Hour), time.Now().Add(time.Hour)
	before, err := a.resolver.Reconcile(ctx, from, to, "manual")
	must(t, err)
	// Sem settledAt o pagamento nem entra no conjunto local: aparece só do lado da conta PI.
	if before.Status != "DIVERGENT" || before.Divergences[0].Type != psp.DivPIWithoutLocal {
		t.Fatalf("antes do reparo: %+v", before)
	}

	n, err := a.resolver.ResolvePending(ctx)
	must(t, err)
	if n != 1 {
		t.Fatalf("resolver examinou %d pendências, quer 1", n)
	}
	if st := a.payment(e2e).str("state"); st != psp.StateConfirmed {
		t.Fatalf("estado depois do resolver = %s", st)
	}
	a.checkLedger()
	after, err := a.resolver.Reconcile(ctx, from, to, "manual")
	must(t, err)
	if after.Status != "OK" {
		t.Fatalf("depois do reparo: %+v", after.Divergences)
	}
}

// A reconciliação diária roda sozinha uma única vez para o dia anterior.
func TestDailyReconciliationRunsOnce(t *testing.T) {
	w := newWorld(t, worldOpts{resolver: true, reconcileAt: "00:00"})
	a := w.nodes[ispbA]
	reports := a.client.Database(a.db).Collection("reconciliation_reports")

	eventually(t, 5*time.Second, "relatório diário", func() bool {
		n, _ := reports.CountDocuments(context.Background(), bson.D{{Key: "trigger", Value: "daily"}})
		return n == 1
	})
	time.Sleep(1500 * time.Millisecond) // vários ciclos depois
	if n, _ := reports.CountDocuments(context.Background(), bson.D{{Key: "trigger", Value: "daily"}}); n != 1 {
		t.Fatalf("%d relatórios diários, quer 1", n)
	}
	var rep psp.Report
	must(t, reports.FindOne(context.Background(), bson.D{}).Decode(&rep))
	today := time.Now().UTC().Truncate(24 * time.Hour)
	if !rep.To.Equal(today) || !rep.From.Equal(today.Add(-24*time.Hour)) || rep.Status != "OK" {
		t.Fatalf("janela %v → %v, status %s", rep.From, rep.To, rep.Status)
	}
}
