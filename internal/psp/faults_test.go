package psp_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/brenokcunha/light-pix/internal/broker"
	"github.com/brenokcunha/light-pix/internal/faults"
	"github.com/brenokcunha/light-pix/internal/psp"
)

// Toda pacs.002 se perde no SPI: a transação liquida, mas nenhum PSP é
// avisado. O resolver descobre pelo status e fecha os dois lados.
func TestInjectedLostPacs002IsRecoveredByResolver(t *testing.T) {
	w := newWorld(t, worldOpts{resolver: true})
	a, b := w.nodes[ispbA], w.nodes[ispbB]
	w.setSPIFaults(faults.Config{DropPacs002: 1})

	e2e := a.pay("k-1", alice, "bob@example.com", "20.00").str("endToEndId")
	a.waitState(e2e, psp.StateConfirmed, 15*time.Second)
	b.waitState(e2e, psp.StateCredited, 15*time.Second)

	if n := a.queueDepth(broker.QueueResults) + b.queueDepth(broker.QueueResults); n != 0 {
		t.Fatalf("%d pacs.002 chegaram apesar da falha injetada", n)
	}
	if av, held := a.balance(alice); av != 9980_00 || held != 0 {
		t.Fatalf("Alice: %s / %s", av, held)
	}
	if av, _ := b.balance(bob); av != 10020_00 {
		t.Fatalf("Bob: %s", av)
	}
	a.checkLedger()
	b.checkLedger()
}

// O recebedor "não responde": o SPI não encaminha a pacs.008, o prazo vence e
// só o pagador é avisado (AB03). O recebedor não vê nada.
func TestInjectedCreditorTimeoutRejectsPayer(t *testing.T) {
	w := newWorld(t, worldOpts{creditorTimeout: time.Second})
	a, b := w.nodes[ispbA], w.nodes[ispbB]
	w.setSPIFaults(faults.Config{CreditorTimeoutRate: 1})

	e2e := a.pay("k-1", alice, "bob@example.com", "20.00").str("endToEndId")
	done := a.waitState(e2e, psp.StateRejected, 10*time.Second)
	if done.str("reason") != "AB03" {
		t.Fatalf("motivo = %q, quer AB03", done.str("reason"))
	}
	time.Sleep(time.Second) // dá tempo de uma notificação indevida chegar
	if r := b.payment(e2e); r.status != http.StatusNotFound {
		t.Fatalf("o recebedor tem registro do Pix que nunca viu: %d %v", r.status, r.body)
	}
	for _, q := range []string{broker.QueueResults, broker.QueueIncoming} {
		if n := b.queueDepth(q) + b.queueDepth(broker.DLQ(q)); n != 0 {
			t.Fatalf("fila %s do recebedor tem %d mensagens", q, n)
		}
	}
	if av, held := a.balance(alice); av != 10000_00 || held != 0 {
		t.Fatalf("Alice: %s / %s", av, held)
	}
}

// SPI indisponível por completo (503 em tudo) e depois recuperado: o Pix
// espera nas tentativas (e, esgotadas, na DLQ) e conclui quando o SPI volta.
func TestInjectedSPIOutageAndRecovery(t *testing.T) {
	w := newWorld(t, worldOpts{resolver: true})
	a := w.nodes[ispbA]
	w.setSPIFaults(faults.Config{Unavailable: true})

	e2e := a.pay("k-1", alice, "bob@example.com", "20.00").str("endToEndId")
	time.Sleep(800 * time.Millisecond)
	if st := a.payment(e2e).str("state"); st != psp.StatePending {
		t.Fatalf("com o SPI fora o estado é %s, quer PENDING", st)
	}
	if av, held := a.balance(alice); av != 9980_00 || held != 20_00 {
		t.Fatalf("a reserva deveria seguir feita: %s / %s", av, held)
	}

	w.clearSPIFaults()
	a.waitState(e2e, psp.StateConfirmed, 20*time.Second)
	a.checkLedger()
}

// Latência alta e erros intermitentes no SPI: todos os Pix terminam bem. Quem
// esgota as tentativas cai na DLQ e o resolver o devolve.
func TestInjectedSlowAndFlakySPIStillCompletes(t *testing.T) {
	w := newWorld(t, worldOpts{resolver: true})
	a := w.nodes[ispbA]
	w.setSPIFaults(faults.Config{
		ErrorRate: 0.4,
		Latency:   faults.Latency{Mode: faults.LatencyUniform, MinMs: 50, MaxMs: 200},
	})

	const n = 8
	ids := make([]string, n)
	for i := range n {
		ids[i] = a.pay("k-"+string(rune('a'+i)), alice, "bob@example.com", "1.00").str("endToEndId")
	}
	for _, id := range ids {
		a.waitState(id, psp.StateConfirmed, 30*time.Second)
	}
	if av, held := a.balance(alice); av != 10000_00-n*1_00 || held != 0 {
		t.Fatalf("Alice: %s / %s", av, held)
	}
	a.checkLedger()
}
