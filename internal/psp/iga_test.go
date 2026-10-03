package psp_test

import (
	"context"
	"testing"
	"time"

	"github.com/brenokcunha/light-pix/internal/faults"
	"github.com/brenokcunha/light-pix/internal/psp"
	"github.com/brenokcunha/light-pix/internal/sim/bacen"
)

// reportIGA faz os dois PSPs enviarem o que mediram e espera o SPI enviar o
// que ele mediu (lote de 1s).
func reportIGA(t *testing.T, w *world) {
	t.Helper()
	for _, n := range w.nodes {
		if _, err := n.resolver.ReportIGA(context.Background()); err != nil {
			t.Fatalf("PSP %s: %v", n.ispb, err)
		}
	}
}

func igaView(t *testing.T, w *world, ispb string, ready func(bacen.IGAReport) bool) bacen.IGAReport {
	t.Helper()
	var rep bacen.IGAReport
	eventually(t, 10*time.Second, "relatório do IGA de "+ispb, func() bool {
		reportIGA(t, w) // idempotente: reenviar só confirma
		var err error
		rep, err = w.bacen.IGA(context.Background(), ispb, time.Hour)
		must(t, err)
		return ready(rep)
	})
	return rep
}

// O regulador enxerga o mesmo fluxo que os PSPs: tempos do pagador, da
// iniciação, da liquidação e da resposta do recebedor.
func TestIGAReflectsThePixFlow(t *testing.T) {
	w := newWorld(t, worldOpts{})
	a, b := w.nodes[ispbA], w.nodes[ispbB]

	const n = 5
	for i := range n {
		id := a.pay("k-"+string(rune('a'+i)), alice, "bob@example.com", "2.00").str("endToEndId")
		a.waitState(id, psp.StateConfirmed, 10*time.Second)
		b.waitState(id, psp.StateCredited, 10*time.Second)
	}

	repA := igaView(t, w, ispbA, func(r bacen.IGAReport) bool {
		return r.Transactions == n && r.PayerExperience.Count == n && r.Initiation.Count == n && r.Settlement.Count == n
	})
	if repA.Settled != n || len(repA.Rejected) != 0 {
		t.Fatalf("A: liquidadas %d, rejeitadas %v", repA.Settled, repA.Rejected)
	}
	// O Pix é rápido: todas as metas do IGA são cumpridas.
	for name, ok := range repA.MeetsTargets {
		if ok == nil || !*ok {
			t.Errorf("meta %s não cumprida: %+v", name, repA)
		}
	}
	// A experiência do pagador (t0 → t6a) inclui a iniciação e a liquidação.
	if repA.PayerExperience.P50 < repA.Initiation.P50 {
		t.Errorf("experiência do pagador (%v) menor que a iniciação (%v)", repA.PayerExperience.P50, repA.Initiation.P50)
	}

	// B é o recebedor: o IGA mede o tempo que ele levou para aceitar.
	repB := igaView(t, w, ispbB, func(r bacen.IGAReport) bool { return r.CreditorResponse.Count == n })
	if repB.Transactions != 0 || repB.CreditorResponse.P99 <= 0 || repB.CreditorResponse.P99 > 5 {
		t.Errorf("B: %+v", repB)
	}

	// Reenviar os relatórios não duplica nada.
	reportIGA(t, w)
	again, err := w.bacen.IGA(context.Background(), ispbA, time.Hour)
	must(t, err)
	if again.PayerExperience.Count != n || again.Transactions != n {
		t.Errorf("reenvio duplicou: %+v", again)
	}
}

// Rejeições aparecem por motivo, e a experiência do pagador reflete a espera.
func TestIGACountsRejectionsByReason(t *testing.T) {
	w := newWorld(t, worldOpts{creditorTimeout: time.Second})
	a := w.nodes[ispbA]
	w.setSPIFaults(faults.Config{CreditorTimeoutRate: 1})

	id := a.pay("k-1", alice, "bob@example.com", "2.00").str("endToEndId")
	a.waitState(id, psp.StateRejected, 10*time.Second)

	rep := igaView(t, w, ispbA, func(r bacen.IGAReport) bool {
		return r.Transactions == 1 && r.PayerExperience.Count == 1
	})
	if rep.Settled != 0 || rep.Rejected["AB03"] != 1 {
		t.Fatalf("rejeições: %+v", rep.Rejected)
	}
	if rep.PayerExperience.P50 < 1 {
		t.Errorf("a espera do prazo (1s) deveria aparecer na experiência do pagador: %v", rep.PayerExperience.P50)
	}
	if rep.Settlement.Count != 0 || rep.CreditorResponse.Count != 0 {
		t.Errorf("sem liquidação nem resposta do recebedor: %+v", rep)
	}
}
