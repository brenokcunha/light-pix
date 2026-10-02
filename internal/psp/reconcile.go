package psp

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/brenokcunha/light-pix/internal/httpx"
	"github.com/brenokcunha/light-pix/internal/pix"
	"github.com/brenokcunha/light-pix/internal/sim/bacen"
	"github.com/brenokcunha/light-pix/internal/store"
)

// Tipos de divergência da reconciliação.
const (
	// DivPIWithoutLocal: o extrato da conta PI tem a liquidação, mas o PSP não
	// tem o pagamento como liquidado (parado, rejeitado ou inexistente).
	DivPIWithoutLocal = "PI_ENTRY_WITHOUT_LOCAL_SETTLEMENT"
	// DivLocalWithoutPI: o PSP deu o pagamento como liquidado, mas a conta PI
	// não tem o lançamento.
	DivLocalWithoutPI = "LOCAL_SETTLEMENT_WITHOUT_PI_ENTRY"
	DivAmountMismatch = "AMOUNT_MISMATCH"
	DivDirection      = "DIRECTION_MISMATCH"
)

// Divergence é uma diferença entre o extrato da conta PI e o PSP.
type Divergence struct {
	Type        string     `bson:"type" json:"type"`
	EndToEndID  string     `bson:"endToEndId" json:"endToEndId"`
	PIAmount    pix.Amount `bson:"piAmount" json:"piAmount"`       // valor no extrato (0 se não há lançamento)
	LocalAmount pix.Amount `bson:"localAmount" json:"localAmount"` // valor no PSP (0 se não há pagamento)
	LocalState  string     `bson:"localState,omitempty" json:"localState,omitempty"`
}

// Report é o resultado de uma reconciliação de [From, To).
type Report struct {
	ID        bson.ObjectID `bson:"_id" json:"id"`
	ISPB      string        `bson:"ispb" json:"ispb"`
	Trigger   string        `bson:"trigger" json:"trigger"` // daily | manual
	From      time.Time     `bson:"from" json:"from"`
	To        time.Time     `bson:"to" json:"to"`
	CreatedAt time.Time     `bson:"createdAt" json:"createdAt"`
	Status    string        `bson:"status" json:"status"` // OK | DIVERGENT

	PIEntries int        `bson:"piEntries" json:"piEntries"` // liquidações no extrato
	PIDebits  pix.Amount `bson:"piDebits" json:"piDebits"`   // saídas (Pix enviados)
	PICredits pix.Amount `bson:"piCredits" json:"piCredits"` // entradas (Pix recebidos)
	LocalOut  pix.Amount `bson:"localOut" json:"localOut"`   // enviados CONFIRMED no PSP
	LocalIn   pix.Amount `bson:"localIn" json:"localIn"`     // recebidos CREDITED no PSP

	Divergences []Divergence `bson:"divergences" json:"divergences"`
}

// Reconcile compara as liquidações da conta PI deste PSP em [from, to) com os
// pagamentos liquidados no PSP (CONFIRMED e CREDITED) e grava o relatório.
// Liquidação aparece nos dois lados com o mesmo EndToEndId, valor e instante,
// então cada lado precisa achar o outro.
func (r *Resolver) Reconcile(ctx context.Context, from, to time.Time, trigger string) (Report, error) {
	ctx, span := r.tracer.Start(ctx, "reconciliation.run")
	defer span.End()

	entries, err := r.pi.Statement(ctx, r.svc.ispb, from, to)
	if err != nil {
		return Report{}, fmt.Errorf("extrato da conta PI: %w", err)
	}
	cur, err := r.svc.store.payments.Find(ctx, bson.D{
		{Key: "state", Value: bson.D{{Key: "$in", Value: bson.A{StateConfirmed, StateCredited}}}},
		{Key: "settledAt", Value: bson.D{{Key: "$gte", Value: from.UTC()}, {Key: "$lt", Value: to.UTC()}}},
	})
	if err != nil {
		return Report{}, err
	}
	var settled []Payment
	if err := cur.All(ctx, &settled); err != nil {
		return Report{}, err
	}
	local := make(map[string]Payment, len(settled))
	rep := Report{ID: bson.NewObjectID(), ISPB: r.svc.ispb, Trigger: trigger, From: from.UTC(), To: to.UTC(),
		CreatedAt: store.Now(), Divergences: []Divergence{}}
	for _, p := range settled {
		local[p.EndToEndID] = p
		if p.Direction == DirectionOut {
			rep.LocalOut += p.Amount
		} else {
			rep.LocalIn += p.Amount
		}
	}

	inStatement := map[string]bool{}
	for _, e := range entries {
		var direction string
		switch e.Kind {
		case bacen.KindSettlementDebit:
			direction, rep.PIDebits = DirectionOut, rep.PIDebits-e.Amount
		case bacen.KindSettlementCredit:
			direction, rep.PICredits = DirectionIn, rep.PICredits+e.Amount
		default:
			continue // aportes e retiradas STR não são Pix
		}
		rep.PIEntries++
		inStatement[e.Reference] = true
		amount := e.Amount
		if amount < 0 {
			amount = -amount
		}

		p, ok := local[e.Reference]
		switch {
		case !ok:
			d := Divergence{Type: DivPIWithoutLocal, EndToEndID: e.Reference, PIAmount: amount, LocalState: "MISSING"}
			switch found, err := r.svc.store.FindPayment(ctx, e.Reference); {
			case err == nil:
				d.LocalState, d.LocalAmount = found.Direction+"/"+found.State, found.Amount
			case !errors.Is(err, mongo.ErrNoDocuments):
				return Report{}, err
			}
			rep.Divergences = append(rep.Divergences, d)
		case p.Direction != direction:
			rep.Divergences = append(rep.Divergences, Divergence{Type: DivDirection, EndToEndID: e.Reference,
				PIAmount: amount, LocalAmount: p.Amount, LocalState: p.Direction + "/" + p.State})
		case p.Amount != amount:
			rep.Divergences = append(rep.Divergences, Divergence{Type: DivAmountMismatch, EndToEndID: e.Reference,
				PIAmount: amount, LocalAmount: p.Amount, LocalState: p.Direction + "/" + p.State})
		}
	}
	for e2e, p := range local {
		if !inStatement[e2e] {
			rep.Divergences = append(rep.Divergences, Divergence{Type: DivLocalWithoutPI, EndToEndID: e2e,
				LocalAmount: p.Amount, LocalState: p.Direction + "/" + p.State})
		}
	}
	slices.SortFunc(rep.Divergences, func(a, b Divergence) int {
		return cmp.Or(cmp.Compare(a.Type, b.Type), cmp.Compare(a.EndToEndID, b.EndToEndID))
	})
	rep.Status = "OK"
	if len(rep.Divergences) > 0 {
		rep.Status = "DIVERGENT"
	}

	if _, err := r.svc.store.reports.InsertOne(ctx, rep); err != nil {
		return Report{}, err
	}
	r.lastReport.Store(&rep)
	r.recRuns.Add(ctx, 1, metric.WithAttributes(attribute.String("status", rep.Status)))
	for _, d := range rep.Divergences {
		r.recDiverge.Add(ctx, 1, metric.WithAttributes(attribute.String("type", d.Type)))
	}
	span.SetAttributes(attribute.String("reconciliation.status", rep.Status), attribute.Int("reconciliation.divergences", len(rep.Divergences)))
	level := slog.LevelInfo
	if rep.Status != "OK" {
		level = slog.LevelError
	}
	slog.Log(ctx, level, "reconciliação concluída", "status", rep.Status, "from", rep.From, "to", rep.To,
		"pi_entries", rep.PIEntries, "divergences", len(rep.Divergences))
	return rep, nil
}

// reconcileIfDue roda a reconciliação diária do dia UTC anterior, uma vez por
// dia, depois do horário configurado. Consultar o banco (e não guardar o
// último dia na memória) faz reinícios e réplicas não repetirem o trabalho.
func (r *Resolver) reconcileIfDue(ctx context.Context) error {
	at, err := time.Parse("15:04", r.cfg.ReconcileAt)
	if err != nil {
		return fmt.Errorf("RECONCILE_AT inválido (use HH:MM): %w", err)
	}
	now := time.Now().UTC()
	today := now.Truncate(24 * time.Hour)
	if now.Before(today.Add(time.Duration(at.Hour())*time.Hour + time.Duration(at.Minute())*time.Minute)) {
		return nil
	}
	from, to := today.Add(-24*time.Hour), today
	n, err := r.svc.store.reports.CountDocuments(ctx, bson.D{{Key: "trigger", Value: "daily"}, {Key: "from", Value: from}})
	if err != nil || n > 0 {
		return err
	}
	_, err = r.Reconcile(ctx, from, to, "daily")
	return err
}

// LoadLastReport carrega o último relatório gravado, para as métricas da
// reconciliação não começarem vazias depois de um reinício.
func (r *Resolver) LoadLastReport(ctx context.Context) error {
	var rep Report
	err := r.svc.store.reports.FindOne(ctx, bson.D{}, options.FindOne().SetSort(bson.D{{Key: "createdAt", Value: -1}})).Decode(&rep)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil
	}
	if err == nil {
		r.lastReport.Store(&rep)
	}
	return err
}

// Routes registra a API administrativa do resolver (sem autenticação: é um
// laboratório local).
//
//	POST /admin/reconciliation?from=&to=  roda agora (padrão: o dia UTC anterior)
//	GET  /admin/reconciliation            último relatório
//	GET  /admin/dead-letters              mensagens estacionadas
func (r *Resolver) Routes(mux *http.ServeMux) {
	mux.HandleFunc("POST /admin/reconciliation", r.handleReconcile)
	mux.HandleFunc("GET /admin/reconciliation", r.handleLastReport)
	mux.HandleFunc("GET /admin/dead-letters", r.handleDeadLetters)
}

func (r *Resolver) handleReconcile(w http.ResponseWriter, req *http.Request) {
	today := time.Now().UTC().Truncate(24 * time.Hour)
	from, to := today.Add(-24*time.Hour), today
	var err error
	if v := req.URL.Query().Get("from"); v != "" {
		if from, err = time.Parse(time.RFC3339, v); err != nil {
			httpx.WriteError(w, http.StatusBadRequest, "INVALID_REQUEST", "from deve ser RFC 3339")
			return
		}
	}
	if v := req.URL.Query().Get("to"); v != "" {
		if to, err = time.Parse(time.RFC3339, v); err != nil {
			httpx.WriteError(w, http.StatusBadRequest, "INVALID_REQUEST", "to deve ser RFC 3339")
			return
		}
	}
	if !to.After(from) {
		httpx.WriteError(w, http.StatusBadRequest, "INVALID_REQUEST", "to deve ser depois de from")
		return
	}
	rep, err := r.Reconcile(req.Context(), from, to, "manual")
	if err != nil {
		slog.ErrorContext(req.Context(), "reconciliação manual falhou", "err", err)
		httpx.WriteError(w, http.StatusBadGateway, "RECONCILIATION_FAILED", err.Error())
		return
	}
	httpx.WriteJSON(w, http.StatusOK, rep)
}

func (r *Resolver) handleLastReport(w http.ResponseWriter, req *http.Request) {
	if rep := r.lastReport.Load(); rep != nil {
		httpx.WriteJSON(w, http.StatusOK, rep)
		return
	}
	httpx.WriteError(w, http.StatusNotFound, "NO_REPORT", "nenhuma reconciliação executada")
}

func (r *Resolver) handleDeadLetters(w http.ResponseWriter, req *http.Request) {
	cur, err := r.svc.store.deadLetters.Find(req.Context(), bson.D{},
		options.Find().SetSort(bson.D{{Key: "_id", Value: -1}}).SetLimit(100))
	var out []DeadLetter
	if err == nil {
		err = cur.All(req.Context(), &out)
	}
	if err != nil {
		httpx.WriteError(w, http.StatusInternalServerError, "INTERNAL", err.Error())
		return
	}
	if out == nil {
		out = []DeadLetter{}
	}
	httpx.WriteJSON(w, http.StatusOK, out)
}
