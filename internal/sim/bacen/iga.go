package bacen

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"slices"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/brenokcunha/light-pix/internal/httpx"
	"github.com/brenokcunha/light-pix/internal/pix"
)

// Metas do IGA (simplificadas). Servem de referência no relatório e nos SLOs.
const (
	TargetPayerP50   = 6 * time.Second        // experiência do pagador, mediana
	TargetPayerP99   = 10 * time.Second       // experiência do pagador, P99
	TargetInitP50    = 900 * time.Millisecond // iniciação, mediana
	TargetInitP95    = 1500 * time.Millisecond
	TargetSettlement = 40 * time.Second // prazo de liquidação no SPI
)

// Papéis de quem reporta.
const (
	RolePayer    = "PAYER"
	RoleCreditor = "CREDITOR"
)

// IGASPIRecord são os instantes que o SPI mede de uma transação encerrada.
type IGASPIRecord struct {
	EndToEndID   string     `json:"endToEndId" bson:"endToEndId"`
	DebtorISPB   string     `json:"debtorIspb" bson:"debtorIspb"`
	CreditorISPB string     `json:"creditorIspb" bson:"creditorIspb"`
	Status       string     `json:"status" bson:"status"` // SETTLED | REJECTED
	Reason       string     `json:"reason,omitempty" bson:"reason,omitempty"`
	ReceivedAt   time.Time  `json:"receivedAt" bson:"receivedAt"`           // pacs.008 chegou ao SPI
	AcceptedAt   *time.Time `json:"acceptedAt,omitempty" bson:"acceptedAt"` // recebedor respondeu ACSP
	SettledAt    *time.Time `json:"settledAt,omitempty" bson:"settledAt"`   // liquidação na conta PI
	FinishedAt   time.Time  `json:"finishedAt" bson:"finishedAt"`           // resultado final no SPI
}

// IGAPSPRecord são os instantes que um PSP mede de um Pix encerrado.
type IGAPSPRecord struct {
	EndToEndID string `json:"endToEndId" bson:"endToEndId"`
	ISPB       string `json:"ispb" bson:"ispb"`
	Role       string `json:"role" bson:"role"` // PAYER | CREDITOR
	// T0: PAYER, o pedido do cliente foi aceito; CREDITOR, a pacs.008 chegou.
	T0 time.Time `json:"t0" bson:"t0"`
	// T1: CREDITOR, o ACSP foi enviado (opcional).
	T1 *time.Time `json:"t1,omitempty" bson:"t1"`
	// T6a: o PSP chegou ao estado final (PAYER: o pagador foi informado).
	T6a    time.Time `json:"t6a" bson:"t6a"`
	State  string    `json:"state" bson:"state"`
	Reason string    `json:"reason,omitempty" bson:"reason,omitempty"`
}

type igaRecord struct {
	ID           string        `bson:"_id"` // EndToEndId
	DebtorISPB   string        `bson:"debtorIspb,omitempty"`
	CreditorISPB string        `bson:"creditorIspb,omitempty"`
	SPI          *IGASPIRecord `bson:"spi,omitempty"`
	Payer        *IGAPSPRecord `bson:"payer,omitempty"`
	Creditor     *IGAPSPRecord `bson:"creditor,omitempty"`
	// Recorded marca os indicadores já medidos nas métricas: cada um entra
	// uma única vez, mesmo que os relatórios cheguem repetidos ou fora de ordem.
	Recorded  map[string]bool `bson:"recorded,omitempty"`
	UpdatedAt time.Time       `bson:"updatedAt"`
}

type igaMetrics struct {
	payerExperience  metric.Float64Histogram
	initiation       metric.Float64Histogram
	settlement       metric.Float64Histogram
	creditorResponse metric.Float64Histogram
	transactions     metric.Int64Counter
}

func (s *Server) initIGA(ctx context.Context, db *mongo.Database) error {
	s.iga = db.Collection("iga_records")
	if _, err := s.iga.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "payer.t0", Value: 1}}},
		{Keys: bson.D{{Key: "debtorIspb", Value: 1}, {Key: "payer.t0", Value: 1}}},
		// Os registros servem à consulta por janela; uma semana basta.
		{Keys: bson.D{{Key: "updatedAt", Value: 1}}, Options: options.Index().SetExpireAfterSeconds(7 * 24 * 3600)},
	}); err != nil {
		return fmt.Errorf("bacen: índices do IGA: %w", err)
	}
	meter := otel.Meter("github.com/brenokcunha/light-pix/internal/sim/bacen")
	var err error
	hist := func(name, desc string) metric.Float64Histogram {
		h, e := meter.Float64Histogram(name, metric.WithUnit("s"), metric.WithDescription(desc))
		err = errors.Join(err, e)
		return h
	}
	s.igaM.payerExperience = hist("iga.payer_experience.duration", "Do pedido do pagador (t0) ao estado final no PSP (t6a), por PSP pagador")
	s.igaM.initiation = hist("iga.initiation.duration", "Do pedido do pagador (t0) à chegada da pacs.008 ao SPI, por PSP pagador")
	s.igaM.settlement = hist("iga.settlement.duration", "Da chegada da pacs.008 ao SPI até a liquidação, por PSP pagador")
	s.igaM.creditorResponse = hist("iga.creditor_response.duration", "Da chegada da pacs.008 ao SPI até o ACSP do recebedor, por PSP recebedor")
	if err != nil {
		return err
	}
	s.igaM.transactions, err = meter.Int64Counter("iga.transactions",
		metric.WithDescription("Transações vistas pelo SPI, por PSP pagador e desfecho"))
	return err
}

// ingestSPI grava os instantes do SPI e mede o que já dá para medir.
func (s *Server) ingestSPI(ctx context.Context, recs []IGASPIRecord) error {
	for _, r := range recs {
		r := r
		if err := s.upsertIGA(ctx, r.EndToEndID, bson.D{
			{Key: "spi", Value: r}, {Key: "debtorIspb", Value: r.DebtorISPB}, {Key: "creditorIspb", Value: r.CreditorISPB},
		}); err != nil {
			return err
		}
	}
	return nil
}

// ingestPSP grava os instantes de um PSP e mede o que já dá para medir.
func (s *Server) ingestPSP(ctx context.Context, recs []IGAPSPRecord) error {
	for _, r := range recs {
		field := "payer"
		if r.Role == RoleCreditor {
			field = "creditor"
		}
		if err := s.upsertIGA(ctx, r.EndToEndID, bson.D{{Key: field, Value: r}}); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) upsertIGA(ctx context.Context, e2e string, set bson.D) error {
	set = append(set, bson.E{Key: "updatedAt", Value: time.Now().UTC()})
	var rec igaRecord
	err := s.iga.FindOneAndUpdate(ctx, bson.D{{Key: "_id", Value: e2e}}, bson.D{{Key: "$set", Value: set}},
		options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)).Decode(&rec)
	if err != nil {
		return err
	}
	for name, ind := range s.indicators(rec) {
		// Só mede se este processo foi quem marcou o indicador.
		res, err := s.iga.UpdateOne(ctx,
			bson.D{{Key: "_id", Value: e2e}, {Key: "recorded." + name, Value: bson.D{{Key: "$ne", Value: true}}}},
			bson.D{{Key: "$set", Value: bson.D{{Key: "recorded." + name, Value: true}}}})
		if err != nil {
			return err
		}
		if res.ModifiedCount == 1 {
			ind.record(ctx)
		}
	}
	return nil
}

type indicator struct {
	record func(ctx context.Context)
}

// indicators devolve os indicadores já mensuráveis do registro e ainda não
// medidos. Um negativo (relógios dessincronizados) é descartado.
func (s *Server) indicators(r igaRecord) map[string]indicator {
	out := map[string]indicator{}
	add := func(name string, h metric.Float64Histogram, ispb string, from, to time.Time) {
		if r.Recorded[name] || to.Before(from) {
			return
		}
		d, attrs := to.Sub(from), metric.WithAttributes(attribute.String("ispb", ispb))
		out[name] = indicator{func(ctx context.Context) { h.Record(ctx, d.Seconds(), attrs) }}
	}
	if r.Payer != nil {
		add("payer_experience", s.igaM.payerExperience, r.Payer.ISPB, r.Payer.T0, r.Payer.T6a)
	}
	if r.Payer != nil && r.SPI != nil {
		add("initiation", s.igaM.initiation, r.Payer.ISPB, r.Payer.T0, r.SPI.ReceivedAt)
	}
	if r.SPI != nil {
		if r.SPI.SettledAt != nil {
			add("settlement", s.igaM.settlement, r.SPI.DebtorISPB, r.SPI.ReceivedAt, *r.SPI.SettledAt)
		}
		if r.SPI.AcceptedAt != nil {
			add("creditor_response", s.igaM.creditorResponse, r.SPI.CreditorISPB, r.SPI.ReceivedAt, *r.SPI.AcceptedAt)
		}
	}
	if r.SPI != nil && !r.Recorded["transaction"] {
		attrs := metric.WithAttributes(attribute.String("ispb", r.SPI.DebtorISPB),
			attribute.String("status", r.SPI.Status), attribute.String("reason", r.SPI.Reason))
		out["transaction"] = indicator{func(ctx context.Context) { s.igaM.transactions.Add(ctx, 1, attrs) }}
	}
	return out
}

// Quantiles resume uma distribuição de tempos, em segundos.
type Quantiles struct {
	Count int     `json:"count"`
	P50   float64 `json:"p50"`
	P95   float64 `json:"p95"`
	P99   float64 `json:"p99"`
}

// IGAReport é a visão do regulador sobre um PSP numa janela.
type IGAReport struct {
	ISPB   string `json:"ispb"`
	Window string `json:"window"`
	// Transactions conta as transações do PSP pagador vistas pelo SPI.
	Transactions int `json:"transactions"`
	Settled      int `json:"settled"`
	// Rejected por motivo (AB03, AM04...).
	Rejected map[string]int `json:"rejected"`
	// PayerExperience e Initiation vêm dos relatórios do PSP pagador;
	// Settlement e CreditorResponse, do SPI.
	PayerExperience  Quantiles `json:"payerExperience"`
	Initiation       Quantiles `json:"initiation"`
	Settlement       Quantiles `json:"settlement"`
	CreditorResponse Quantiles `json:"creditorResponse"`
	Targets          struct {
		PayerP50, PayerP99, InitiationP50, InitiationP95, Settlement float64
	} `json:"targets"`
	// MeetsTargets diz se cada meta está sendo cumprida (nil sem dados).
	MeetsTargets map[string]*bool `json:"meetsTargets"`
}

// Report calcula os indicadores de ispb sobre as transações cujo pedido (t0)
// ou chegada ao SPI cai em [now-window, now].
func (s *Server) Report(ctx context.Context, ispb string, window time.Duration) (IGAReport, error) {
	since := time.Now().UTC().Add(-window)
	cur, err := s.iga.Find(ctx, bson.D{
		{Key: "updatedAt", Value: bson.D{{Key: "$gte", Value: since}}},
		{Key: "$or", Value: bson.A{
			bson.D{{Key: "debtorIspb", Value: ispb}}, bson.D{{Key: "creditorIspb", Value: ispb}},
			bson.D{{Key: "payer.ispb", Value: ispb}}, bson.D{{Key: "creditor.ispb", Value: ispb}},
		}},
	}, options.Find().SetLimit(100_000))
	if err != nil {
		return IGAReport{}, err
	}
	var recs []igaRecord
	if err := cur.All(ctx, &recs); err != nil {
		return IGAReport{}, err
	}

	rep := IGAReport{ISPB: ispb, Window: window.String(), Rejected: map[string]int{}}
	var payer, init, settle, cred []float64
	secs := func(a, b time.Time) (float64, bool) { return b.Sub(a).Seconds(), !b.Before(a) }
	for _, r := range recs {
		if r.SPI != nil && r.SPI.DebtorISPB == ispb {
			rep.Transactions++
			if r.SPI.Status == "SETTLED" {
				rep.Settled++
			} else {
				rep.Rejected[r.SPI.Reason]++
			}
			if r.SPI.SettledAt != nil {
				if v, ok := secs(r.SPI.ReceivedAt, *r.SPI.SettledAt); ok {
					settle = append(settle, v)
				}
			}
		}
		if r.Payer != nil && r.Payer.ISPB == ispb {
			if v, ok := secs(r.Payer.T0, r.Payer.T6a); ok {
				payer = append(payer, v)
			}
			if r.SPI != nil {
				if v, ok := secs(r.Payer.T0, r.SPI.ReceivedAt); ok {
					init = append(init, v)
				}
			}
		}
		if r.SPI != nil && r.SPI.CreditorISPB == ispb && r.SPI.AcceptedAt != nil {
			if v, ok := secs(r.SPI.ReceivedAt, *r.SPI.AcceptedAt); ok {
				cred = append(cred, v)
			}
		}
	}
	rep.PayerExperience, rep.Initiation = quantiles(payer), quantiles(init)
	rep.Settlement, rep.CreditorResponse = quantiles(settle), quantiles(cred)
	rep.Targets.PayerP50, rep.Targets.PayerP99 = TargetPayerP50.Seconds(), TargetPayerP99.Seconds()
	rep.Targets.InitiationP50, rep.Targets.InitiationP95 = TargetInitP50.Seconds(), TargetInitP95.Seconds()
	rep.Targets.Settlement = TargetSettlement.Seconds()
	meets := func(q Quantiles, ok bool) *bool {
		if q.Count == 0 {
			return nil
		}
		return &ok
	}
	rep.MeetsTargets = map[string]*bool{
		"payerExperience": meets(rep.PayerExperience, rep.PayerExperience.P50 <= rep.Targets.PayerP50 && rep.PayerExperience.P99 <= rep.Targets.PayerP99),
		"initiation":      meets(rep.Initiation, rep.Initiation.P50 <= rep.Targets.InitiationP50 && rep.Initiation.P95 <= rep.Targets.InitiationP95),
		"settlement":      meets(rep.Settlement, rep.Settlement.P99 <= rep.Targets.Settlement),
	}
	return rep, nil
}

func quantiles(v []float64) Quantiles {
	if len(v) == 0 {
		return Quantiles{}
	}
	slices.Sort(v)
	at := func(p float64) float64 {
		// Percentil por posição mais próxima (nearest-rank).
		i := int(math.Ceil(p*float64(len(v)))) - 1
		return math.Round(v[max(i, 0)]*1000) / 1000
	}
	return Quantiles{Count: len(v), P50: at(0.50), P95: at(0.95), P99: at(0.99)}
}

type igaBatch[T any] struct {
	Records []T `json:"records"`
}

func (s *Server) handleIGASPI(w http.ResponseWriter, r *http.Request) {
	var b igaBatch[IGASPIRecord]
	if err := httpx.DecodeJSON(w, r, &b); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, CodeInvalidRequest, err.Error())
		return
	}
	for _, rec := range b.Records {
		if _, err := pix.ParseEndToEndID(rec.EndToEndID); err != nil || !pix.ValidISPB(rec.DebtorISPB) || !pix.ValidISPB(rec.CreditorISPB) ||
			rec.ReceivedAt.IsZero() || rec.FinishedAt.IsZero() || (rec.Status != "SETTLED" && rec.Status != "REJECTED") {
			httpx.WriteError(w, http.StatusBadRequest, CodeInvalidRequest, "registro do SPI inválido: "+rec.EndToEndID)
			return
		}
	}
	if err := s.ingestSPI(r.Context(), b.Records); err != nil {
		httpx.WriteError(w, http.StatusInternalServerError, "INTERNAL", err.Error())
		return
	}
	httpx.WriteJSON(w, http.StatusAccepted, map[string]int{"accepted": len(b.Records)})
}

func (s *Server) handleIGAReports(w http.ResponseWriter, r *http.Request) {
	requester := r.Header.Get(pix.HeaderRequestingParticipant)
	var b igaBatch[IGAPSPRecord]
	if err := httpx.DecodeJSON(w, r, &b); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, CodeInvalidRequest, err.Error())
		return
	}
	for _, rec := range b.Records {
		switch {
		case rec.ISPB != requester:
			httpx.WriteError(w, http.StatusForbidden, "FORBIDDEN", "o PSP só reporta as próprias transações")
			return
		case rec.Role != RolePayer && rec.Role != RoleCreditor, rec.T0.IsZero(), rec.T6a.IsZero():
			httpx.WriteError(w, http.StatusBadRequest, CodeInvalidRequest, "registro inválido: "+rec.EndToEndID)
			return
		}
		if _, err := pix.ParseEndToEndID(rec.EndToEndID); err != nil {
			httpx.WriteError(w, http.StatusBadRequest, CodeInvalidRequest, err.Error())
			return
		}
	}
	if err := s.ingestPSP(r.Context(), b.Records); err != nil {
		httpx.WriteError(w, http.StatusInternalServerError, "INTERNAL", err.Error())
		return
	}
	httpx.WriteJSON(w, http.StatusAccepted, map[string]int{"accepted": len(b.Records)})
}

func (s *Server) handleIGA(w http.ResponseWriter, r *http.Request) {
	window := time.Hour
	if v := strings.TrimSpace(r.URL.Query().Get("window")); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 || d > 7*24*time.Hour {
			httpx.WriteError(w, http.StatusBadRequest, CodeInvalidRequest, "window deve ser uma duração entre 1s e 168h")
			return
		}
		window = d
	}
	rep, err := s.Report(r.Context(), r.PathValue("ispb"), window)
	if err != nil {
		httpx.WriteError(w, http.StatusInternalServerError, "INTERNAL", err.Error())
		return
	}
	httpx.WriteJSON(w, http.StatusOK, rep)
}
