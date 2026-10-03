package dict

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"time"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/brenokcunha/light-pix/internal/httpx"
	"github.com/brenokcunha/light-pix/internal/pix"
	"github.com/brenokcunha/light-pix/internal/store"
)

// Estados do relato de infração (MED).
const (
	InfractionOpen        = "OPEN"        // aberto pelo PSP do pagador, aguardando a análise do recebedor
	InfractionAgreed      = "AGREED"      // o recebedor concordou (fraude confirmada)
	InfractionDisapproved = "DISAPPROVED" // o recebedor discordou
	InfractionCancelled   = "CANCELLED"   // o PSP do pagador cancelou antes da análise
)

// Motivos do relato.
const (
	ReasonFraud = "FRAUD"
	ReasonScam  = "SCAM"
)

// DefaultAnalysisDeadline é o prazo do recebedor para analisar um relato.
const DefaultAnalysisDeadline = 72 * time.Hour

// InfractionReport é um relato de infração: o PSP do pagador avisa que um Pix
// pode ser fraude e o PSP recebedor analisa e, concordando, devolve o valor
// (MED, Mecanismo Especial de Devolução).
type InfractionReport struct {
	ID               string     `bson:"_id" json:"id"`
	EndToEndID       string     `bson:"endToEndId" json:"endToEndId"`
	Reason           string     `bson:"reason" json:"reason"`
	Details          string     `bson:"details,omitempty" json:"details,omitempty"`
	ReporterISPB     string     `bson:"reporterIspb" json:"reporterIspb"`         // PSP do pagador
	CounterpartyISPB string     `bson:"counterpartyIspb" json:"counterpartyIspb"` // PSP recebedor
	Amount           pix.Amount `bson:"amount" json:"amount"`
	State            string     `bson:"state" json:"state"`
	CreatedAt        time.Time  `bson:"createdAt" json:"createdAt"`
	UpdatedAt        time.Time  `bson:"updatedAt" json:"updatedAt"`
	AnalysisDeadline time.Time  `bson:"analysisDeadline" json:"analysisDeadline"`
	Analysis         *Analysis  `bson:"analysis,omitempty" json:"analysis,omitempty"`
	// ActiveKey garante um relato ativo por Pix (índice único parcial); sai ao cancelar.
	ActiveKey string `bson:"activeKey,omitempty" json:"-"`
	// Overdue é calculado na leitura: aberto além do prazo de análise.
	Overdue bool `bson:"-" json:"overdue"`
}

// Analysis é a decisão do recebedor.
type Analysis struct {
	Result         string     `bson:"result" json:"result"` // AGREED | DISAPPROVED
	Details        string     `bson:"details,omitempty" json:"details,omitempty"`
	ReturnID       string     `bson:"returnId,omitempty" json:"returnId,omitempty"` // devolução especial gerada
	ReturnedAmount pix.Amount `bson:"returnedAmount,omitempty" json:"returnedAmount,omitempty"`
	DecidedAt      time.Time  `bson:"decidedAt" json:"decidedAt"`
}

// InfractionRequest é o pedido de abertura de um relato.
type InfractionRequest struct {
	EndToEndID       string     `json:"endToEndId"`
	Reason           string     `json:"reason"`
	Details          string     `json:"details"`
	CounterpartyISPB string     `json:"counterpartyIspb"`
	Amount           pix.Amount `json:"amount"`
}

func (s *Server) initMED(ctx context.Context, db *mongo.Database, deadline time.Duration) error {
	s.reports = db.Collection("infraction_reports")
	s.analysisDeadline = deadline
	if s.analysisDeadline == 0 {
		s.analysisDeadline = DefaultAnalysisDeadline
	}
	if _, err := s.reports.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "activeKey", Value: 1}}, Options: options.Index().SetUnique(true).
			SetPartialFilterExpression(bson.D{{Key: "activeKey", Value: bson.D{{Key: "$exists", Value: true}}}})},
		{Keys: bson.D{{Key: "counterpartyIspb", Value: 1}, {Key: "state", Value: 1}, {Key: "createdAt", Value: 1}}},
		{Keys: bson.D{{Key: "reporterIspb", Value: 1}, {Key: "state", Value: 1}, {Key: "createdAt", Value: 1}}},
	}); err != nil {
		return fmt.Errorf("dict: índices dos relatos: %w", err)
	}
	var err error
	s.infractions, err = otel.Meter("github.com/brenokcunha/light-pix/internal/sim/dict").Int64Counter("dict_sim.infraction_reports",
		metric.WithDescription("Eventos dos relatos de infração (MED), por evento"))
	return err
}

func (s *Server) routesMED(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/infraction-reports", s.handleCreateInfraction)
	mux.HandleFunc("GET /api/v1/infraction-reports", s.handleListInfractions)
	mux.HandleFunc("GET /api/v1/infraction-reports/{id}", s.handleGetInfraction)
	mux.HandleFunc("POST /api/v1/infraction-reports/{id}/analysis", s.handleAnalyzeInfraction)
	mux.HandleFunc("POST /api/v1/infraction-reports/{id}/cancel", s.handleCancelInfraction)
}

func (s *Server) countInfraction(ctx context.Context, event string) {
	s.infractions.Add(ctx, 1, metric.WithAttributes(attribute.String("event", event)))
}

func (s *Server) view(r InfractionReport) InfractionReport {
	r.Overdue = r.State == InfractionOpen && time.Now().After(r.AnalysisDeadline)
	return r
}

// handleCreateInfraction abre o relato. Só o PSP do pagador relata, e só Pix que
// ele iniciou (o EndToEndId leva o ISPB do pagador).
func (s *Server) handleCreateInfraction(w http.ResponseWriter, r *http.Request) {
	reporter := r.Header.Get(pix.HeaderRequestingParticipant)
	var req InfractionRequest
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, "INVALID_REQUEST", err.Error())
		return
	}
	payer, err := pix.ParseEndToEndID(req.EndToEndID)
	switch {
	case !pix.ValidISPB(reporter):
		httpx.WriteError(w, http.StatusBadRequest, "INVALID_REQUEST", "header "+pix.HeaderRequestingParticipant+" obrigatório")
		return
	case err != nil:
		httpx.WriteError(w, http.StatusBadRequest, "INVALID_REQUEST", err.Error())
		return
	case payer != reporter:
		httpx.WriteError(w, http.StatusForbidden, "FORBIDDEN", "só o PSP do pagador relata a infração")
		return
	case !pix.ValidISPB(req.CounterpartyISPB) || req.CounterpartyISPB == reporter:
		httpx.WriteError(w, http.StatusBadRequest, "INVALID_REQUEST", "counterpartyIspb deve ser o ISPB do PSP recebedor")
		return
	case req.Amount <= 0:
		httpx.WriteError(w, http.StatusBadRequest, "INVALID_REQUEST", "amount deve ser positivo")
		return
	case !slices.Contains([]string{ReasonFraud, ReasonScam}, req.Reason):
		httpx.WriteError(w, http.StatusBadRequest, "INVALID_REQUEST", "reason deve ser FRAUD ou SCAM")
		return
	}

	now := store.Now()
	rep := InfractionReport{
		ID: uuid.NewString(), EndToEndID: req.EndToEndID, Reason: req.Reason, Details: req.Details,
		ReporterISPB: reporter, CounterpartyISPB: req.CounterpartyISPB, Amount: req.Amount,
		State: InfractionOpen, CreatedAt: now, UpdatedAt: now, AnalysisDeadline: now.Add(s.analysisDeadline),
		ActiveKey: req.EndToEndID,
	}
	if _, err := s.reports.InsertOne(r.Context(), rep); err != nil {
		if mongo.IsDuplicateKeyError(err) {
			httpx.WriteError(w, http.StatusConflict, "INFRACTION_ALREADY_REPORTED", "já existe um relato ativo para este Pix")
			return
		}
		httpx.WriteError(w, http.StatusInternalServerError, "INTERNAL", err.Error())
		return
	}
	s.countInfraction(r.Context(), "created")
	httpx.WriteJSON(w, http.StatusCreated, s.view(rep))
}

func (s *Server) find(ctx context.Context, id string) (InfractionReport, error) {
	var rep InfractionReport
	err := s.reports.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&rep)
	return rep, err
}

// handleGetInfraction devolve o relato a quem participa dele; para os outros ele não existe.
func (s *Server) handleGetInfraction(w http.ResponseWriter, r *http.Request) {
	who := r.Header.Get(pix.HeaderRequestingParticipant)
	rep, err := s.find(r.Context(), r.PathValue("id"))
	switch {
	case errors.Is(err, mongo.ErrNoDocuments) || (err == nil && who != rep.ReporterISPB && who != rep.CounterpartyISPB):
		httpx.WriteError(w, http.StatusNotFound, "INFRACTION_NOT_FOUND", "relato não encontrado")
	case err != nil:
		httpx.WriteError(w, http.StatusInternalServerError, "INTERNAL", err.Error())
	default:
		httpx.WriteJSON(w, http.StatusOK, s.view(rep))
	}
}

// handleListInfractions lista os relatos do participante: role=counterparty (os
// que ele precisa analisar, padrão) ou role=reporter (os que ele abriu).
func (s *Server) handleListInfractions(w http.ResponseWriter, r *http.Request) {
	who := r.Header.Get(pix.HeaderRequestingParticipant)
	q := r.URL.Query()
	field := "counterpartyIspb"
	if q.Get("role") == "reporter" {
		field = "reporterIspb"
	}
	filter := bson.D{{Key: field, Value: who}}
	if st := q.Get("state"); st != "" {
		filter = append(filter, bson.E{Key: "state", Value: st})
	}
	cur, err := s.reports.Find(r.Context(), filter, options.Find().SetSort(bson.D{{Key: "createdAt", Value: 1}}).SetLimit(200))
	var list []InfractionReport
	if err == nil {
		err = cur.All(r.Context(), &list)
	}
	if err != nil {
		httpx.WriteError(w, http.StatusInternalServerError, "INTERNAL", err.Error())
		return
	}
	out := make([]InfractionReport, len(list))
	for i, rep := range list {
		out[i] = s.view(rep)
	}
	httpx.WriteJSON(w, http.StatusOK, out)
}

// AnalysisRequest é a decisão do recebedor sobre um relato.
type AnalysisRequest struct {
	Result         string     `json:"result"`
	Details        string     `json:"details"`
	ReturnID       string     `json:"returnId"`
	ReturnedAmount pix.Amount `json:"returnedAmount"`
}

// handleAnalyzeInfraction registra a decisão do PSP recebedor, uma única vez.
func (s *Server) handleAnalyzeInfraction(w http.ResponseWriter, r *http.Request) {
	who := r.Header.Get(pix.HeaderRequestingParticipant)
	var req AnalysisRequest
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, "INVALID_REQUEST", err.Error())
		return
	}
	if req.Result != InfractionAgreed && req.Result != InfractionDisapproved {
		httpx.WriteError(w, http.StatusBadRequest, "INVALID_REQUEST", "result deve ser AGREED ou DISAPPROVED")
		return
	}
	if req.Result == InfractionDisapproved && (req.ReturnID != "" || req.ReturnedAmount != 0) {
		httpx.WriteError(w, http.StatusBadRequest, "INVALID_REQUEST", "relato recusado não tem devolução")
		return
	}
	if req.ReturnID != "" {
		if _, err := pix.ParseReturnID(req.ReturnID); err != nil {
			httpx.WriteError(w, http.StatusBadRequest, "INVALID_REQUEST", err.Error())
			return
		}
	}
	s.decide(w, r, who, "counterpartyIspb", bson.D{
		{Key: "state", Value: req.Result},
		{Key: "analysis", Value: Analysis{Result: req.Result, Details: req.Details, ReturnID: req.ReturnID,
			ReturnedAmount: req.ReturnedAmount, DecidedAt: store.Now()}},
	}, false, "analyzed")
}

// handleCancelInfraction cancela o relato, só pelo PSP do pagador e só antes da análise.
func (s *Server) handleCancelInfraction(w http.ResponseWriter, r *http.Request) {
	s.decide(w, r, r.Header.Get(pix.HeaderRequestingParticipant), "reporterIspb",
		bson.D{{Key: "state", Value: InfractionCancelled}}, true, "cancelled")
}

// decide muda um relato OPEN, condicionado ao estado e ao papel de quem pede:
// uma decisão por relato, e nunca por quem não é parte dele.
func (s *Server) decide(w http.ResponseWriter, r *http.Request, who, roleField string, set bson.D, dropKey bool, event string) {
	id := r.PathValue("id")
	set = append(set, bson.E{Key: "updatedAt", Value: store.Now()})
	update := bson.D{{Key: "$set", Value: set}}
	if dropKey {
		update = append(update, bson.E{Key: "$unset", Value: bson.D{{Key: "activeKey", Value: ""}}})
	}
	var rep InfractionReport
	err := s.reports.FindOneAndUpdate(r.Context(),
		bson.D{{Key: "_id", Value: id}, {Key: roleField, Value: who}, {Key: "state", Value: InfractionOpen}},
		update, options.FindOneAndUpdate().SetReturnDocument(options.After)).Decode(&rep)
	if errors.Is(err, mongo.ErrNoDocuments) {
		// Distingue "não é seu / não existe" de "já foi decidido".
		cur, ferr := s.find(r.Context(), id)
		switch {
		case ferr != nil || (cur.ReporterISPB != who && cur.CounterpartyISPB != who):
			httpx.WriteError(w, http.StatusNotFound, "INFRACTION_NOT_FOUND", "relato não encontrado")
		case cur.State != InfractionOpen:
			httpx.WriteError(w, http.StatusConflict, "INFRACTION_NOT_OPEN", "o relato já está "+cur.State)
		default:
			httpx.WriteError(w, http.StatusForbidden, "FORBIDDEN", "esta ação cabe à outra parte do relato")
		}
		return
	}
	if err != nil {
		httpx.WriteError(w, http.StatusInternalServerError, "INTERNAL", err.Error())
		return
	}
	s.countInfraction(r.Context(), event)
	httpx.WriteJSON(w, http.StatusOK, s.view(rep))
}
