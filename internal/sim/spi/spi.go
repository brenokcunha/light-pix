// Package spi simula o Sistema de Pagamentos Instantâneos: recebe pacs.008 e
// pacs.002 dos participantes, roteia para o destinatário, liquida contra as
// contas PI (bacen-sim) e expõe a consulta de status. A interface é inspirada
// no ICOM: POST para enviar e long-polling para receber.
package spi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/brenokcunha/light-pix/internal/faults"
	"github.com/brenokcunha/light-pix/internal/httpx"
	"github.com/brenokcunha/light-pix/internal/iso20022"
	"github.com/brenokcunha/light-pix/internal/pix"
	"github.com/brenokcunha/light-pix/internal/sim/bacen"
	"github.com/brenokcunha/light-pix/internal/store"
)

// Estados da transação no SPI.
const (
	StatusAwaitingCreditor = "AWAITING_CREDITOR" // pacs.008 entregue ao recebedor
	StatusSettling         = "SETTLING"          // aceita (ACSP), liquidação em curso
	StatusSettled          = "SETTLED"
	StatusRejected         = "REJECTED"
)

// Settler liquida a transação nas contas PI (bacen-sim).
type Settler interface {
	Settle(ctx context.Context, req bacen.SettlementRequest) (bacen.SettlementResult, error)
}

// IGAReporter entrega os instantes medidos pelo SPI (o bacen-sim, no IGA).
type IGAReporter interface {
	ReportSPI(ctx context.Context, recs []bacen.IGASPIRecord) error
}

// Config parametriza o simulador.
type Config struct {
	Participants    []string      // ISPBs habilitados
	CreditorTimeout time.Duration // prazo do recebedor para responder a pacs.008
	PollWait        time.Duration // espera máxima de um long-polling sem mensagens
	Lease           time.Duration // tempo até um lote não confirmado ser reentregue
	SweepInterval   time.Duration // varredura de prazos vencidos e liquidações pendentes
	SettleRetry     time.Duration // idade mínima de uma liquidação parada para nova tentativa
	Settler         Settler
	ReturnWindow    time.Duration // prazo para devolver um Pix liquidado (90 dias)
	// IGA, se definido, recebe os instantes de cada transação encerrada, em
	// lotes e sem bloquear o fluxo: é um canal de medição, não de negócio.
	IGA IGAReporter
	// Faults, se definido, injeta falhas do SPI: pacs.002 perdidas e
	// recebedor que não responde.
	Faults *faults.State
}

// Transaction é o registro da transação no SPI.
type Transaction struct {
	EndToEndId   string     `bson:"_id"`
	MsgId        string     `bson:"msgId"` // MsgId da pacs.008 original
	DebtorISPB   string     `bson:"debtorIspb"`
	CreditorISPB string     `bson:"creditorIspb"`
	Amount       pix.Amount `bson:"amount"`
	// Kind é vazio num Pix e "RETURN" numa devolução (então o _id é o "D..."
	// e OriginalEndToEndID aponta para o Pix devolvido).
	Kind               string `bson:"kind,omitempty"`
	OriginalEndToEndID string `bson:"originalEndToEndId,omitempty"`
	// ReturnedAmount, num Pix, é o valor já devolvido ou em devolução.
	ReturnedAmount pix.Amount        `bson:"returnedAmount,omitempty"`
	Status         string            `bson:"status"`
	Reason         string            `bson:"reason,omitempty"`
	TraceContext   map[string]string `bson:"traceContext,omitempty"`
	ReceivedAt     time.Time         `bson:"receivedAt"`
	// NotForwarded marca a transação que o SPI não encaminhou ao recebedor
	// (falha injetada): no timeout, o recebedor não é avisado do que não viu.
	NotForwarded bool       `bson:"notForwarded,omitempty"`
	Deadline     time.Time  `bson:"deadline"`
	AcceptedAt   *time.Time `bson:"acceptedAt,omitempty"`
	SettledAt    *time.Time `bson:"settledAt,omitempty"`
	FinishedAt   *time.Time `bson:"finishedAt,omitempty"`
	UpdatedAt    time.Time  `bson:"updatedAt"`
}

// Server é o simulador do SPI.
type Server struct {
	cfg      Config
	client   *mongo.Client
	txs      *mongo.Collection
	mailbox  *mongo.Collection
	notifier *notifier
	tracer   trace.Tracer
	stopping chan struct{} // fechado quando Run termina: encerra os long-pollings
	igaQueue chan bacen.IGASPIRecord

	received  metric.Int64Counter
	completed metric.Int64Counter
	duration  metric.Float64Histogram
	// igaDropped conta registros do IGA descartados (fila cheia ou envio falho).
	igaDropped metric.Int64Counter
}

// New prepara coleções, índices e métricas.
func New(ctx context.Context, client *mongo.Client, dbName string, cfg Config) (*Server, error) {
	db := client.Database(dbName)
	s := &Server{
		cfg:      cfg,
		client:   client,
		txs:      db.Collection("transactions"),
		mailbox:  db.Collection("mailbox"),
		notifier: newNotifier(),
		stopping: make(chan struct{}),
		igaQueue: make(chan bacen.IGASPIRecord, igaQueueSize),
		tracer:   otel.Tracer("github.com/brenokcunha/light-pix/internal/sim/spi"),
	}
	if _, err := s.txs.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "status", Value: 1}, {Key: "deadline", Value: 1}}},
		{Keys: bson.D{{Key: "status", Value: 1}, {Key: "updatedAt", Value: 1}}},
	}); err != nil {
		return nil, fmt.Errorf("spi: índices de transactions: %w", err)
	}
	if _, err := s.mailbox.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "ispb", Value: 1}, {Key: "acked", Value: 1}, {Key: "leaseUntil", Value: 1}, {Key: "_id", Value: 1}}},
		{Keys: bson.D{{Key: "batch", Value: 1}}},
		// Mensagens confirmadas ficam um dia para depuração.
		{Keys: bson.D{{Key: "ackedAt", Value: 1}}, Options: options.Index().SetExpireAfterSeconds(86400)},
	}); err != nil {
		return nil, fmt.Errorf("spi: índices de mailbox: %w", err)
	}
	return s, s.initMetrics()
}

func (s *Server) initMetrics() error {
	meter := otel.Meter("github.com/brenokcunha/light-pix/internal/sim/spi")
	var err error
	if s.received, err = meter.Int64Counter("spi_sim.messages.received",
		metric.WithDescription("Mensagens recebidas dos participantes, por tipo e resultado")); err != nil {
		return err
	}
	if s.completed, err = meter.Int64Counter("spi_sim.transactions.completed",
		metric.WithDescription("Transações finalizadas, por estado e motivo")); err != nil {
		return err
	}
	if s.duration, err = meter.Float64Histogram("spi_sim.transaction.duration",
		metric.WithDescription("Da chegada da pacs.008 à liquidação ou rejeição"),
		metric.WithUnit("s")); err != nil {
		return err
	}
	if s.igaDropped, err = meter.Int64Counter("spi_sim.iga.dropped",
		metric.WithDescription("Registros do IGA descartados (fila cheia ou envio ao bacen-sim falho)")); err != nil {
		return err
	}
	_, err = meter.Int64ObservableGauge("spi_sim.mailbox.pending",
		metric.WithDescription("Mensagens aguardando entrega/confirmação por participante"),
		metric.WithInt64Callback(s.observePending))
	return err
}

// Routes registra a API.
func (s *Server) Routes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/in/{ispb}/msgs", s.handleInbound)
	mux.HandleFunc("GET /api/v1/out/{ispb}/stream", s.handlePull)
	mux.HandleFunc("GET /api/v1/status/{endToEndId}", s.handleStatus)
}

// apiError carrega status e código HTTP até o handler.
type apiError struct {
	status int
	code   string
	msg    string
}

func (e *apiError) Error() string { return e.msg }

func newAPIError(status int, code, format string, args ...any) error {
	return &apiError{status: status, code: code, msg: fmt.Sprintf(format, args...)}
}

func (s *Server) participant(ispb string) bool { return slices.Contains(s.cfg.Participants, ispb) }

func (s *Server) handleInbound(w http.ResponseWriter, r *http.Request) {
	ispb := r.PathValue("ispb")
	var env iso20022.Envelope
	err := httpx.DecodeJSON(w, r, &env)
	if err == nil {
		err = s.receive(r.Context(), ispb, env)
	}
	result := "accepted"
	var apiErr *apiError
	switch {
	case errors.As(err, &apiErr):
		result = apiErr.code
		httpx.WriteError(w, apiErr.status, apiErr.code, apiErr.msg)
	case err != nil && env.AppHdr.MsgDefIdr == "":
		result = "invalid"
		httpx.WriteError(w, http.StatusBadRequest, "INVALID_MESSAGE", err.Error())
	case err != nil:
		result = "error"
		slog.ErrorContext(r.Context(), "falha ao processar mensagem", "msg_type", env.AppHdr.MsgDefIdr, "err", err)
		httpx.WriteError(w, http.StatusInternalServerError, "INTERNAL", "falha ao processar mensagem")
	default:
		w.WriteHeader(http.StatusAccepted)
	}
	s.received.Add(r.Context(), 1, metric.WithAttributes(
		attribute.String("msg_type", env.AppHdr.MsgDefIdr),
		attribute.String("result", result),
	))
}

func (s *Server) receive(ctx context.Context, ispb string, env iso20022.Envelope) error {
	if err := env.Validate(); err != nil {
		return newAPIError(http.StatusBadRequest, "INVALID_MESSAGE", "%v", err)
	}
	switch {
	case !s.participant(ispb) || env.AppHdr.Fr != ispb:
		return newAPIError(http.StatusForbidden, "FORBIDDEN", "remetente %s não habilitado ou diferente do caminho", env.AppHdr.Fr)
	case env.AppHdr.To != pix.BCBISPB:
		return newAPIError(http.StatusBadRequest, "INVALID_MESSAGE", "AppHdr.To deve ser o SPI (%s)", pix.BCBISPB)
	}
	trace.SpanFromContext(ctx).SetAttributes(attribute.String("pix.msg_type", env.AppHdr.MsgDefIdr))
	switch env.AppHdr.MsgDefIdr {
	case iso20022.MsgPacs008:
		return s.receivePacs008(ctx, env)
	case iso20022.MsgPacs004:
		return s.receivePacs004(ctx, env)
	default:
		return s.receivePacs002(ctx, env)
	}
}

// receivePacs008 registra a transação e a encaminha ao recebedor na mesma
// transação do MongoDB. O EndToEndId é a chave: reenviar a mesma mensagem é
// inofensivo; reutilizar o id com outra mensagem é rejeitado.
func (s *Server) receivePacs008(ctx context.Context, env iso20022.Envelope) error {
	doc := env.FIToFICstmrCdtTrf
	tx := doc.CdtTrfTxInf
	creditor := tx.CdtrAgt.MmbId
	trace.SpanFromContext(ctx).SetAttributes(attribute.String("pix.end_to_end_id", tx.PmtId.EndToEndId))
	switch {
	case tx.DbtrAgt.MmbId != env.AppHdr.Fr:
		return newAPIError(http.StatusBadRequest, "INVALID_MESSAGE", "DbtrAgt deve ser o remetente")
	case creditor == env.AppHdr.Fr:
		return newAPIError(http.StatusBadRequest, "INVALID_MESSAGE", "transação entre contas do mesmo participante não passa pelo SPI")
	case !s.participant(creditor):
		return newAPIError(http.StatusUnprocessableEntity, "UNKNOWN_PARTICIPANT", "recebedor %s não habilitado", creditor)
	}

	now := store.Now()
	tc := injectTrace(ctx)
	// Falha injetada: o SPI registra a transação mas não a encaminha, como se o
	// recebedor nunca respondesse; o prazo vence e o pagador recebe AB03.
	forward := !s.cfg.Faults.Roll(ctx, s.cfg.Faults.Get().CreditorTimeoutRate, "creditor_timeout")
	rec := Transaction{
		NotForwarded: !forward,
		EndToEndId:   tx.PmtId.EndToEndId,
		MsgId:        doc.GrpHdr.MsgId,
		DebtorISPB:   tx.DbtrAgt.MmbId,
		CreditorISPB: creditor,
		Amount:       tx.IntrBkSttlmAmt.Value,
		Status:       StatusAwaitingCreditor,
		TraceContext: tc,
		ReceivedAt:   now,
		Deadline:     now.Add(s.cfg.CreditorTimeout),
		UpdatedAt:    now,
	}
	// O SPI reemite a mensagem com o próprio cabeçalho; o documento segue intacto.
	fwd := env
	fwd.AppHdr = iso20022.AppHdr{Fr: pix.BCBISPB, To: creditor, BizMsgIdr: pix.NewMsgID(pix.BCBISPB),
		MsgDefIdr: iso20022.MsgPacs008, CreDt: now}

	err := store.Transact(ctx, s.client, func(ctx context.Context) error {
		if _, err := s.txs.InsertOne(ctx, rec); err != nil {
			return err
		}
		if !forward {
			return nil
		}
		return s.enqueue(ctx, creditor, fwd, tc, now)
	})
	if mongo.IsDuplicateKeyError(err) {
		var prev Transaction
		if err := s.txs.FindOne(ctx, bson.D{{Key: "_id", Value: rec.EndToEndId}}).Decode(&prev); err != nil {
			return err
		}
		if prev.MsgId == rec.MsgId {
			slog.InfoContext(ctx, "pacs.008 repetida ignorada", "end_to_end_id", rec.EndToEndId)
			return nil
		}
		return newAPIError(http.StatusConflict, "DUPLICATE_END_TO_END_ID", "EndToEndId já usado por outra mensagem")
	}
	if err != nil {
		return err
	}
	s.notifier.notify(creditor)
	slog.InfoContext(ctx, "pacs.008 encaminhada", "end_to_end_id", rec.EndToEndId,
		"debtor_ispb", rec.DebtorISPB, "creditor_ispb", creditor)
	return nil
}

// receivePacs002 trata a resposta do recebedor: RJCT encerra a transação;
// ACSP dispara a liquidação. Respostas repetidas ou tardias são aceitas e
// ignoradas, porque o estado só avança.
func (s *Server) receivePacs002(ctx context.Context, env iso20022.Envelope) error {
	st := env.FIToFIPmtStsRpt.TxInfAndSts
	trace.SpanFromContext(ctx).SetAttributes(
		attribute.String("pix.end_to_end_id", st.OrgnlEndToEndId),
		attribute.String("pix.tx_status", st.TxSts),
	)
	var tx Transaction
	err := s.txs.FindOne(ctx, bson.D{{Key: "_id", Value: st.OrgnlEndToEndId}}).Decode(&tx)
	switch {
	case errors.Is(err, mongo.ErrNoDocuments):
		return newAPIError(http.StatusNotFound, "UNKNOWN_END_TO_END_ID", "transação %s não encontrada", st.OrgnlEndToEndId)
	case err != nil:
		return err
	case tx.CreditorISPB != env.AppHdr.Fr:
		return newAPIError(http.StatusForbidden, "FORBIDDEN", "só o recebedor responde à pacs.008")
	}

	switch st.TxSts {
	case iso20022.StatusRejected:
		return s.reject(ctx, tx, StatusAwaitingCreditor, st.StsRsnInf.Rsn, false)
	case iso20022.StatusAccepted:
		now := store.Now()
		accepted := now
		if st.AccptncDtTm != nil {
			accepted = st.AccptncDtTm.UTC()
		}
		res, err := s.txs.UpdateOne(ctx,
			bson.D{{Key: "_id", Value: tx.EndToEndId}, {Key: "status", Value: StatusAwaitingCreditor}},
			bson.D{{Key: "$set", Value: bson.D{
				{Key: "status", Value: StatusSettling},
				{Key: "acceptedAt", Value: accepted},
				{Key: "updatedAt", Value: now},
			}}})
		if err != nil {
			return err
		}
		if res.ModifiedCount == 0 && tx.Status != StatusSettling {
			slog.InfoContext(ctx, "pacs.002 tardia ou repetida ignorada", "end_to_end_id", tx.EndToEndId, "status", tx.Status)
			return nil
		}
		// A liquidação não depende mais de quem chamou: se o cliente desistir,
		// ela segue (e a varredura retoma se algo falhar no meio).
		settleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		s.settle(settleCtx, tx)
		return nil
	default:
		return newAPIError(http.StatusBadRequest, "INVALID_MESSAGE", "participante só envia ACSP ou RJCT")
	}
}

// reject encerra a transação que estiver em from e avisa o pagador (e o
// recebedor, quando ele precisa saber que não haverá liquidação).
func (s *Server) reject(ctx context.Context, tx Transaction, from, reason string, notifyCreditor bool) error {
	now := store.Now()
	changed := false
	err := store.Transact(ctx, s.client, func(ctx context.Context) error {
		res, err := s.txs.UpdateOne(ctx,
			bson.D{{Key: "_id", Value: tx.EndToEndId}, {Key: "status", Value: from}},
			bson.D{{Key: "$set", Value: bson.D{
				{Key: "status", Value: StatusRejected},
				{Key: "reason", Value: reason},
				{Key: "finishedAt", Value: now},
				{Key: "updatedAt", Value: now},
			}}})
		if changed = err == nil && res.ModifiedCount == 1; !changed {
			return err
		}
		// Devolução rejeitada libera o valor que ela reservava do Pix original.
		if tx.Kind == KindReturn {
			if _, err := s.txs.UpdateOne(ctx, bson.D{{Key: "_id", Value: tx.OriginalEndToEndID}},
				bson.D{{Key: "$inc", Value: bson.D{{Key: "returnedAmount", Value: -tx.Amount}}}}); err != nil {
				return err
			}
		}
		st := iso20022.TxInfAndSts{
			OrgnlEndToEndId: tx.EndToEndId,
			TxSts:           iso20022.StatusRejected,
			StsRsnInf:       &iso20022.StsRsnInf{Rsn: reason},
		}
		return s.enqueueStatus(ctx, tx, st, notifyCreditor, now)
	})
	if err != nil || !changed {
		return err
	}
	s.notifier.notify(tx.DebtorISPB)
	if notifyCreditor {
		s.notifier.notify(tx.CreditorISPB)
	}
	s.finished(ctx, tx, StatusRejected, reason, now)
	return nil
}

// settle liquida no bacen-sim e confirma (ACSC) a pagador e recebedor. Falha
// temporária deixa a transação em SETTLING para a varredura tentar de novo;
// a liquidação no bacen é idempotente por EndToEndId.
func (s *Server) settle(ctx context.Context, tx Transaction) {
	res, err := s.cfg.Settler.Settle(ctx, bacen.SettlementRequest{
		EndToEndId:   tx.EndToEndId,
		DebtorISPB:   tx.DebtorISPB,
		CreditorISPB: tx.CreditorISPB,
		Amount:       tx.Amount,
	})
	if err != nil {
		if reason, permanent := settlementRejection(err); permanent {
			if err := s.reject(ctx, tx, StatusSettling, reason, true); err != nil {
				slog.ErrorContext(ctx, "falha ao rejeitar transação", "end_to_end_id", tx.EndToEndId, "err", err)
			}
			return
		}
		slog.WarnContext(ctx, "liquidação pendente; nova tentativa na varredura", "end_to_end_id", tx.EndToEndId, "err", err)
		_, _ = s.txs.UpdateOne(ctx, bson.D{{Key: "_id", Value: tx.EndToEndId}},
			bson.D{{Key: "$set", Value: bson.D{{Key: "updatedAt", Value: store.Now()}}}})
		return
	}

	now := store.Now()
	settledAt := res.SettledAt.UTC()
	changed := false
	err = store.Transact(ctx, s.client, func(ctx context.Context) error {
		upd, err := s.txs.UpdateOne(ctx,
			bson.D{{Key: "_id", Value: tx.EndToEndId}, {Key: "status", Value: StatusSettling}},
			bson.D{{Key: "$set", Value: bson.D{
				{Key: "status", Value: StatusSettled},
				{Key: "settledAt", Value: settledAt},
				{Key: "finishedAt", Value: now},
				{Key: "updatedAt", Value: now},
			}}})
		if changed = err == nil && upd.ModifiedCount == 1; !changed {
			return err
		}
		st := iso20022.TxInfAndSts{
			OrgnlEndToEndId:   tx.EndToEndId,
			TxSts:             iso20022.StatusSettled,
			FctvIntrBkSttlmDt: &settledAt,
		}
		return s.enqueueStatus(ctx, tx, st, true, now)
	})
	if err != nil {
		slog.ErrorContext(ctx, "falha ao confirmar liquidação", "end_to_end_id", tx.EndToEndId, "err", err)
		return
	}
	if !changed {
		return
	}
	s.notifier.notify(tx.DebtorISPB)
	s.notifier.notify(tx.CreditorISPB)
	s.finished(ctx, tx, StatusSettled, "", now)
	slog.InfoContext(ctx, "transação liquidada", "end_to_end_id", tx.EndToEndId)
}

// settlementRejection traduz erro definitivo do bacen-sim em motivo de RJCT.
func settlementRejection(err error) (reason string, permanent bool) {
	// Só resposta da API conta como definitiva; erro de rede é sempre temporário.
	var apiErr interface {
		error
		ErrorCode() string
		Temporary() bool
	}
	if !errors.As(err, &apiErr) || apiErr.Temporary() {
		return "", false
	}
	if apiErr.ErrorCode() == bacen.CodeInsufficientFunds {
		return iso20022.ReasonInsufficientFunds, true
	}
	return iso20022.ReasonSettlementFailed, true
}

// enqueueStatus envia a pacs.002 do SPI ao pagador e, se pedido, ao recebedor.
func (s *Server) enqueueStatus(ctx context.Context, tx Transaction, st iso20022.TxInfAndSts, toCreditor bool, now time.Time) error {
	targets := []string{tx.DebtorISPB}
	if toCreditor {
		targets = append(targets, tx.CreditorISPB)
	}
	tc := injectTrace(ctx)
	for _, to := range targets {
		// Falha injetada: a pacs.002 se perde. O estado da transação já mudou,
		// então só a consulta de status (resolver) revela o resultado.
		if s.cfg.Faults.Roll(ctx, s.cfg.Faults.Get().DropPacs002, "drop_pacs002") {
			continue
		}
		env := iso20022.NewPacs002Envelope(pix.BCBISPB, to, tx.MsgId, st, now)
		if tx.Kind == KindReturn {
			env.FIToFIPmtStsRpt.OrgnlGrpInfAndSts.OrgnlMsgNmId = iso20022.MsgPacs004
		}
		if err := s.enqueue(ctx, to, env, tc, now); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) finished(ctx context.Context, tx Transaction, status, reason string, now time.Time) {
	attrs := metric.WithAttributes(attribute.String("status", status), attribute.String("reason", reason))
	s.completed.Add(ctx, 1, attrs)
	s.duration.Record(ctx, now.Sub(tx.ReceivedAt).Seconds(), metric.WithAttributes(attribute.String("status", status)))
	if reason != "" {
		slog.InfoContext(ctx, "transação rejeitada", "end_to_end_id", tx.EndToEndId, "reason", reason)
	}
	s.queueIGA(ctx, tx, status, reason, now)
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	requester := r.Header.Get(pix.HeaderRequestingParticipant)
	var tx Transaction
	err := s.txs.FindOne(r.Context(), bson.D{{Key: "_id", Value: r.PathValue("endToEndId")}}).Decode(&tx)
	switch {
	case errors.Is(err, mongo.ErrNoDocuments) || (err == nil && requester != tx.DebtorISPB && requester != tx.CreditorISPB):
		// Quem não participa da transação não descobre nem que ela existe.
		httpx.WriteError(w, http.StatusNotFound, "UNKNOWN_END_TO_END_ID", "transação não encontrada")
		return
	case err != nil:
		httpx.WriteInternal(w, r, err)
		return
	}
	st := iso20022.TxInfAndSts{
		OrgnlEndToEndId:   tx.EndToEndId,
		AccptncDtTm:       tx.AcceptedAt,
		FctvIntrBkSttlmDt: tx.SettledAt,
	}
	switch tx.Status {
	case StatusAwaitingCreditor:
		st.TxSts = iso20022.StatusPending
	case StatusSettling:
		st.TxSts = iso20022.StatusAccepted
	case StatusSettled:
		st.TxSts = iso20022.StatusSettled
	case StatusRejected:
		st.TxSts = iso20022.StatusRejected
		st.StsRsnInf = &iso20022.StsRsnInf{Rsn: tx.Reason}
	}
	httpx.WriteJSON(w, http.StatusOK, st)
}

// Run executa a varredura periódica e o envio dos instantes do IGA até o
// contexto ser cancelado.
func (s *Server) Run(ctx context.Context) error {
	defer close(s.stopping)
	if s.cfg.IGA != nil {
		var wg sync.WaitGroup
		wg.Go(func() { s.flushIGA(ctx) })
		defer wg.Wait()
	}
	t := time.NewTicker(s.cfg.SweepInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if err := s.sweep(ctx); err != nil && ctx.Err() == nil {
				slog.ErrorContext(ctx, "falha na varredura", "err", err)
			}
		}
	}
}

// sweep rejeita (AB03) transações cujo recebedor perdeu o prazo e retoma
// liquidações interrompidas. Cada ação continua o trace da pacs.008 original.
func (s *Server) sweep(ctx context.Context) error {
	now := store.Now()
	expired, err := s.find(ctx, bson.D{
		{Key: "status", Value: StatusAwaitingCreditor},
		{Key: "deadline", Value: bson.D{{Key: "$lte", Value: now}}},
	})
	if err != nil {
		return err
	}
	for _, tx := range expired {
		tctx, span := s.tracer.Start(extractTrace(ctx, tx.TraceContext), "spi.creditor_timeout",
			trace.WithAttributes(attribute.String("pix.end_to_end_id", tx.EndToEndId)))
		if err := s.reject(tctx, tx, StatusAwaitingCreditor, iso20022.ReasonSettlementTimeout, !tx.NotForwarded); err != nil {
			slog.ErrorContext(tctx, "falha ao rejeitar por prazo", "end_to_end_id", tx.EndToEndId, "err", err)
		}
		span.End()
	}

	stuck, err := s.find(ctx, bson.D{
		{Key: "status", Value: StatusSettling},
		{Key: "updatedAt", Value: bson.D{{Key: "$lte", Value: now.Add(-s.cfg.SettleRetry)}}},
	})
	if err != nil {
		return err
	}
	for _, tx := range stuck {
		tctx, span := s.tracer.Start(extractTrace(ctx, tx.TraceContext), "spi.settlement_retry",
			trace.WithAttributes(attribute.String("pix.end_to_end_id", tx.EndToEndId)))
		s.settle(tctx, tx)
		span.End()
	}
	return nil
}

func (s *Server) find(ctx context.Context, filter bson.D) ([]Transaction, error) {
	cur, err := s.txs.Find(ctx, filter, options.Find().SetLimit(100))
	if err != nil {
		return nil, err
	}
	var txs []Transaction
	return txs, cur.All(ctx, &txs)
}

// injectTrace serializa o contexto de trace (traceparent) para viajar junto
// com a mensagem até o destinatário.
func injectTrace(ctx context.Context) map[string]string {
	c := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, c)
	if len(c) == 0 {
		return nil
	}
	return c
}

func extractTrace(ctx context.Context, tc map[string]string) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(tc))
}
