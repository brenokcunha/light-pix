package psp

import (
	"context"
	"encoding/json"
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

	"github.com/brenokcunha/light-pix/internal/broker"
	"github.com/brenokcunha/light-pix/internal/clients"
	"github.com/brenokcunha/light-pix/internal/iso20022"
	"github.com/brenokcunha/light-pix/internal/outbox"
	"github.com/brenokcunha/light-pix/internal/pix"
	"github.com/brenokcunha/light-pix/internal/sim/dict"
	"github.com/brenokcunha/light-pix/internal/store"
)

// Papéis num relato de infração (MED).
const (
	RoleReporter = "REPORTER" // PSP do pagador: relata e acompanha
	RoleReceiver = "RECEIVER" // PSP recebedor: bloqueia, analisa e devolve
)

// Estados do relato no PSP.
const (
	InfractionOpen         = "OPEN"          // REPORTER: aberto no DICT, aguardando a análise
	InfractionBlocked      = "BLOCKED"       // RECEIVER: valor bloqueado, aguardando a análise
	InfractionAgreed       = "AGREED"        // fraude confirmada; devolução especial em andamento
	InfractionReturned     = "RETURNED"      // devolução especial concluída
	InfractionReturnFailed = "RETURN_FAILED" // devolução especial recusada: exige ação manual
	InfractionNoFunds      = "NO_FUNDS"      // concordou, mas o valor já tinha saído da conta
	InfractionDisapproved  = "DISAPPROVED"   // o recebedor discordou
	InfractionCancelled    = "CANCELLED"     // cancelado pelo PSP do pagador antes da análise
)

// DefaultInfractionWindow é o prazo para relatar um Pix.
const DefaultInfractionWindow = 80 * 24 * time.Hour

// Infraction é um relato de infração do MED visto por um dos PSPs.
type Infraction struct {
	ID            string        `bson:"_id" json:"id"` // identificador do relato no DICT
	Role          string        `bson:"role" json:"role"`
	EndToEndID    string        `bson:"endToEndId" json:"endToEndId"`
	Reason        string        `bson:"reason" json:"reason"`
	Details       string        `bson:"details,omitempty" json:"details,omitempty"`
	Amount        pix.Amount    `bson:"amount" json:"amount"`                                   // valor relatado
	BlockedAmount pix.Amount    `bson:"blockedAmount,omitempty" json:"blockedAmount,omitempty"` // RECEIVER: o que deu para bloquear
	Counterparty  string        `bson:"counterparty" json:"counterparty"`                       // ISPB do outro PSP
	State         string        `bson:"state" json:"state"`
	Decision      string        `bson:"decision,omitempty" json:"decision,omitempty"` // RECEIVER: AGREED | DISAPPROVED
	DecisionNote  string        `bson:"decisionNote,omitempty" json:"decisionNote,omitempty"`
	DictSynced    bool          `bson:"dictSynced,omitempty" json:"-"`                // RECEIVER: a decisão já foi ao DICT
	ReturnID      string        `bson:"returnId,omitempty" json:"returnId,omitempty"` // devolução especial
	AccountID     bson.ObjectID `bson:"accountId,omitempty" json:"-"`                 // RECEIVER: conta bloqueada
	History       []Transition  `bson:"history" json:"history"`
	Deadline      time.Time     `bson:"deadline" json:"deadline"` // prazo de análise
	CreatedAt     time.Time     `bson:"createdAt" json:"createdAt"`
	UpdatedAt     time.Time     `bson:"updatedAt" json:"updatedAt"`
}

// MEDGateway é o que o PSP usa do DICT para os relatos.
type MEDGateway interface {
	CreateInfraction(ctx context.Context, req dict.InfractionRequest) (dict.InfractionReport, error)
	GetInfraction(ctx context.Context, id string) (dict.InfractionReport, error)
	ListInfractions(ctx context.Context, role, state string) ([]dict.InfractionReport, error)
	AnalyzeInfraction(ctx context.Context, id string, a dict.AnalysisRequest) (dict.InfractionReport, error)
	CancelInfraction(ctx context.Context, id string) (dict.InfractionReport, error)
}

// SetMED liga o MED ao serviço. Sem isso, as chamadas de relato falham.
func (s *Service) SetMED(g MEDGateway) { s.med = g }

func (s *Service) infractionWindow() time.Duration {
	if s.InfractionWindow > 0 {
		return s.InfractionWindow
	}
	return DefaultInfractionWindow
}

func (s *Service) countInfraction(ctx context.Context, role, state string) {
	s.infractionEvents.Add(ctx, 1, metric.WithAttributes(attribute.String("role", role), attribute.String("state", state)))
}

func (s *Service) requireMED() error {
	if s.med == nil {
		return newError(http.StatusServiceUnavailable, "MED_UNAVAILABLE", "MED não configurado neste PSP")
	}
	return nil
}

// dictToAPIError traduz respostas do DICT em erros da API, preservando 4xx.
func dictToAPIError(err error) error {
	var apiErr *clients.APIError
	if errors.As(err, &apiErr) && apiErr.Status >= 400 && apiErr.Status < 500 {
		return &Error{Status: apiErr.Status, Code: apiErr.Code, Message: apiErr.Message}
	}
	return newError(http.StatusBadGateway, "DICT_ERROR", "falha ao falar com o DICT: %v", err)
}

// ReportInfraction é o PSP do pagador relatando que um Pix confirmado pode ser
// fraude: abre o relato no DICT e o acompanha. O PSP recebedor bloqueia o valor
// ao ver o relato e decide.
func (s *Service) ReportInfraction(ctx context.Context, endToEndID, reason, details string) (Infraction, error) {
	if err := s.requireMED(); err != nil {
		return Infraction{}, err
	}
	if reason == "" {
		reason = dict.ReasonFraud
	}
	if reason != dict.ReasonFraud && reason != dict.ReasonScam {
		return Infraction{}, newError(http.StatusBadRequest, "INVALID_REQUEST", "reason deve ser %s ou %s", dict.ReasonFraud, dict.ReasonScam)
	}
	p, err := s.store.FindPayment(ctx, endToEndID)
	switch {
	case errors.Is(err, mongo.ErrNoDocuments):
		return Infraction{}, newError(http.StatusNotFound, "PAYMENT_NOT_FOUND", "pagamento não encontrado")
	case err != nil:
		return Infraction{}, err
	case p.Direction != DirectionOut || p.State != StateConfirmed:
		return Infraction{}, newError(http.StatusUnprocessableEntity, "NOT_REPORTABLE", "só se relata Pix enviado e confirmado (este é %s %s)", p.Direction, p.State)
	}
	settled := p.CreatedAt
	if p.SettledAt != nil {
		settled = *p.SettledAt
	}
	if time.Since(settled) > s.infractionWindow() {
		return Infraction{}, newError(http.StatusUnprocessableEntity, "REPORT_WINDOW_EXPIRED", "o prazo de %s para relatar o Pix venceu", s.infractionWindow())
	}

	rep, err := s.med.CreateInfraction(ctx, dict.InfractionRequest{
		EndToEndID: endToEndID, Reason: reason, Details: details, CounterpartyISPB: p.Creditor.ISPB, Amount: p.Amount,
	})
	if err != nil {
		return Infraction{}, dictToAPIError(err)
	}
	now := store.Now()
	inf := Infraction{
		ID: rep.ID, Role: RoleReporter, EndToEndID: endToEndID, Reason: reason, Details: details, Amount: p.Amount,
		Counterparty: p.Creditor.ISPB, State: InfractionOpen, Deadline: rep.AnalysisDeadline,
		History: []Transition{{State: InfractionOpen, At: now}}, CreatedAt: now, UpdatedAt: now,
	}
	if _, err := s.store.infractions.InsertOne(ctx, inf); err != nil && !mongo.IsDuplicateKeyError(err) {
		return Infraction{}, err
	}
	s.countInfraction(ctx, RoleReporter, InfractionOpen)
	slog.InfoContext(ctx, "relato de infração aberto", "infraction_id", rep.ID, "end_to_end_id", endToEndID, "counterparty", p.Creditor.ISPB)
	return inf, nil
}

// CancelInfraction cancela um relato aberto (só o PSP do pagador, antes da análise).
func (s *Service) CancelInfraction(ctx context.Context, id string) (Infraction, error) {
	if err := s.requireMED(); err != nil {
		return Infraction{}, err
	}
	inf, err := s.GetInfraction(ctx, id)
	switch {
	case err != nil:
		return Infraction{}, err
	case inf.Role != RoleReporter:
		return Infraction{}, newError(http.StatusForbidden, "FORBIDDEN", "só o PSP do pagador cancela o relato")
	case inf.State != InfractionOpen:
		return Infraction{}, newError(http.StatusConflict, "INFRACTION_NOT_OPEN", "o relato já está %s", inf.State)
	}
	if _, err := s.med.CancelInfraction(ctx, id); err != nil {
		return Infraction{}, dictToAPIError(err)
	}
	if err := s.setInfractionState(ctx, id, []string{InfractionOpen}, InfractionCancelled, nil); err != nil {
		return Infraction{}, err
	}
	return s.GetInfraction(ctx, id)
}

// GetInfraction devolve o relato pelo identificador do DICT.
func (s *Service) GetInfraction(ctx context.Context, id string) (Infraction, error) {
	var inf Infraction
	err := s.store.infractions.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&inf)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return Infraction{}, newError(http.StatusNotFound, "INFRACTION_NOT_FOUND", "relato não encontrado")
	}
	return inf, err
}

// ListInfractions lista os relatos do PSP, filtrando por papel e estado (vazios não filtram).
func (s *Service) ListInfractions(ctx context.Context, role, state string) ([]Infraction, error) {
	filter := bson.D{}
	if role != "" {
		filter = append(filter, bson.E{Key: "role", Value: role})
	}
	if state != "" {
		filter = append(filter, bson.E{Key: "state", Value: state})
	}
	cur, err := s.store.infractions.Find(ctx, filter, options.Find().SetSort(bson.D{{Key: "createdAt", Value: 1}}).SetLimit(200))
	if err != nil {
		return nil, err
	}
	out := []Infraction{}
	return out, cur.All(ctx, &out)
}

// setInfractionState muda o estado, condicionado ao atual, e registra no histórico.
// extra são campos adicionais do $set.
func (s *Service) setInfractionState(ctx context.Context, id string, from []string, to string, extra bson.D) error {
	now := store.Now()
	set := append(bson.D{{Key: "state", Value: to}, {Key: "updatedAt", Value: now}}, extra...)
	var inf Infraction
	err := s.store.infractions.FindOneAndUpdate(ctx,
		bson.D{{Key: "_id", Value: id}, {Key: "state", Value: bson.D{{Key: "$in", Value: from}}}},
		bson.D{{Key: "$set", Value: set}, {Key: "$push", Value: bson.D{{Key: "history", Value: Transition{State: to, At: now}}}}},
		options.FindOneAndUpdate().SetReturnDocument(options.After)).Decode(&inf)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil // outro ciclo já mudou: idempotente
	}
	if err == nil {
		s.countInfraction(ctx, inf.Role, to)
	}
	return err
}

var errBlockRetry = errors.New("saldo mudou durante o bloqueio")

// receiveInfraction trata um relato que chegou ao PSP recebedor: bloqueia
// cautelarmente, na conta que recebeu o Pix, o valor relatado ou o que dele
// ainda estiver disponível (o MED 2.0 completo rastrearia o que já saiu para
// outras contas; aqui o bloqueio para na conta que recebeu). Se o Pix não é
// deste PSP ou não bate com o relato, o relato é recusado.
func (s *Service) receiveInfraction(ctx context.Context, rep dict.InfractionReport) error {
	now := store.Now()
	inf := Infraction{
		ID: rep.ID, Role: RoleReceiver, EndToEndID: rep.EndToEndID, Reason: rep.Reason, Details: rep.Details,
		Amount: rep.Amount, Counterparty: rep.ReporterISPB, Deadline: rep.AnalysisDeadline,
		CreatedAt: now, UpdatedAt: now, History: []Transition{{State: InfractionOpen, At: now}},
	}
	p, err := s.store.FindPayment(ctx, rep.EndToEndID)
	switch {
	case err != nil && !errors.Is(err, mongo.ErrNoDocuments):
		return err
	case errors.Is(err, mongo.ErrNoDocuments) || p.Direction != DirectionIn || p.State != StateCredited ||
		p.Debtor.ISPB != rep.ReporterISPB || rep.Amount > p.Amount:
		inf.State, inf.Decision, inf.DecisionNote = InfractionDisapproved, dict.InfractionDisapproved, "Pix não encontrado ou inconsistente com o relato"
		inf.History = append(inf.History, Transition{State: InfractionDisapproved, At: now, Reason: inf.DecisionNote})
		if _, err := s.store.infractions.InsertOne(ctx, inf); err != nil && !mongo.IsDuplicateKeyError(err) {
			return err
		}
		s.countInfraction(ctx, RoleReceiver, InfractionDisapproved)
		return nil
	}
	inf.AccountID = p.Creditor.AccountID

	for range 3 {
		var acc Account
		if err := s.store.accounts.FindOne(ctx, bson.D{{Key: "_id", Value: inf.AccountID}}).Decode(&acc); err != nil {
			return err
		}
		blocked := min(rep.Amount, acc.Balance.Available)
		inf.BlockedAmount = blocked
		inf.State = InfractionBlocked
		inf.History = []Transition{{State: InfractionOpen, At: now}, {State: InfractionBlocked, At: now}}
		err = store.Transact(ctx, s.store.client, func(ctx context.Context) error {
			if blocked > 0 {
				res, err := s.store.accounts.UpdateOne(ctx,
					bson.D{{Key: "_id", Value: acc.ID}, {Key: "balance.available", Value: bson.D{{Key: "$gte", Value: blocked}}}},
					bson.D{{Key: "$inc", Value: bson.D{{Key: "balance.available", Value: -blocked}, {Key: "balance.blocked", Value: blocked}}},
						{Key: "$set", Value: bson.D{{Key: "updatedAt", Value: now}}}})
				if err != nil {
					return err
				}
				if res.MatchedCount == 0 {
					return errBlockRetry
				}
				if _, err := s.store.ledger.InsertOne(ctx, LedgerEntry{ID: bson.NewObjectID(), AccountID: acc.ID,
					EndToEndID: inf.ID, Kind: EntryBlock, AvailableDelta: -blocked, BlockedDelta: blocked, CreatedAt: now}); err != nil {
					return err
				}
			}
			_, err := s.store.infractions.InsertOne(ctx, inf)
			return err
		})
		if errors.Is(err, errBlockRetry) {
			continue
		}
		break
	}
	if mongo.IsDuplicateKeyError(err) {
		return nil
	}
	if err != nil {
		return err
	}
	s.countInfraction(ctx, RoleReceiver, InfractionBlocked)
	slog.WarnContext(ctx, "valor bloqueado por relato de infração", "infraction_id", rep.ID, "end_to_end_id", rep.EndToEndID,
		"reported", rep.Amount.String(), "blocked", inf.BlockedAmount.String())
	return nil
}

// DecideInfraction é a análise do PSP recebedor. Concordar converte o bloqueio
// numa devolução especial (pacs.004, motivo FR01) do valor bloqueado; discordar
// libera o valor. A decisão vai ao DICT no ciclo seguinte do MED.
func (s *Service) DecideInfraction(ctx context.Context, id string, agree bool, note string) (Infraction, error) {
	inf, err := s.GetInfraction(ctx, id)
	switch {
	case err != nil:
		return Infraction{}, err
	case inf.Role != RoleReceiver:
		return Infraction{}, newError(http.StatusForbidden, "FORBIDDEN", "a análise cabe ao PSP recebedor")
	case inf.State != InfractionBlocked:
		return Infraction{}, newError(http.StatusConflict, "INFRACTION_NOT_BLOCKED", "o relato já está %s", inf.State)
	}
	now := store.Now()
	switch {
	case !agree:
		err = s.unblock(ctx, inf, InfractionDisapproved, dict.InfractionDisapproved, note)
	case inf.BlockedAmount == 0:
		err = s.setInfractionState(ctx, id, []string{InfractionBlocked}, InfractionNoFunds, bson.D{
			{Key: "decision", Value: dict.InfractionAgreed}, {Key: "decisionNote", Value: note}})
	default:
		err = s.specialReturn(ctx, inf, note, now)
	}
	if err != nil {
		return Infraction{}, err
	}
	return s.GetInfraction(ctx, id)
}

// unblock devolve o valor bloqueado ao saldo disponível e encerra o relato.
func (s *Service) unblock(ctx context.Context, inf Infraction, to, decision, note string) error {
	now := store.Now()
	return store.Transact(ctx, s.store.client, func(ctx context.Context) error {
		res, err := s.store.infractions.UpdateOne(ctx,
			bson.D{{Key: "_id", Value: inf.ID}, {Key: "state", Value: InfractionBlocked}},
			bson.D{{Key: "$set", Value: bson.D{{Key: "state", Value: to}, {Key: "decision", Value: decision},
				{Key: "decisionNote", Value: note}, {Key: "updatedAt", Value: now}}},
				{Key: "$push", Value: bson.D{{Key: "history", Value: Transition{State: to, At: now}}}}})
		if err != nil {
			return err
		}
		if res.ModifiedCount == 0 {
			return nil // outro ciclo já decidiu
		}
		if inf.BlockedAmount > 0 {
			if _, err := s.store.accounts.UpdateOne(ctx, bson.D{{Key: "_id", Value: inf.AccountID}},
				bson.D{{Key: "$inc", Value: bson.D{{Key: "balance.blocked", Value: -inf.BlockedAmount}, {Key: "balance.available", Value: inf.BlockedAmount}}},
					{Key: "$set", Value: bson.D{{Key: "updatedAt", Value: now}}}}); err != nil {
				return err
			}
			if _, err := s.store.ledger.InsertOne(ctx, LedgerEntry{ID: bson.NewObjectID(), AccountID: inf.AccountID,
				EndToEndID: inf.ID, Kind: EntryUnblock, AvailableDelta: inf.BlockedAmount, BlockedDelta: -inf.BlockedAmount, CreatedAt: now}); err != nil {
				return err
			}
		}
		s.countInfraction(ctx, RoleReceiver, to)
		return nil
	})
}

// specialReturn converte o bloqueio na devolução especial, numa transação:
// reserva o valor devolvível do Pix original, passa o valor bloqueado para a
// reserva da devolução (blocked → held), grava a devolução PENDING, a pacs.004
// na outbox e o relato como AGREED. Depois segue o caminho de qualquer devolução.
func (s *Service) specialReturn(ctx context.Context, inf Infraction, note string, now time.Time) error {
	orig, err := s.store.FindPayment(ctx, inf.EndToEndID)
	if err != nil {
		return err
	}
	amount := inf.BlockedAmount
	id := pix.NewReturnID(s.ispb, now)
	env := iso20022.NewPacs004Envelope(iso20022.RtrTxInf{
		RtrId:              id,
		OrgnlEndToEndId:    orig.EndToEndID,
		RtrdIntrBkSttlmAmt: iso20022.ActiveCurrencyAndAmount{Ccy: "BRL", Value: amount},
		RtrRsnInf:          iso20022.RtrRsnInf{Rsn: iso20022.ReturnFraud, AddtlInf: "MED:" + inf.ID},
		Dbtr:               iso20022.Party{Nm: orig.Creditor.Name, Id: orig.Creditor.TaxID},
		DbtrAcct:           iso20022.Account{Issr: orig.Creditor.Branch, Id: orig.Creditor.Number, Tp: orig.Creditor.Type},
		DbtrAgt:            iso20022.Agent{MmbId: s.ispb},
		Cdtr:               iso20022.Party{Nm: orig.Debtor.Name, Id: orig.Debtor.TaxID},
		CdtrAcct:           iso20022.Account{Issr: orig.Debtor.Branch, Id: orig.Debtor.Number, Tp: orig.Debtor.Type},
		CdtrAgt:            iso20022.Agent{MmbId: orig.Debtor.ISPB},
	}, now)
	if err := env.Validate(); err != nil {
		return fmt.Errorf("pacs.004 inválida: %w", err)
	}
	body, err := json.Marshal(env)
	if err != nil {
		return err
	}
	ret := Payment{
		EndToEndID: id, Direction: DirectionOut, State: StatePending, Amount: amount, Description: "MED " + inf.ID,
		Debtor: orig.Creditor, Creditor: orig.Debtor, MsgID: env.PmtRtr.GrpHdr.MsgId,
		ReturnOf: orig.EndToEndID, ReturnReason: iso20022.ReturnFraud,
		History: []Transition{{State: StatePending, At: now}}, CreatedAt: now, UpdatedAt: now,
	}
	ret.Debtor.Key, ret.Creditor.Key = "", ""

	err = store.Transact(ctx, s.store.client, func(ctx context.Context) error {
		res, err := s.store.infractions.UpdateOne(ctx,
			bson.D{{Key: "_id", Value: inf.ID}, {Key: "state", Value: InfractionBlocked}},
			bson.D{{Key: "$set", Value: bson.D{{Key: "state", Value: InfractionAgreed}, {Key: "decision", Value: dict.InfractionAgreed},
				{Key: "decisionNote", Value: note}, {Key: "returnId", Value: id}, {Key: "updatedAt", Value: now}}},
				{Key: "$push", Value: bson.D{{Key: "history", Value: Transition{State: InfractionAgreed, At: now}}}}})
		if err != nil {
			return err
		}
		if res.ModifiedCount == 0 {
			return errSkip
		}
		if res, err = s.store.payments.UpdateOne(ctx, exceedsFilter(orig.EndToEndID, DirectionIn, StateCredited, amount),
			bson.D{{Key: "$inc", Value: bson.D{{Key: "returnedAmount", Value: amount}}}}); err != nil {
			return err
		}
		if res.MatchedCount == 0 {
			return errReturnExceeds
		}
		if _, err := s.store.accounts.UpdateOne(ctx, bson.D{{Key: "_id", Value: inf.AccountID}},
			bson.D{{Key: "$inc", Value: bson.D{{Key: "balance.blocked", Value: -amount}, {Key: "balance.held", Value: amount}}},
				{Key: "$set", Value: bson.D{{Key: "updatedAt", Value: now}}}}); err != nil {
			return err
		}
		if _, err := s.store.ledger.InsertOne(ctx, LedgerEntry{ID: bson.NewObjectID(), AccountID: inf.AccountID,
			EndToEndID: id, Kind: EntryMedReserve, HeldDelta: amount, BlockedDelta: -amount, CreatedAt: now}); err != nil {
			return err
		}
		if _, err := s.store.returns.InsertOne(ctx, ret); err != nil {
			return err
		}
		return outbox.Add(ctx, s.store.outbox, broker.QueueSPIOut, env.AppHdr.BizMsgIdr, body)
	})
	switch {
	case errors.Is(err, errSkip):
		return nil
	case errors.Is(err, errReturnExceeds):
		return newError(http.StatusUnprocessableEntity, "RETURN_AMOUNT_EXCEEDED", "o cliente já devolveu parte deste Pix: nada a devolver pelo MED além do que resta")
	case err != nil:
		return err
	}
	s.countInfraction(ctx, RoleReceiver, InfractionAgreed)
	slog.WarnContext(ctx, "devolução especial iniciada pelo MED", "infraction_id", inf.ID, "return_id", id, "amount", amount.String())
	return nil
}

// SyncInfractions é um ciclo do MED: traz do DICT os relatos novos e bloqueia,
// envia ao DICT as decisões tomadas, acompanha cancelamentos e decisões do outro
// lado e fecha os relatos cuja devolução especial terminou.
func (s *Service) SyncInfractions(ctx context.Context) error {
	if s.med == nil {
		return nil
	}
	var errs []error
	for _, step := range []func(context.Context) error{
		s.intakeInfractions, s.syncReceiverBlocked, s.syncReceiverDecisions, s.syncReporter, s.syncReturns,
	} {
		if err := step(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (s *Service) intakeInfractions(ctx context.Context) error {
	reports, err := s.med.ListInfractions(ctx, "counterparty", dict.InfractionOpen)
	if err != nil {
		return fmt.Errorf("listar relatos: %w", err)
	}
	for _, rep := range reports {
		n, err := s.store.infractions.CountDocuments(ctx, bson.D{{Key: "_id", Value: rep.ID}})
		if err != nil {
			return err
		}
		if n > 0 {
			continue
		}
		if err := s.receiveInfraction(ctx, rep); err != nil {
			return fmt.Errorf("relato %s: %w", rep.ID, err)
		}
	}
	return nil
}

func (s *Service) findInfractions(ctx context.Context, filter bson.D) ([]Infraction, error) {
	cur, err := s.store.infractions.Find(ctx, filter, options.Find().SetLimit(200))
	if err != nil {
		return nil, err
	}
	var list []Infraction
	return list, cur.All(ctx, &list)
}

// syncReceiverBlocked desbloqueia o valor se o PSP do pagador cancelou o relato.
func (s *Service) syncReceiverBlocked(ctx context.Context) error {
	list, err := s.findInfractions(ctx, bson.D{{Key: "role", Value: RoleReceiver}, {Key: "state", Value: InfractionBlocked}})
	if err != nil {
		return err
	}
	for _, inf := range list {
		rep, err := s.med.GetInfraction(ctx, inf.ID)
		if err != nil {
			return fmt.Errorf("relato %s: %w", inf.ID, err)
		}
		if rep.State == dict.InfractionCancelled {
			if err := s.unblock(ctx, inf, InfractionCancelled, "", "relato cancelado pelo PSP do pagador"); err != nil {
				return err
			}
		}
	}
	return nil
}

// syncReceiverDecisions leva ao DICT as decisões que ainda não foram.
func (s *Service) syncReceiverDecisions(ctx context.Context) error {
	list, err := s.findInfractions(ctx, bson.D{
		{Key: "role", Value: RoleReceiver},
		{Key: "dictSynced", Value: bson.D{{Key: "$ne", Value: true}}},
		{Key: "state", Value: bson.D{{Key: "$in", Value: bson.A{InfractionAgreed, InfractionNoFunds, InfractionDisapproved}}}},
	})
	if err != nil {
		return err
	}
	for _, inf := range list {
		result, returned := dict.InfractionAgreed, pix.Amount(0)
		if inf.State == InfractionDisapproved {
			result = dict.InfractionDisapproved
		} else if inf.ReturnID != "" {
			returned = inf.BlockedAmount
		}
		_, err := s.med.AnalyzeInfraction(ctx, inf.ID, dict.AnalysisRequest{
			Result: result, Details: inf.DecisionNote, ReturnID: inf.ReturnID, ReturnedAmount: returned})
		var apiErr *clients.APIError
		if err != nil && !(errors.As(err, &apiErr) && apiErr.Status == http.StatusConflict) {
			return fmt.Errorf("decisão do relato %s: %w", inf.ID, err)
		}
		if _, err := s.store.infractions.UpdateOne(ctx, bson.D{{Key: "_id", Value: inf.ID}},
			bson.D{{Key: "$set", Value: bson.D{{Key: "dictSynced", Value: true}}}}); err != nil {
			return err
		}
	}
	return nil
}

// syncReporter acompanha, no DICT, os relatos que este PSP abriu.
func (s *Service) syncReporter(ctx context.Context) error {
	list, err := s.findInfractions(ctx, bson.D{{Key: "role", Value: RoleReporter}, {Key: "state", Value: InfractionOpen}})
	if err != nil {
		return err
	}
	for _, inf := range list {
		rep, err := s.med.GetInfraction(ctx, inf.ID)
		if err != nil {
			return fmt.Errorf("relato %s: %w", inf.ID, err)
		}
		var to string
		var extra bson.D
		switch {
		case rep.State == dict.InfractionDisapproved:
			to = InfractionDisapproved
		case rep.State == dict.InfractionCancelled:
			to = InfractionCancelled
		case rep.State == dict.InfractionAgreed && rep.Analysis != nil && rep.Analysis.ReturnID == "":
			to = InfractionNoFunds
		case rep.State == dict.InfractionAgreed && rep.Analysis != nil:
			to, extra = InfractionAgreed, bson.D{{Key: "returnId", Value: rep.Analysis.ReturnID}}
		default:
			continue
		}
		if rep.Analysis != nil {
			extra = append(extra, bson.E{Key: "decisionNote", Value: rep.Analysis.Details})
		}
		if err := s.setInfractionState(ctx, inf.ID, []string{InfractionOpen}, to, extra); err != nil {
			return err
		}
	}
	return nil
}

// syncReturns fecha os relatos cuja devolução especial terminou: RETURNED se
// foi liquidada, RETURN_FAILED se foi recusada.
func (s *Service) syncReturns(ctx context.Context) error {
	list, err := s.findInfractions(ctx, bson.D{
		{Key: "state", Value: InfractionAgreed}, {Key: "returnId", Value: bson.D{{Key: "$exists", Value: true}}}})
	if err != nil {
		return err
	}
	for _, inf := range list {
		var ret Payment
		err := s.store.returns.FindOne(ctx, bson.D{{Key: "_id", Value: inf.ReturnID}}).Decode(&ret)
		if errors.Is(err, mongo.ErrNoDocuments) {
			continue // a pacs.004 ainda não chegou a este PSP
		}
		if err != nil {
			return err
		}
		switch {
		case slices.Contains([]string{StateConfirmed, StateCredited}, ret.State):
			err = s.setInfractionState(ctx, inf.ID, []string{InfractionAgreed}, InfractionReturned, nil)
		case ret.State == StateRejected:
			err = s.setInfractionState(ctx, inf.ID, []string{InfractionAgreed}, InfractionReturnFailed,
				bson.D{{Key: "decisionNote", Value: "devolução especial recusada: " + ret.Reason}})
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// overdueInfractions conta os relatos abertos ou bloqueados além do prazo de análise, por papel.
func (s *Service) overdueInfractions(ctx context.Context) (map[string]int64, error) {
	cur, err := s.store.infractions.Aggregate(ctx, bson.A{
		bson.D{{Key: "$match", Value: bson.D{
			{Key: "state", Value: bson.D{{Key: "$in", Value: bson.A{InfractionOpen, InfractionBlocked}}}},
			{Key: "deadline", Value: bson.D{{Key: "$lt", Value: time.Now().UTC()}}}}}},
		bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$role"}, {Key: "n", Value: bson.D{{Key: "$sum", Value: 1}}}}}},
	})
	if err != nil {
		return nil, err
	}
	var rows []struct {
		Role string `bson:"_id"`
		N    int64  `bson:"n"`
	}
	if err := cur.All(ctx, &rows); err != nil {
		return nil, err
	}
	out := map[string]int64{RoleReporter: 0, RoleReceiver: 0}
	for _, r := range rows {
		out[r.Role] = r.N
	}
	return out, nil
}
