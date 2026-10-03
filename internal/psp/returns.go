package psp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/brenokcunha/light-pix/internal/broker"
	"github.com/brenokcunha/light-pix/internal/iso20022"
	"github.com/brenokcunha/light-pix/internal/outbox"
	"github.com/brenokcunha/light-pix/internal/pix"
	"github.com/brenokcunha/light-pix/internal/store"
)

// DefaultReturnWindow é o prazo para devolver um Pix recebido.
const DefaultReturnWindow = 90 * 24 * time.Hour

var errReturnExceeds = errors.New("devolução acima do que resta do Pix")

// ReturnRequest é o pedido de devolução (total ou parcial) de um Pix recebido.
type ReturnRequest struct {
	Amount      pix.Amount `json:"amount"`
	Reason      string     `json:"reason,omitempty"` // MD06 (padrão: pedido do cliente) ou BE08 (erro do PSP)
	Description string     `json:"description,omitempty"`
}

// exceedsFilter casa o Pix original se somar amount ao já devolvido (ou em
// devolução) ainda cabe no valor do Pix.
func exceedsFilter(originalID, direction, state string, amount pix.Amount) bson.D {
	return bson.D{
		{Key: "_id", Value: originalID},
		{Key: "direction", Value: direction},
		{Key: "state", Value: state},
		{Key: "$expr", Value: bson.D{{Key: "$lte", Value: bson.A{
			bson.D{{Key: "$add", Value: bson.A{bson.D{{Key: "$ifNull", Value: bson.A{"$returnedAmount", 0}}}, amount}}},
			"$amount",
		}}}},
	}
}

// releaseReturned devolve ao Pix original o valor que uma devolução rejeitada
// reservava. Roda dentro da transação que rejeita a devolução.
func (s *Service) releaseReturned(ctx context.Context, ret Payment) error {
	if ret.ReturnOf == "" {
		return nil
	}
	_, err := s.store.payments.UpdateOne(ctx, bson.D{{Key: "_id", Value: ret.ReturnOf}},
		bson.D{{Key: "$inc", Value: bson.D{{Key: "returnedAmount", Value: -ret.Amount}}}})
	return err
}

func (s *Service) returnWindow() time.Duration {
	if s.ReturnWindow > 0 {
		return s.ReturnWindow
	}
	return DefaultReturnWindow
}

// CreateReturn devolve, total ou parcialmente, um Pix recebido. Numa transação:
// grava a chave de idempotência, reserva o valor devolvível do Pix original
// (teto: o valor dele, somando devoluções simultâneas), reserva o saldo do
// cliente que devolve, grava a devolução PENDING e a pacs.004 na outbox. Depois
// segue o mesmo caminho de um Pix enviado (SENT → CONFIRMED | REJECTED).
func (s *Service) CreateReturn(ctx context.Context, originalID, idemKey string, req ReturnRequest) (ret Payment, created bool, err error) {
	if req.Reason == "" {
		req.Reason = iso20022.ReturnCustomerRequest
	}
	switch {
	case idemKey == "" || len(idemKey) > 64:
		return ret, false, newError(http.StatusBadRequest, "INVALID_REQUEST", "header Idempotency-Key obrigatório (até 64 caracteres)")
	case req.Amount <= 0:
		return ret, false, newError(http.StatusBadRequest, "INVALID_REQUEST", "amount deve ser positivo")
	case req.Reason != iso20022.ReturnCustomerRequest && req.Reason != iso20022.ReturnBankError:
		return ret, false, newError(http.StatusBadRequest, "INVALID_REQUEST", "reason deve ser %s ou %s", iso20022.ReturnCustomerRequest, iso20022.ReturnBankError)
	}

	orig, err := s.store.FindPayment(ctx, originalID)
	switch {
	case errors.Is(err, mongo.ErrNoDocuments):
		return ret, false, newError(http.StatusNotFound, "PAYMENT_NOT_FOUND", "pagamento não encontrado")
	case err != nil:
		return ret, false, err
	case orig.Direction != DirectionIn || orig.State != StateCredited:
		return ret, false, newError(http.StatusUnprocessableEntity, "NOT_RETURNABLE", "só se devolve Pix recebido e creditado (este é %s %s)", orig.Direction, orig.State)
	}
	settled := orig.CreatedAt
	if orig.SettledAt != nil {
		settled = *orig.SettledAt
	}
	if time.Since(settled) > s.returnWindow() {
		return ret, false, newError(http.StatusUnprocessableEntity, "RETURN_WINDOW_EXPIRED", "o prazo de devolução de %s venceu", s.returnWindow())
	}
	var acc Account
	if err := s.store.accounts.FindOne(ctx, bson.D{{Key: "_id", Value: orig.Creditor.AccountID}}).Decode(&acc); err != nil {
		return ret, false, fmt.Errorf("conta do Pix %s: %w", originalID, err)
	}
	if acc.Status != AccountActive {
		return ret, false, newError(http.StatusUnprocessableEntity, "ACCOUNT_NOT_ACTIVE", "conta %s", strings.ToLower(acc.Status))
	}

	scope, hash := acc.ID.Hex(), requestHash(struct {
		Original string
		Req      ReturnRequest
	}{originalID, req})
	if prev, found, err := s.replay(ctx, scope, idemKey, hash); err != nil || found {
		return prev, false, err
	}

	now := store.Now()
	id := pix.NewReturnID(s.ispb, now)
	env := iso20022.NewPacs004Envelope(iso20022.RtrTxInf{
		RtrId:              id,
		OrgnlEndToEndId:    orig.EndToEndID,
		RtrdIntrBkSttlmAmt: iso20022.ActiveCurrencyAndAmount{Ccy: "BRL", Value: req.Amount},
		RtrRsnInf:          iso20022.RtrRsnInf{Rsn: req.Reason, AddtlInf: req.Description},
		Dbtr:               iso20022.Party{Nm: orig.Creditor.Name, Id: orig.Creditor.TaxID},
		DbtrAcct:           iso20022.Account{Issr: orig.Creditor.Branch, Id: orig.Creditor.Number, Tp: orig.Creditor.Type},
		DbtrAgt:            iso20022.Agent{MmbId: s.ispb},
		Cdtr:               iso20022.Party{Nm: orig.Debtor.Name, Id: orig.Debtor.TaxID},
		CdtrAcct:           iso20022.Account{Issr: orig.Debtor.Branch, Id: orig.Debtor.Number, Tp: orig.Debtor.Type},
		CdtrAgt:            iso20022.Agent{MmbId: orig.Debtor.ISPB},
	}, now)
	if err := env.Validate(); err != nil {
		return ret, false, fmt.Errorf("pacs.004 inválida: %w", err)
	}
	body, err := json.Marshal(env)
	if err != nil {
		return ret, false, err
	}
	trace.SpanFromContext(ctx).SetAttributes(attribute.String("pix.end_to_end_id", id),
		attribute.String("pix.original_end_to_end_id", orig.EndToEndID))

	ret = Payment{
		EndToEndID:   id,
		Direction:    DirectionOut,
		State:        StatePending,
		Amount:       req.Amount,
		Description:  req.Description,
		Debtor:       orig.Creditor, // quem devolve é o recebedor do Pix original
		Creditor:     orig.Debtor,
		MsgID:        env.PmtRtr.GrpHdr.MsgId,
		ReturnOf:     orig.EndToEndID,
		ReturnReason: req.Reason,
		History:      []Transition{{State: StatePending, At: now}},
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	ret.Debtor.Key, ret.Creditor.Key = "", ""

	err = store.Transact(ctx, s.store.client, func(ctx context.Context) error {
		if _, err := s.store.idem.InsertOne(ctx, IdempotencyKey{
			ID: bson.NewObjectID(), Scope: scope, Key: idemKey, RequestHash: hash, EndToEndID: id, CreatedAt: now,
		}); err != nil {
			return err
		}
		res, err := s.store.payments.UpdateOne(ctx, exceedsFilter(orig.EndToEndID, DirectionIn, StateCredited, req.Amount),
			bson.D{{Key: "$inc", Value: bson.D{{Key: "returnedAmount", Value: req.Amount}}}})
		if err != nil {
			return err
		}
		if res.MatchedCount == 0 {
			return errReturnExceeds
		}
		res, err = s.store.accounts.UpdateOne(ctx,
			bson.D{{Key: "_id", Value: acc.ID}, {Key: "status", Value: AccountActive},
				{Key: "balance.available", Value: bson.D{{Key: "$gte", Value: req.Amount}}}},
			bson.D{{Key: "$inc", Value: bson.D{{Key: "balance.available", Value: -req.Amount}, {Key: "balance.held", Value: req.Amount}}},
				{Key: "$set", Value: bson.D{{Key: "updatedAt", Value: now}}}})
		if err != nil {
			return err
		}
		if res.MatchedCount == 0 {
			return errInsufficientFunds
		}
		if _, err := s.store.ledger.InsertOne(ctx, LedgerEntry{
			ID: bson.NewObjectID(), AccountID: acc.ID, EndToEndID: id, Kind: EntryHold,
			AvailableDelta: -req.Amount, HeldDelta: req.Amount, CreatedAt: now,
		}); err != nil {
			return err
		}
		if _, err := s.store.returns.InsertOne(ctx, ret); err != nil {
			return err
		}
		return outbox.Add(ctx, s.store.outbox, broker.QueueSPIOut, env.AppHdr.BizMsgIdr, body)
	})
	switch {
	case errors.Is(err, errReturnExceeds):
		return Payment{}, false, newError(http.StatusUnprocessableEntity, "RETURN_AMOUNT_EXCEEDED",
			"a devolução passaria do valor do Pix (%s; já devolvido ou em devolução: %s)", orig.Amount, orig.ReturnedAmount)
	case errors.Is(err, errInsufficientFunds):
		return Payment{}, false, newError(http.StatusUnprocessableEntity, "INSUFFICIENT_FUNDS", "saldo insuficiente")
	case mongo.IsDuplicateKeyError(err):
		prev, found, rerr := s.replay(ctx, scope, idemKey, hash)
		if rerr != nil || !found {
			return Payment{}, false, errors.Join(err, rerr)
		}
		return prev, false, nil
	case err != nil:
		return Payment{}, false, err
	}
	slog.InfoContext(ctx, "devolução iniciada", "return_id", id, "original", orig.EndToEndID, "amount", req.Amount.String(), "reason", req.Reason)
	return ret, true, nil
}

// GetReturn devolve a devolução pelo identificador "D...".
func (s *Service) GetReturn(ctx context.Context, id string) (Payment, error) {
	var p Payment
	err := s.store.returns.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&p)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return Payment{}, newError(http.StatusNotFound, "RETURN_NOT_FOUND", "devolução não encontrada")
	}
	return p, err
}

// ListReturns lista as devoluções de um Pix, enviadas ou recebidas.
func (s *Service) ListReturns(ctx context.Context, originalID string) ([]Payment, error) {
	cur, err := s.store.returns.Find(ctx, bson.D{{Key: "returnOf", Value: originalID}},
		options.Find().SetSort(bson.D{{Key: "createdAt", Value: 1}}))
	if err != nil {
		return nil, err
	}
	out := []Payment{}
	return out, cur.All(ctx, &out)
}

// handleIncomingReturn trata uma pacs.004 vinda do SPI: este PSP é o do
// pagador original e vai receber a devolução. Aceita (ACSP) se o Pix original é
// dele, foi confirmado, a devolução cabe no que resta e a conta pode receber;
// senão recusa (RJCT):
//
//   - BE01: não conhece o Pix, ele não foi confirmado, ou não bate com a devolução;
//   - AC03 / AC06: conta inexistente / bloqueada ou encerrada;
//   - AM09: a devolução passaria do valor do Pix.
//
// O crédito só acontece na liquidação (ACSC), como no Pix recebido.
func (s *Service) handleIncomingReturn(ctx context.Context, env iso20022.Envelope) error {
	doc := env.PmtRtr
	tx := doc.TxInf
	if tx.CdtrAgt.MmbId != s.ispb {
		return broker.Permanent(fmt.Errorf("pacs.004 destinada a %s chegou ao PSP %s", tx.CdtrAgt.MmbId, s.ispb))
	}
	id := tx.RtrId
	trace.SpanFromContext(ctx).SetAttributes(attribute.String("pix.end_to_end_id", id),
		attribute.String("pix.original_end_to_end_id", tx.OrgnlEndToEndId))

	now := store.Now()
	ret := Payment{
		EndToEndID: id, Direction: DirectionIn, Amount: tx.RtrdIntrBkSttlmAmt.Value,
		Description: tx.RtrRsnInf.AddtlInf,
		Debtor: Party{ISPB: tx.DbtrAgt.MmbId, Branch: tx.DbtrAcct.Issr, Number: tx.DbtrAcct.Id, Type: tx.DbtrAcct.Tp,
			Name: tx.Dbtr.Nm, TaxID: tx.Dbtr.Id},
		Creditor: Party{ISPB: s.ispb, Branch: tx.CdtrAcct.Issr, Number: tx.CdtrAcct.Id, Type: tx.CdtrAcct.Tp,
			Name: tx.Cdtr.Nm, TaxID: tx.Cdtr.Id},
		MsgID: doc.GrpHdr.MsgId, ReturnOf: tx.OrgnlEndToEndId, ReturnReason: tx.RtrRsnInf.Rsn,
		History: []Transition{{State: StateReceived, At: now}}, CreatedAt: now, UpdatedAt: now,
	}

	reason := ""
	orig, oerr := s.store.FindPayment(ctx, tx.OrgnlEndToEndId)
	acc, aerr := s.store.FindAccount(ctx, tx.CdtrAcct.Issr, tx.CdtrAcct.Id)
	switch {
	case oerr != nil && !errors.Is(oerr, mongo.ErrNoDocuments):
		return oerr
	case aerr != nil && !errors.Is(aerr, mongo.ErrNoDocuments):
		return aerr
	case errors.Is(oerr, mongo.ErrNoDocuments) || orig.Direction != DirectionOut || orig.State != StateConfirmed ||
		orig.Creditor.ISPB != tx.DbtrAgt.MmbId || orig.Debtor.Number != tx.CdtrAcct.Id || orig.Debtor.Branch != tx.CdtrAcct.Issr:
		reason = iso20022.ReasonInconsistent
	case errors.Is(aerr, mongo.ErrNoDocuments):
		reason = iso20022.ReasonInvalidAccount
	case acc.Status != AccountActive:
		reason = iso20022.ReasonBlockedAccount
	default:
		ret.Creditor.AccountID, ret.Creditor.Name, ret.Creditor.TaxID = acc.ID, acc.Holder.Name, acc.Holder.TaxID
	}

	err := s.recordIncomingReturn(ctx, ret, reason, now)
	if errors.Is(err, errReturnExceeds) {
		// Duas devoluções concorrentes passaram juntas pela leitura; a reserva
		// atômica barrou esta.
		err = s.recordIncomingReturn(ctx, ret, iso20022.ReasonAmountInvalid, now)
	}
	if mongo.IsDuplicateKeyError(err) {
		slog.InfoContext(ctx, "pacs.004 repetida ignorada", "return_id", id)
		return nil
	}
	return err
}

// recordIncomingReturn grava a devolução recebida e a resposta (pacs.002) na
// outbox numa transação. Sem motivo, aceita: reserva o valor no Pix original (se
// não couber, devolve errReturnExceeds e nada é gravado).
func (s *Service) recordIncomingReturn(ctx context.Context, ret Payment, reason string, now time.Time) error {
	st := iso20022.TxInfAndSts{OrgnlEndToEndId: ret.EndToEndID, TxSts: iso20022.StatusAccepted, AccptncDtTm: &now}
	if reason == "" {
		ret.State, ret.AcceptedAt = StateAccepted, &now
		ret.History = append(ret.History, Transition{State: StateAccepted, At: now})
	} else {
		st = rejection(ret.EndToEndID, reason)
		ret.State, ret.Reason, ret.FinishedAt = StateRejected, reason, &now
		ret.History = append(ret.History, Transition{State: StateRejected, At: now, Reason: reason})
	}
	reply := iso20022.NewPacs002Envelope(s.ispb, pix.BCBISPB, ret.MsgID, st, now)
	reply.FIToFIPmtStsRpt.OrgnlGrpInfAndSts.OrgnlMsgNmId = iso20022.MsgPacs004
	body, err := json.Marshal(reply)
	if err != nil {
		return err
	}
	err = store.Transact(ctx, s.store.client, func(ctx context.Context) error {
		if reason == "" {
			res, err := s.store.payments.UpdateOne(ctx, exceedsFilter(ret.ReturnOf, DirectionOut, StateConfirmed, ret.Amount),
				bson.D{{Key: "$inc", Value: bson.D{{Key: "returnedAmount", Value: ret.Amount}}}})
			if err != nil {
				return err
			}
			if res.MatchedCount == 0 {
				return errReturnExceeds
			}
		}
		if _, err := s.store.returns.InsertOne(ctx, ret); err != nil {
			return err
		}
		return outbox.Add(ctx, s.store.outbox, broker.QueueSPIOut, reply.AppHdr.BizMsgIdr, body)
	})
	if err != nil {
		return err
	}
	slog.InfoContext(ctx, "devolução recebida", "return_id", ret.EndToEndID, "original", ret.ReturnOf, "state", ret.State, "reason", ret.Reason)
	return nil
}
