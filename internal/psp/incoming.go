package psp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/brenokcunha/light-pix/internal/broker"
	"github.com/brenokcunha/light-pix/internal/iso20022"
	"github.com/brenokcunha/light-pix/internal/outbox"
	"github.com/brenokcunha/light-pix/internal/pix"
	"github.com/brenokcunha/light-pix/internal/store"
)

// HandleIncoming trata uma pacs.008 vinda do SPI (fila pix.incoming). Numa
// única transação grava o recebimento e a resposta (pacs.002) na outbox:
//
//   - conta do recebedor existe e está ativa: recebimento ACCEPTED e ACSP;
//   - conta inexistente: REJECTED e RJCT AC03;
//   - conta bloqueada ou encerrada: REJECTED e RJCT AC06.
//
// O crédito só acontece quando o SPI confirma a liquidação (ACSC, tratado em
// HandleResult). Reentrega da mesma pacs.008 não faz nada: o _id do
// recebimento é o EndToEndId, e a resposta da primeira vez já está na outbox.
func (s *Service) HandleIncoming(ctx context.Context, d amqp.Delivery) error {
	var env iso20022.Envelope
	if err := json.Unmarshal(d.Body, &env); err != nil {
		return broker.Permanent(fmt.Errorf("pacs.008 ilegível: %w", err))
	}
	if err := env.Validate(); err != nil {
		return broker.Permanent(fmt.Errorf("mensagem inválida: %v", err))
	}
	if env.AppHdr.MsgDefIdr == iso20022.MsgPacs004 {
		return s.handleIncomingReturn(ctx, env)
	}
	if env.FIToFICstmrCdtTrf == nil {
		return broker.Permanent(errors.New("pacs.008 sem documento"))
	}
	doc := env.FIToFICstmrCdtTrf
	tx := doc.CdtTrfTxInf
	if tx.CdtrAgt.MmbId != s.ispb {
		return broker.Permanent(fmt.Errorf("pacs.008 destinada a %s chegou ao PSP %s", tx.CdtrAgt.MmbId, s.ispb))
	}
	e2e := tx.PmtId.EndToEndId
	trace.SpanFromContext(ctx).SetAttributes(attribute.String("pix.end_to_end_id", e2e))

	now := store.Now()
	creditor := Party{ISPB: s.ispb, Branch: tx.CdtrAcct.Issr, Number: tx.CdtrAcct.Id, Type: tx.CdtrAcct.Tp,
		Name: tx.Cdtr.Nm, TaxID: tx.Cdtr.Id, Key: tx.CdtrAcct.Prxy}
	p := Payment{
		EndToEndID:  e2e,
		Direction:   DirectionIn,
		Amount:      tx.IntrBkSttlmAmt.Value,
		Description: tx.RmtInf,
		Debtor: Party{ISPB: tx.DbtrAgt.MmbId, Branch: tx.DbtrAcct.Issr, Number: tx.DbtrAcct.Id, Type: tx.DbtrAcct.Tp,
			Name: tx.Dbtr.Nm, TaxID: tx.Dbtr.Id},
		MsgID:     doc.GrpHdr.MsgId,
		History:   []Transition{{State: StateReceived, At: now}},
		CreatedAt: now,
		UpdatedAt: now,
	}

	acc, err := s.store.FindAccount(ctx, tx.CdtrAcct.Issr, tx.CdtrAcct.Id)
	st := iso20022.TxInfAndSts{OrgnlEndToEndId: e2e, TxSts: iso20022.StatusAccepted, AccptncDtTm: &now}
	switch {
	case errors.Is(err, mongo.ErrNoDocuments):
		st = rejection(e2e, iso20022.ReasonInvalidAccount)
	case err != nil:
		return err
	case acc.Status != AccountActive:
		creditor.AccountID, creditor.Name = acc.ID, acc.Holder.Name
		st = rejection(e2e, iso20022.ReasonBlockedAccount)
	default:
		creditor.AccountID, creditor.Name, creditor.TaxID = acc.ID, acc.Holder.Name, acc.Holder.TaxID
	}
	p.Creditor = creditor
	if st.TxSts == iso20022.StatusAccepted {
		p.State, p.AcceptedAt = StateAccepted, &now
		p.History = append(p.History, Transition{State: StateAccepted, At: now})
	} else {
		p.State, p.Reason, p.FinishedAt = StateRejected, st.StsRsnInf.Rsn, &now
		p.History = append(p.History, Transition{State: StateRejected, At: now, Reason: p.Reason})
	}

	reply := iso20022.NewPacs002Envelope(s.ispb, pix.BCBISPB, doc.GrpHdr.MsgId, st, now)
	body, err := json.Marshal(reply)
	if err != nil {
		return err
	}
	err = store.Transact(ctx, s.store.client, func(ctx context.Context) error {
		if _, err := s.store.payments.InsertOne(ctx, p); err != nil {
			return err
		}
		return outbox.Add(ctx, s.store.outbox, broker.QueueSPIOut, reply.AppHdr.BizMsgIdr, body)
	})
	if mongo.IsDuplicateKeyError(err) {
		slog.InfoContext(ctx, "pacs.008 repetida ignorada", "end_to_end_id", e2e)
		return nil
	}
	if err != nil {
		return err
	}
	s.received.Add(ctx, 1, metric.WithAttributes(attribute.String("state", p.State), attribute.String("reason", p.Reason)))
	slog.InfoContext(ctx, "pix recebido", "end_to_end_id", e2e, "state", p.State, "reason", p.Reason,
		"debtor_ispb", p.Debtor.ISPB, "amount", p.Amount.String())
	return nil
}

func rejection(endToEndID, reason string) iso20022.TxInfAndSts {
	return iso20022.TxInfAndSts{OrgnlEndToEndId: endToEndID, TxSts: iso20022.StatusRejected,
		StsRsnInf: &iso20022.StsRsnInf{Rsn: reason}}
}

// FinishIncoming leva um Pix recebido de ACCEPTED a CREDITED (liquidado: a
// conta recebe o valor) ou a REJECTED (o SPI desistiu: nada a creditar). Na
// mesma transação, muda o estado, credita a conta e lança no razão. Aplicar de
// novo é inofensivo: o recebimento já não está em ACCEPTED.
func (s *Service) FinishIncoming(ctx context.Context, endToEndID, state, reason string, settledAt *time.Time) error {
	now := store.Now()
	set := bson.D{
		{Key: "state", Value: state},
		{Key: "finishedAt", Value: now},
		{Key: "updatedAt", Value: now},
	}
	if reason != "" {
		set = append(set, bson.E{Key: "reason", Value: reason})
	}
	if settledAt != nil {
		set = append(set, bson.E{Key: "settledAt", Value: settledAt.UTC()})
	}

	var p Payment
	err := store.Transact(ctx, s.store.client, func(ctx context.Context) error {
		err := s.store.coll(endToEndID).FindOneAndUpdate(ctx,
			bson.D{{Key: "_id", Value: endToEndID}, {Key: "direction", Value: DirectionIn}, {Key: "state", Value: StateAccepted}},
			bson.D{{Key: "$set", Value: set},
				{Key: "$push", Value: bson.D{{Key: "history", Value: Transition{State: state, At: now, Reason: reason}}}}},
			options.FindOneAndUpdate().SetReturnDocument(options.After)).Decode(&p)
		if errors.Is(err, mongo.ErrNoDocuments) {
			return errSkip
		}
		if err != nil {
			return err
		}
		if state == StateRejected {
			return s.releaseReturned(ctx, p) // devolução recebida e cancelada libera o Pix original
		}
		if state != StateCredited {
			return nil
		}
		if _, err := s.store.accounts.UpdateOne(ctx, bson.D{{Key: "_id", Value: p.Creditor.AccountID}},
			bson.D{{Key: "$inc", Value: bson.D{{Key: "balance.available", Value: p.Amount}}},
				{Key: "$set", Value: bson.D{{Key: "updatedAt", Value: now}}}}); err != nil {
			return err
		}
		_, err = s.store.ledger.InsertOne(ctx, LedgerEntry{ID: bson.NewObjectID(), AccountID: p.Creditor.AccountID,
			EndToEndID: endToEndID, Kind: EntryCredit, AvailableDelta: p.Amount, CreatedAt: now})
		return err
	})
	if errors.Is(err, errSkip) {
		slog.InfoContext(ctx, "resultado ignorado: recebimento já finalizado ou não aceito",
			"end_to_end_id", endToEndID, "state", state)
		return nil
	}
	if err != nil {
		return err
	}
	if pix.IsReturnID(endToEndID) {
		s.returnsFinished.Add(ctx, 1, metric.WithAttributes(attribute.String("direction", DirectionIn),
			attribute.String("state", state), attribute.String("reason", reason)))
	} else {
		s.received.Add(ctx, 1, metric.WithAttributes(attribute.String("state", state), attribute.String("reason", reason)))
	}
	slog.InfoContext(ctx, "recebimento finalizado", "end_to_end_id", endToEndID, "state", state, "reason", reason,
		"amount", p.Amount.String())
	return nil
}
