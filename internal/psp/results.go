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
	"github.com/brenokcunha/light-pix/internal/pix"
	"github.com/brenokcunha/light-pix/internal/store"
)

// errSkip aborta a transação de Finish quando não há o que fazer.
var errSkip = errors.New("nada a aplicar")

// HandleResult trata uma pacs.002 vinda do SPI (fila pix.results). Para o Pix
// que este PSP enviou, ACSC confirma e RJCT rejeita; para o que recebeu, ACSC
// credita a conta e RJCT (prazo estourado, saldo do pagador) cancela o
// recebimento.
//
// Uma resposta sobre um Pix recebido que ainda não foi registrado (a pacs.002
// correu na frente da pacs.008 entre as filas) volta à fila e é tentada de novo.
func (s *Service) HandleResult(ctx context.Context, d amqp.Delivery) error {
	var env iso20022.Envelope
	if err := json.Unmarshal(d.Body, &env); err != nil {
		return broker.Permanent(fmt.Errorf("pacs.002 ilegível: %w", err))
	}
	if err := env.Validate(); err != nil || env.FIToFIPmtStsRpt == nil {
		return broker.Permanent(fmt.Errorf("pacs.002 inválida: %v", err))
	}
	st := env.FIToFIPmtStsRpt.TxInfAndSts
	e2e := st.OrgnlEndToEndId
	trace.SpanFromContext(ctx).SetAttributes(
		attribute.String("pix.end_to_end_id", e2e),
		attribute.String("pix.tx_status", st.TxSts))

	var state, reason string
	var settledAt *time.Time
	switch st.TxSts {
	case iso20022.StatusSettled:
		state, settledAt = StateConfirmed, st.FctvIntrBkSttlmDt
	case iso20022.StatusRejected:
		state, reason = StateRejected, st.StsRsnInf.Rsn
	default:
		return nil // ACSP não muda o estado de ninguém
	}

	p, err := s.store.FindTx(ctx, e2e)
	switch {
	case errors.Is(err, mongo.ErrNoDocuments):
		if origin, _, _ := pix.ParseTransactionID(e2e); origin == s.ispb {
			slog.WarnContext(ctx, "resultado de Pix enviado que não existe neste PSP; ignorado", "end_to_end_id", e2e)
			return nil
		}
		return fmt.Errorf("pix recebido %s ainda não registrado", e2e)
	case err != nil:
		return err
	case p.Direction == DirectionIn:
		if state == StateConfirmed {
			state = StateCredited
		}
		return s.FinishIncoming(ctx, e2e, state, reason, settledAt)
	default:
		return s.Finish(ctx, e2e, state, reason, settledAt)
	}
}

// Finish leva um Pix enviado a CONFIRMED ou REJECTED. Na mesma transação,
// muda o estado do pagamento (condicionado a ainda estar em PENDING ou SENT),
// baixa ou devolve a reserva da conta e lança no razão. Aplicar de novo é
// inofensivo: o pagamento já não está em estado não terminal.
func (s *Service) Finish(ctx context.Context, endToEndID, state, reason string, settledAt *time.Time) error {
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
			bson.D{{Key: "_id", Value: endToEndID}, {Key: "direction", Value: DirectionOut},
				{Key: "state", Value: bson.D{{Key: "$in", Value: bson.A{StatePending, StateSent}}}}},
			bson.D{{Key: "$set", Value: set},
				{Key: "$push", Value: bson.D{{Key: "history", Value: Transition{State: state, At: now, Reason: reason}}}}},
			options.FindOneAndUpdate().SetReturnDocument(options.After)).Decode(&p)
		if errors.Is(err, mongo.ErrNoDocuments) {
			return errSkip
		}
		if err != nil {
			return err
		}

		// Devolução rejeitada libera o valor que reservava do Pix original.
		if state == StateRejected {
			if err := s.releaseReturned(ctx, p); err != nil {
				return err
			}
		}
		// Confirmar baixa a reserva; rejeitar a devolve ao saldo disponível.
		entry := LedgerEntry{ID: bson.NewObjectID(), AccountID: p.Debtor.AccountID, EndToEndID: endToEndID,
			Kind: EntryConfirm, HeldDelta: -p.Amount, CreatedAt: now}
		if state == StateRejected {
			entry.Kind, entry.AvailableDelta = EntryRelease, p.Amount
		}
		if _, err := s.store.accounts.UpdateOne(ctx, bson.D{{Key: "_id", Value: p.Debtor.AccountID}},
			bson.D{{Key: "$inc", Value: bson.D{
				{Key: "balance.available", Value: entry.AvailableDelta},
				{Key: "balance.held", Value: entry.HeldDelta}}},
				{Key: "$set", Value: bson.D{{Key: "updatedAt", Value: now}}}}); err != nil {
			return err
		}
		_, err = s.store.ledger.InsertOne(ctx, entry)
		return err
	})
	if errors.Is(err, errSkip) {
		slog.InfoContext(ctx, "resultado ignorado: não há Pix enviado em andamento com esse EndToEndId",
			"end_to_end_id", endToEndID, "state", state)
		return nil
	}
	if err != nil {
		return err
	}

	if pix.IsReturnID(endToEndID) {
		s.returnsFinished.Add(ctx, 1, metric.WithAttributes(attribute.String("direction", DirectionOut),
			attribute.String("state", state), attribute.String("reason", reason)))
		slog.InfoContext(ctx, "devolução finalizada", "return_id", endToEndID, "original", p.ReturnOf, "state", state, "reason", reason)
		return nil
	}
	attrs := metric.WithAttributes(attribute.String("state", state))
	s.finished.Add(ctx, 1, metric.WithAttributes(attribute.String("state", state), attribute.String("reason", reason)))
	s.duration.Record(ctx, now.Sub(p.CreatedAt).Seconds(), attrs)
	slog.InfoContext(ctx, "pix finalizado", "end_to_end_id", endToEndID, "state", state, "reason", reason,
		"duration_ms", now.Sub(p.CreatedAt).Milliseconds())
	return nil
}

// MarkSent registra que o SPI aceitou a pacs.008. Só avança de PENDING: se o
// resultado já chegou, o estado final prevalece.
func (s *Service) MarkSent(ctx context.Context, endToEndID string) error {
	now := store.Now()
	_, err := s.store.coll(endToEndID).UpdateOne(ctx,
		bson.D{{Key: "_id", Value: endToEndID}, {Key: "state", Value: StatePending}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "state", Value: StateSent}, {Key: "sentAt", Value: now}, {Key: "updatedAt", Value: now}}},
			{Key: "$push", Value: bson.D{{Key: "history", Value: Transition{State: StateSent, At: now}}}}})
	return err
}
