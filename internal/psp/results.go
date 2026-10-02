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
	"github.com/brenokcunha/light-pix/internal/store"
)

// errSkip aborta a transação de Finish quando não há o que fazer.
var errSkip = errors.New("nada a aplicar")

// HandleResult trata uma pacs.002 vinda do SPI (fila pix.results). ACSC
// confirma o Pix enviado; RJCT o rejeita. Mensagens sobre transações que este
// PSP não enviou são ignoradas (o lado recebedor é tratado à parte).
func (s *Service) HandleResult(ctx context.Context, d amqp.Delivery) error {
	var env iso20022.Envelope
	if err := json.Unmarshal(d.Body, &env); err != nil {
		return broker.Permanent(fmt.Errorf("pacs.002 ilegível: %w", err))
	}
	if err := env.Validate(); err != nil || env.FIToFIPmtStsRpt == nil {
		return broker.Permanent(fmt.Errorf("pacs.002 inválida: %v", err))
	}
	st := env.FIToFIPmtStsRpt.TxInfAndSts
	trace.SpanFromContext(ctx).SetAttributes(
		attribute.String("pix.end_to_end_id", st.OrgnlEndToEndId),
		attribute.String("pix.tx_status", st.TxSts))

	switch st.TxSts {
	case iso20022.StatusSettled:
		return s.Finish(ctx, st.OrgnlEndToEndId, StateConfirmed, "", st.FctvIntrBkSttlmDt)
	case iso20022.StatusRejected:
		return s.Finish(ctx, st.OrgnlEndToEndId, StateRejected, st.StsRsnInf.Rsn, nil)
	default:
		return nil // ACSP não muda o estado do pagador
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
		err := s.store.payments.FindOneAndUpdate(ctx,
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
	_, err := s.store.payments.UpdateOne(ctx,
		bson.D{{Key: "_id", Value: endToEndID}, {Key: "state", Value: StatePending}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "state", Value: StateSent}, {Key: "sentAt", Value: now}, {Key: "updatedAt", Value: now}}},
			{Key: "$push", Value: bson.D{{Key: "history", Value: Transition{State: StateSent, At: now}}}}})
	return err
}
