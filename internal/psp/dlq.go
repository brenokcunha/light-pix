package psp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/brenokcunha/light-pix/internal/broker"
	"github.com/brenokcunha/light-pix/internal/clients"
	"github.com/brenokcunha/light-pix/internal/iso20022"
	"github.com/brenokcunha/light-pix/internal/store"
)

// dlqBatch limita as mensagens tratadas por fila a cada ciclo.
const dlqBatch = 100

// DeadLetter é uma mensagem que esgotou os reenvios da DLQ e foi estacionada
// para análise manual.
type DeadLetter struct {
	ID        bson.ObjectID     `bson:"_id" json:"id"`
	Queue     string            `bson:"queue" json:"queue"`
	MessageID string            `bson:"messageId" json:"messageId"`
	Body      string            `bson:"body" json:"body"`
	Headers   map[string]string `bson:"headers" json:"headers"` // último erro, tentativas, reenvios
	ParkedAt  time.Time         `bson:"parkedAt" json:"parkedAt"`
}

// RedriveDLQs devolve as mensagens das DLQs à fila de origem. Isso é seguro
// porque todos os consumidores são idempotentes: o que já foi aplicado não se
// aplica de novo. Cada mensagem pode sair da DLQ até MaxRedrives vezes
// (contador no header x-redrive); depois disso é estacionada na coleção
// dead_letters, e a DLQ não acumula mensagens que nunca vão passar.
func (r *Resolver) RedriveDLQs(ctx context.Context) error {
	ch, err := r.conn.Channel()
	if err != nil {
		return fmt.Errorf("abrir canal: %w", err)
	}
	defer ch.Close()
	pub, err := broker.NewPublisher(r.conn)
	if err != nil {
		return err
	}
	defer pub.Close()

	var errs []error
	for _, q := range broker.Queues {
		if err := r.redriveQueue(ctx, ch, pub, q); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", broker.DLQ(q), err))
		}
	}
	return errors.Join(errs...)
}

func (r *Resolver) redriveQueue(ctx context.Context, ch *amqp.Channel, pub *broker.Publisher, queue string) error {
	for range dlqBatch {
		d, ok, err := ch.Get(broker.DLQ(queue), false)
		if err != nil || !ok {
			return err
		}
		if err := r.redriveOne(ctx, pub, queue, d); err != nil {
			_ = d.Nack(false, true) // fica na DLQ para o próximo ciclo
			return err
		}
		_ = d.Ack(false)
	}
	return nil
}

func (r *Resolver) redriveOne(ctx context.Context, pub *broker.Publisher, queue string, d amqp.Delivery) error {
	ctx = broker.ExtractContext(ctx, d.Headers)
	ctx, span := r.tracer.Start(ctx, "resolver.redrive "+queue)
	defer span.End()
	span.SetAttributes(attribute.String("messaging.message.id", d.MessageId))

	redrives := broker.HeaderInt(d.Headers, broker.HeaderRedrive)
	if redrives >= r.cfg.MaxRedrives {
		if err := r.park(ctx, queue, d); err != nil {
			return err
		}
		r.parked.Add(ctx, 1, metric.WithAttributes(attribute.String("queue", queue)))
		return nil
	}
	var enqueued time.Time
	if ms, err := strconv.ParseInt(fmt.Sprint(d.Headers[broker.HeaderEnqueuedAt]), 10, 64); err == nil {
		enqueued = time.UnixMilli(ms)
	}
	// x-attempt recomeça do zero: a mensagem ganha um novo ciclo de retries.
	err := pub.Publish(ctx, broker.ExchangePix, queue, broker.Message{
		ID: d.MessageId, Body: d.Body, EnqueueAt: enqueued,
		Headers: amqp.Table{broker.HeaderRedrive: redrives + 1},
	})
	if err != nil {
		return err
	}
	slog.InfoContext(ctx, "mensagem devolvida da DLQ", "queue", queue, "message_id", d.MessageId, "redrive", redrives+1)
	r.redriven.Add(ctx, 1, metric.WithAttributes(attribute.String("queue", queue)))
	return nil
}

// park estaciona a mensagem em dead_letters. Se for a pacs.008 de um Pix
// enviado que nunca chegou ao SPI, resolve o pagamento antes: sem a mensagem,
// ele não vai mais sair, então é seguro devolver a reserva ao pagador.
func (r *Resolver) park(ctx context.Context, queue string, d amqp.Delivery) error {
	if queue == broker.QueueSPIOut {
		if err := r.settleUnsent(ctx, d.Body); err != nil {
			return err
		}
	}
	headers := map[string]string{}
	for _, k := range []string{broker.HeaderLastError, broker.HeaderSourceQueue, broker.HeaderAttempt, broker.HeaderRedrive} {
		if v, ok := d.Headers[k]; ok {
			headers[k] = fmt.Sprint(v)
		}
	}
	_, err := r.svc.store.deadLetters.InsertOne(ctx, DeadLetter{
		ID: bson.NewObjectID(), Queue: queue, MessageID: d.MessageId, Body: string(d.Body),
		Headers: headers, ParkedAt: store.Now(),
	})
	if err == nil {
		slog.ErrorContext(ctx, "mensagem estacionada em dead_letters", "queue", queue, "message_id", d.MessageId,
			"last_error", headers[broker.HeaderLastError])
	}
	return err
}

// settleUnsent resolve o Pix enviado cuja pacs.008 está sendo estacionada.
// Pergunta ao SPI: se ele nunca ouviu falar da transação, ela é rejeitada
// (ED05) e a reserva volta; se já existe, vale a situação dele.
func (r *Resolver) settleUnsent(ctx context.Context, body []byte) error {
	var env iso20022.Envelope
	if err := json.Unmarshal(body, &env); err != nil {
		return nil
	}
	e2e := originatedID(env)
	if e2e == "" {
		return nil // não inicia Pix nem devolução (é uma pacs.002): só estaciona
	}
	st, err := r.spi.Status(ctx, r.svc.ispb, e2e)
	var apiErr *clients.APIError
	switch {
	case errors.As(err, &apiErr) && apiErr.Status == 404:
		return r.svc.Finish(ctx, e2e, StateRejected, iso20022.ReasonSettlementFailed, nil)
	case err != nil:
		return err // sem resposta do SPI não dá para decidir: tenta no próximo ciclo
	}
	_, err = r.apply(ctx, DirectionOut, e2e, st)
	return err
}
