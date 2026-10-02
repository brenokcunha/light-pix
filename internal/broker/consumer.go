package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// ErrPermanent marca um erro em que reprocessar não adianta (mensagem
// malformada, por exemplo): a mensagem vai direto para a DLQ.
var ErrPermanent = errors.New("erro permanente")

// Permanent embrulha err como permanente.
func Permanent(err error) error { return fmt.Errorf("%w: %w", ErrPermanent, err) }

// Handler processa uma mensagem. Nil confirma (ack); ErrPermanent rejeita
// para a DLQ; qualquer outro erro devolve a mensagem à fila.
type Handler func(ctx context.Context, d amqp.Delivery) error

// ConsumeConfig ajusta um consumidor.
type ConsumeConfig struct {
	Queue       string
	Concurrency int           // mensagens em processamento ao mesmo tempo
	RetryDelay  time.Duration // espera antes de devolver uma mensagem que falhou
}

type consumerMetrics struct {
	processed metric.Int64Counter
	age       metric.Float64Histogram
}

func newConsumerMetrics() (consumerMetrics, error) {
	m := otel.Meter("github.com/brenokcunha/light-pix/internal/broker")
	processed, err := m.Int64Counter("broker.messages.processed",
		metric.WithDescription("Mensagens consumidas, por fila e resultado"))
	if err != nil {
		return consumerMetrics{}, err
	}
	age, err := m.Float64Histogram("broker.message.age", metric.WithUnit("s"),
		metric.WithDescription("Idade da mensagem ao ser consumida (desde x-enqueued-at)"))
	return consumerMetrics{processed: processed, age: age}, err
}

// Consume lê a fila até o contexto terminar, com ack manual e prefetch igual
// à concorrência. Cada mensagem roda num span de consumidor, filho do
// contexto de trace que veio nos headers. Devolve erro se o broker fechar o
// canal; nesse caso o processo deve sair e ser reiniciado.
//
// Falha transitória: espera RetryDelay e devolve a mensagem à fila (nack com
// requeue); depois de DeliveryLimit entregas a fila a manda para a DLQ.
func Consume(ctx context.Context, conn *amqp.Connection, cfg ConsumeConfig, h Handler) error {
	met, err := newConsumerMetrics()
	if err != nil {
		return err
	}
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("broker: abrir canal: %w", err)
	}
	defer ch.Close()
	concurrency := max(cfg.Concurrency, 1)
	if err := ch.Qos(concurrency, 0, false); err != nil {
		return fmt.Errorf("broker: qos: %w", err)
	}
	closed := ch.NotifyClose(make(chan *amqp.Error, 1))
	tag := fmt.Sprintf("%s-%d", cfg.Queue, time.Now().UnixNano())
	deliveries, err := ch.Consume(cfg.Queue, tag, false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("broker: consumir %s: %w", cfg.Queue, err)
	}

	var wg sync.WaitGroup
	for range concurrency {
		wg.Go(func() {
			for d := range deliveries {
				process(ctx, cfg, met, h, d)
			}
		})
	}

	select {
	case <-ctx.Done():
		// Para de receber e deixa as mensagens em andamento terminarem.
		_ = ch.Cancel(tag, false)
		wg.Wait()
		return nil
	case <-closed:
		wg.Wait()
		return fmt.Errorf("broker: canal de %s fechado", cfg.Queue)
	}
}

func process(ctx context.Context, cfg ConsumeConfig, met consumerMetrics, h Handler, d amqp.Delivery) {
	// O processamento termina mesmo que o processo esteja sendo desligado:
	// só a espera do retry respeita o cancelamento.
	pctx := otel.GetTextMapPropagator().Extract(context.WithoutCancel(ctx), tableCarrier(d.Headers))
	pctx, span := otel.Tracer("github.com/brenokcunha/light-pix/internal/broker").Start(pctx, "process "+cfg.Queue,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "rabbitmq"),
			attribute.String("messaging.destination.name", cfg.Queue),
			attribute.String("messaging.operation.type", "process"),
			attribute.String("messaging.message.id", d.MessageId),
		))
	defer span.End()

	if ms, err := strconv.ParseInt(fmt.Sprint(d.Headers[HeaderEnqueuedAt]), 10, 64); err == nil {
		met.age.Record(pctx, time.Since(time.UnixMilli(ms)).Seconds(),
			metric.WithAttributes(attribute.String("queue", cfg.Queue)))
	}

	err := h(pctx, d)
	result := "ok"
	switch {
	case err == nil:
		_ = d.Ack(false)
	case errors.Is(err, ErrPermanent):
		result = "dead_letter"
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		slog.ErrorContext(pctx, "mensagem rejeitada para a DLQ", "queue", cfg.Queue, "message_id", d.MessageId, "err", err)
		_ = d.Reject(false)
	default:
		result = "retry"
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		slog.WarnContext(pctx, "falha ao processar mensagem; devolvendo à fila", "queue", cfg.Queue,
			"message_id", d.MessageId, "redelivered", d.Redelivered, "err", err)
		select {
		case <-ctx.Done():
		case <-time.After(cfg.RetryDelay):
		}
		_ = d.Nack(false, true)
	}
	met.processed.Add(pctx, 1, metric.WithAttributes(
		attribute.String("queue", cfg.Queue), attribute.String("result", result)))
}
