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

// Handler processa uma mensagem. Nil confirma (ack); ErrPermanent manda a
// mensagem direto para a DLQ; qualquer outro erro a manda para a próxima fila
// de retry (ou para a DLQ, se as tentativas acabaram).
type Handler func(ctx context.Context, d amqp.Delivery) error

// ConsumeConfig ajusta um consumidor.
type ConsumeConfig struct {
	Queue       string
	Concurrency int // mensagens em processamento ao mesmo tempo
	// RetryDelays são as esperas entre as tentativas; vazio usa
	// DefaultRetryDelays. Devem ser as mesmas declaradas na topologia.
	RetryDelays []time.Duration
}

type consumerMetrics struct {
	processed metric.Int64Counter
	age       metric.Float64Histogram
}

func newConsumerMetrics() (consumerMetrics, error) {
	m := otel.Meter("github.com/brenokcunha/light-pix/internal/broker")
	processed, err := m.Int64Counter("broker.messages.processed",
		metric.WithDescription("Mensagens consumidas, por fila e resultado (ok, retry, dead_letter)"))
	if err != nil {
		return consumerMetrics{}, err
	}
	age, err := m.Float64Histogram("broker.message.age", metric.WithUnit("s"),
		metric.WithDescription("Idade da mensagem ao ser consumida (desde x-enqueued-at)"))
	return consumerMetrics{processed: processed, age: age}, err
}

type consumer struct {
	cfg    ConsumeConfig
	delays []time.Duration
	met    consumerMetrics
	pub    *Publisher
	h      Handler
}

// Consume lê a fila até o contexto terminar, com ack manual e prefetch igual
// à concorrência. Cada mensagem roda num span de consumidor, filho do
// contexto de trace que veio nos headers. Devolve erro se o broker fechar o
// canal; nesse caso o processo deve sair e ser reiniciado.
//
// Falha transitória: a mensagem é republicada (com confirm) na fila de espera
// da tentativa seguinte (por padrão 5s, 30s, 2min) e confirmada na original;
// quando o TTL vence, o broker a devolve à fila de origem com x-attempt
// incrementado. Esgotadas as tentativas, ou em erro permanente, vai para a DLQ
// com o último erro no header x-last-error. Se nem a republicação funcionar, a
// mensagem volta à fila (nack com requeue) e a quorum queue limita as entregas.
func Consume(ctx context.Context, conn *amqp.Connection, cfg ConsumeConfig, h Handler) error {
	met, err := newConsumerMetrics()
	if err != nil {
		return err
	}
	pub, err := NewPublisher(conn)
	if err != nil {
		return err
	}
	defer pub.Close()
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

	c := &consumer{cfg: cfg, delays: cfg.RetryDelays, met: met, pub: pub, h: h}
	if len(c.delays) == 0 {
		c.delays = DefaultRetryDelays
	}
	var wg sync.WaitGroup
	for range concurrency {
		wg.Go(func() {
			for d := range deliveries {
				c.process(ctx, d)
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

func (c *consumer) process(ctx context.Context, d amqp.Delivery) {
	// O processamento termina mesmo que o processo esteja sendo desligado.
	pctx := ExtractContext(context.WithoutCancel(ctx), d.Headers)
	attempt := HeaderInt(d.Headers, HeaderAttempt)
	pctx, span := otel.Tracer("github.com/brenokcunha/light-pix/internal/broker").Start(pctx, "process "+c.cfg.Queue,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "rabbitmq"),
			attribute.String("messaging.destination.name", c.cfg.Queue),
			attribute.String("messaging.operation.type", "process"),
			attribute.String("messaging.message.id", d.MessageId),
			attribute.Int("messaging.retry.attempt", attempt),
		))
	defer span.End()

	if ms, err := strconv.ParseInt(fmt.Sprint(d.Headers[HeaderEnqueuedAt]), 10, 64); err == nil {
		c.met.age.Record(pctx, time.Since(time.UnixMilli(ms)).Seconds(),
			metric.WithAttributes(attribute.String("queue", c.cfg.Queue)))
	}

	err := c.h(pctx, d)
	result := "ok"
	switch {
	case err == nil:
		_ = d.Ack(false)
	default:
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		result = c.fail(pctx, d, attempt, err)
	}
	c.met.processed.Add(pctx, 1, metric.WithAttributes(
		attribute.String("queue", c.cfg.Queue), attribute.String("result", result)))
}

// fail encaminha a mensagem que falhou e devolve o resultado para a métrica.
func (c *consumer) fail(ctx context.Context, d amqp.Delivery, attempt int, cause error) string {
	headers := amqp.Table{}
	for _, k := range []string{HeaderRedrive} {
		if v, ok := d.Headers[k]; ok {
			headers[k] = v
		}
	}
	headers[HeaderLastError] = truncate(cause.Error(), 500)

	exchange, key, result := "", "", "retry"
	switch {
	case errors.Is(cause, ErrPermanent) || attempt >= len(c.delays):
		headers[HeaderAttempt] = attempt + 1
		headers[HeaderSourceQueue] = c.cfg.Queue
		exchange, key, result = ExchangeDLX, DLQ(c.cfg.Queue), "dead_letter"
		slog.ErrorContext(ctx, "mensagem enviada para a DLQ", "queue", c.cfg.Queue, "message_id", d.MessageId,
			"attempts", attempt+1, "err", cause)
	default:
		headers[HeaderAttempt] = attempt + 1
		key = RetryQueue(c.cfg.Queue, c.delays[attempt])
		slog.WarnContext(ctx, "falha ao processar mensagem; tentará de novo", "queue", c.cfg.Queue,
			"message_id", d.MessageId, "attempt", attempt+1, "retry_in", c.delays[attempt].String(), "err", cause)
	}

	var enqueued time.Time
	if ms, err := strconv.ParseInt(fmt.Sprint(d.Headers[HeaderEnqueuedAt]), 10, 64); err == nil {
		enqueued = time.UnixMilli(ms)
	}
	if err := c.pub.Publish(ctx, exchange, key, Message{ID: d.MessageId, Body: d.Body, EnqueueAt: enqueued, Headers: headers}); err != nil {
		slog.ErrorContext(ctx, "falha ao republicar mensagem; devolvendo à fila", "queue", c.cfg.Queue, "err", err)
		_ = d.Nack(false, true)
		return "requeue"
	}
	_ = d.Ack(false)
	return result
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
