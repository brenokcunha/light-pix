package broker

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
)

// HeaderEnqueuedAt guarda o instante (Unix ms) em que a mensagem foi criada,
// para medir a idade dela quando é consumida.
const HeaderEnqueuedAt = "x-enqueued-at"

// tableCarrier deixa o propagador do OTel ler e escrever headers AMQP.
type tableCarrier amqp.Table

func (c tableCarrier) Get(key string) string {
	v, _ := c[key].(string)
	return v
}
func (c tableCarrier) Set(key, value string) { c[key] = value }
func (c tableCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

// Message é o que se publica.
type Message struct {
	ID        string    // MessageId AMQP (ex.: identificador da mensagem ISO)
	Body      []byte    // JSON
	EnqueueAt time.Time // quando a mensagem nasceu; zero = agora
}

// Publisher publica com publisher confirms: Publish só volta depois que o
// broker confirmou a gravação (quorum). É seguro para uso concorrente, mas
// serializa as publicações.
type Publisher struct {
	mu sync.Mutex
	ch *amqp.Channel
}

// NewPublisher abre um canal em modo confirm.
func NewPublisher(conn *amqp.Connection) (*Publisher, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("broker: abrir canal: %w", err)
	}
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("broker: modo confirm: %w", err)
	}
	return &Publisher{ch: ch}, nil
}

// Close fecha o canal.
func (p *Publisher) Close() error { return p.ch.Close() }

// Publish envia m ao exchange com a chave de roteamento, propagando o
// contexto de trace de ctx nos headers (traceparent).
func (p *Publisher) Publish(ctx context.Context, exchange, key string, m Message) error {
	enqueued := m.EnqueueAt
	if enqueued.IsZero() {
		enqueued = time.Now()
	}
	headers := amqp.Table{HeaderEnqueuedAt: strconv.FormatInt(enqueued.UnixMilli(), 10)}
	otel.GetTextMapPropagator().Inject(ctx, tableCarrier(headers))

	p.mu.Lock()
	defer p.mu.Unlock()
	conf, err := p.ch.PublishWithDeferredConfirmWithContext(ctx, exchange, key, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		MessageId:    m.ID,
		Timestamp:    time.Now(),
		Headers:      headers,
		Body:         m.Body,
	})
	if err != nil {
		return fmt.Errorf("broker: publicar em %s: %w", key, err)
	}
	acked, err := conf.WaitContext(ctx)
	if err != nil {
		return fmt.Errorf("broker: confirmação de %s: %w", key, err)
	}
	if !acked {
		return errors.New("broker: publicação recusada (nack) pelo broker")
	}
	return nil
}
