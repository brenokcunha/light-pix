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

// Headers de controle do retry e da DLQ.
const (
	HeaderAttempt     = "x-attempt"      // quantas vezes a mensagem já falhou
	HeaderLastError   = "x-last-error"   // erro da última falha
	HeaderSourceQueue = "x-source-queue" // fila de onde a mensagem veio, na DLQ
	HeaderRedrive     = "x-redrive"      // quantas vezes saiu da DLQ de volta à fila
)

// ExtractContext devolve ctx com o contexto de trace que veio nos headers.
func ExtractContext(ctx context.Context, headers amqp.Table) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, tableCarrier(headers))
}

// HeaderInt lê um header numérico (0 se ausente), seja qual for o tipo
// inteiro com que o broker o devolveu.
func HeaderInt(headers amqp.Table, key string) int {
	n, _ := strconv.Atoi(fmt.Sprint(headers[key]))
	return n
}

// Message é o que se publica.
type Message struct {
	ID        string     // MessageId AMQP (ex.: identificador da mensagem ISO)
	Body      []byte     // JSON
	EnqueueAt time.Time  // quando a mensagem nasceu; zero = agora
	Headers   amqp.Table // headers adicionais (controle de retry e DLQ)
}

// Publisher publica com publisher confirms: Publish só volta depois que o
// broker confirmou a gravação (quorum). É seguro para uso concorrente; o
// envio de cada mensagem é serializado, a espera pelo confirm não.
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

// Confirmation é a confirmação pendente de uma mensagem publicada com
// PublishAsync.
type Confirmation struct {
	key  string
	conf *amqp.DeferredConfirmation
}

// Wait espera o broker confirmar (ou recusar) a mensagem.
func (c *Confirmation) Wait(ctx context.Context) error {
	acked, err := c.conf.WaitContext(ctx)
	if err != nil {
		return fmt.Errorf("broker: confirmação de %s: %w", c.key, err)
	}
	if !acked {
		return errors.New("broker: publicação recusada (nack) pelo broker")
	}
	return nil
}

// Publish envia m ao exchange com a chave de roteamento e só volta depois do
// confirm do broker. O contexto de trace de ctx segue nos headers
// (traceparent). Com exchange vazio, a mensagem vai direto para a fila de nome
// igual à chave.
func (p *Publisher) Publish(ctx context.Context, exchange, key string, m Message) error {
	c, err := p.PublishAsync(ctx, exchange, key, m)
	if err != nil {
		return err
	}
	return c.Wait(ctx)
}

// PublishAsync envia m sem esperar o confirm: várias mensagens podem estar em
// voo ao mesmo tempo e o chamador espera cada Confirmation depois. É o que dá
// vazão ao relay, em vez de uma ida e volta ao broker por mensagem.
func (p *Publisher) PublishAsync(ctx context.Context, exchange, key string, m Message) (*Confirmation, error) {
	enqueued := m.EnqueueAt
	if enqueued.IsZero() {
		enqueued = time.Now()
	}
	headers := amqp.Table{}
	for k, v := range m.Headers {
		headers[k] = v
	}
	headers[HeaderEnqueuedAt] = strconv.FormatInt(enqueued.UnixMilli(), 10)
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
		return nil, fmt.Errorf("broker: publicar em %s: %w", key, err)
	}
	return &Confirmation{key: key, conf: conf}, nil
}
