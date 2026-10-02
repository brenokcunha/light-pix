package broker

import (
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Nomes da topologia de cada PSP (um vhost por PSP).
const (
	ExchangePix = "pix"     // topic: roteia pelo nome da fila
	ExchangeDLX = "pix.dlx" // direct: recebe o que vai para a DLQ

	QueueSPIOut   = "spi.out"      // mensagens do PSP para o SPI (pacs.008, pacs.002)
	QueueResults  = "pix.results"  // pacs.002 vindas do SPI
	QueueIncoming = "pix.incoming" // pacs.008 vindas do SPI

	// DeliveryLimit é uma rede de segurança: quantas vezes uma mensagem pode
	// voltar à fila (nack com requeue, só em falha do próprio retry) antes de
	// a quorum queue mandá-la à DLQ.
	DeliveryLimit = 10
)

// DefaultRetryDelays são as esperas entre as tentativas de uma mensagem que
// falhou: 5s, 30s e 2min. Esgotadas, a mensagem vai para a DLQ.
var DefaultRetryDelays = []time.Duration{5 * time.Second, 30 * time.Second, 2 * time.Minute}

// Queues lista as filas de trabalho; cada uma tem a DLQ "<fila>.dlq" e as
// filas de retry "<fila>.retry.<espera>".
var Queues = []string{QueueSPIOut, QueueResults, QueueIncoming}

// DLQ devolve o nome da fila de mensagens mortas de queue.
func DLQ(queue string) string { return queue + ".dlq" }

// RetryQueue devolve a fila de espera de queue para o atraso d (ex.:
// "pix.results.retry.30s"). A mensagem fica nela até o TTL vencer e então
// volta sozinha para queue.
func RetryQueue(queue string, d time.Duration) string {
	return queue + ".retry." + delayLabel(d)
}

func delayLabel(d time.Duration) string {
	switch {
	case d%time.Minute == 0:
		return fmt.Sprintf("%dm", d/time.Minute)
	case d%time.Second == 0:
		return fmt.Sprintf("%ds", d/time.Second)
	default:
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
}

// DeclareTopology declara exchanges, filas e bindings. É idempotente, então
// cada processo pode chamá-la ao subir, em qualquer ordem. delays define as
// filas de retry; vazio usa DefaultRetryDelays. Todos os processos de um
// mesmo vhost devem usar os mesmos delays.
func DeclareTopology(ch *amqp.Channel, delays []time.Duration) error {
	if len(delays) == 0 {
		delays = DefaultRetryDelays
	}
	if err := ch.ExchangeDeclare(ExchangePix, "topic", true, false, false, false, nil); err != nil {
		return fmt.Errorf("broker: exchange %s: %w", ExchangePix, err)
	}
	if err := ch.ExchangeDeclare(ExchangeDLX, "direct", true, false, false, false, nil); err != nil {
		return fmt.Errorf("broker: exchange %s: %w", ExchangeDLX, err)
	}
	for _, q := range Queues {
		if _, err := ch.QueueDeclare(DLQ(q), true, false, false, false, amqp.Table{"x-queue-type": "quorum"}); err != nil {
			return fmt.Errorf("broker: fila %s: %w", DLQ(q), err)
		}
		if err := ch.QueueBind(DLQ(q), DLQ(q), ExchangeDLX, false, nil); err != nil {
			return fmt.Errorf("broker: bind %s: %w", DLQ(q), err)
		}
		if _, err := ch.QueueDeclare(q, true, false, false, false, amqp.Table{
			"x-queue-type":              "quorum",
			"x-delivery-limit":          DeliveryLimit,
			"x-dead-letter-exchange":    ExchangeDLX,
			"x-dead-letter-routing-key": DLQ(q),
		}); err != nil {
			return fmt.Errorf("broker: fila %s: %w", q, err)
		}
		if err := ch.QueueBind(q, q, ExchangePix, false, nil); err != nil {
			return fmt.Errorf("broker: bind %s: %w", q, err)
		}
		// Fila de espera: sem consumidor; ao vencer o TTL a mensagem é
		// dead-lettered para o exchange pix com a chave da fila de origem.
		for _, d := range delays {
			if _, err := ch.QueueDeclare(RetryQueue(q, d), true, false, false, false, amqp.Table{
				"x-queue-type":              "quorum",
				"x-message-ttl":             d.Milliseconds(),
				"x-dead-letter-exchange":    ExchangePix,
				"x-dead-letter-routing-key": q,
			}); err != nil {
				return fmt.Errorf("broker: fila %s: %w", RetryQueue(q, d), err)
			}
		}
	}
	return nil
}

// Declare abre um canal só para declarar a topologia (ver DeclareTopology).
func Declare(conn *amqp.Connection, delays ...time.Duration) error {
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("broker: abrir canal: %w", err)
	}
	defer ch.Close()
	return DeclareTopology(ch, delays)
}
