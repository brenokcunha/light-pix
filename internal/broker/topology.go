package broker

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Nomes da topologia de cada PSP (um vhost por PSP).
const (
	ExchangePix = "pix"     // topic: roteia pelo nome da fila
	ExchangeDLX = "pix.dlx" // direct: recebe o que a fila rejeita

	QueueSPIOut   = "spi.out"      // mensagens do PSP para o SPI (pacs.008, pacs.002)
	QueueResults  = "pix.results"  // pacs.002 vindas do SPI
	QueueIncoming = "pix.incoming" // pacs.008 vindas do SPI

	// DeliveryLimit é quantas vezes uma mensagem volta à fila (nack com
	// requeue) antes de a quorum queue mandá-la à DLQ.
	DeliveryLimit = 10
)

// Queues lista as filas de trabalho; cada uma tem a DLQ "<fila>.dlq".
var Queues = []string{QueueSPIOut, QueueResults, QueueIncoming}

// DLQ devolve o nome da fila de mensagens mortas de queue.
func DLQ(queue string) string { return queue + ".dlq" }

// DeclareTopology declara exchanges, filas e bindings. É idempotente, então
// cada processo pode chamá-la ao subir, em qualquer ordem.
func DeclareTopology(ch *amqp.Channel) error {
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
	}
	return nil
}

// Declare abre um canal só para declarar a topologia.
func Declare(conn *amqp.Connection) error {
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("broker: abrir canal: %w", err)
	}
	defer ch.Close()
	return DeclareTopology(ch)
}
