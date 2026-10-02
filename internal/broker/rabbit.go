// Package broker concentra o acesso ao RabbitMQ.
package broker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Dial conecta ao RabbitMQ tentando de novo até o contexto expirar, para
// tolerar a ordem de subida dos containers.
func Dial(ctx context.Context, url string) (*amqp.Connection, error) {
	backoff := 500 * time.Millisecond
	for {
		conn, err := amqp.Dial(url)
		if err == nil {
			return conn, nil
		}
		slog.WarnContext(ctx, "rabbitmq indisponível, tentando de novo", "err", err, "retry_in", backoff)
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("broker: dial: %w", err)
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 5*time.Second)
	}
}
