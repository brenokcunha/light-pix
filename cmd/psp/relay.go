package main

import (
	"context"
	"log/slog"

	"github.com/brenokcunha/light-pix/internal/broker"
	"github.com/brenokcunha/light-pix/internal/outbox"
)

// runRelay sobe o outbox-relay: publica no RabbitMQ os eventos gravados no
// MongoDB junto com as mudanças de estado.
func runRelay(ctx context.Context) error {
	return runRole(ctx, "outbox-relay", func(ctx context.Context, r *role) (func(context.Context) error, error) {
		pub, err := broker.NewPublisher(r.rabbit)
		if err != nil {
			return nil, err
		}
		relay, err := outbox.NewRelay(ctx, r.db, pub, outbox.RelayConfig{Name: "outbox-relay"})
		if err != nil {
			return nil, err
		}
		slog.InfoContext(ctx, "outbox-relay iniciando", "ispb", r.ispb, "version", version)
		return func(ctx context.Context) error {
			defer pub.Close()
			return relay.Run(ctx)
		}, nil
	})
}
