package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/brenokcunha/light-pix/internal/broker"
	"github.com/brenokcunha/light-pix/internal/config"
)

// runWorker sobe os consumidores de pix.results (fecha o Pix enviado) e de
// pix.incoming (responde ao Pix recebido).
func runWorker(ctx context.Context) error {
	return runRole(ctx, "pix-worker", func(ctx context.Context, r *role) (func(context.Context) error, error) {
		retryDelay, err := config.Duration("RETRY_DELAY", time.Second)
		if err != nil {
			return nil, err
		}
		concurrency, err := config.Float("WORKER_CONCURRENCY", 4)
		if err != nil {
			return nil, err
		}
		slog.InfoContext(ctx, "pix-worker iniciando", "ispb", r.ispb, "version", version)
		consume := func(queue string, h broker.Handler) func(context.Context) error {
			return func(ctx context.Context) error {
				return broker.Consume(ctx, r.rabbit, broker.ConsumeConfig{
					Queue: queue, Concurrency: int(concurrency), RetryDelay: retryDelay,
				}, h)
			}
		}
		return func(ctx context.Context) error {
			return runAll(ctx,
				consume(broker.QueueResults, r.svc.HandleResult),
				consume(broker.QueueIncoming, r.svc.HandleIncoming),
			)
		}, nil
	})
}
