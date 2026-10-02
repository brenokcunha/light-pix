package main

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/brenokcunha/light-pix/internal/broker"
	"github.com/brenokcunha/light-pix/internal/clients"
	"github.com/brenokcunha/light-pix/internal/config"
	"github.com/brenokcunha/light-pix/internal/psp"
)

// runConnector sobe o spi-connector: envia ao SPI o que está em spi.out e
// traz do SPI as mensagens destinadas ao PSP.
func runConnector(ctx context.Context) error {
	return runRole(ctx, "spi-connector", func(ctx context.Context, r *role) (func(context.Context) error, error) {
		spiURL, err := config.Required("SPI_URL")
		if err != nil {
			return nil, err
		}
		pollWait, err := config.Duration("SPI_POLL_WAIT", 20*time.Second)
		if err != nil {
			return nil, err
		}
		retryDelay, err := config.Duration("RETRY_DELAY", time.Second)
		if err != nil {
			return nil, err
		}
		concurrency, err := config.Float("WORKER_CONCURRENCY", 4)
		if err != nil {
			return nil, err
		}
		pub, err := broker.NewPublisher(r.rabbit)
		if err != nil {
			return nil, err
		}
		conn := psp.NewConnector(r.ispb, r.svc, clients.NewSPI(spiURL, 5*time.Second), pub, pollWait)
		slog.InfoContext(ctx, "spi-connector iniciando", "ispb", r.ispb, "version", version)

		return func(ctx context.Context) error {
			defer pub.Close()
			return runAll(ctx,
				func(ctx context.Context) error {
					return broker.Consume(ctx, r.rabbit, broker.ConsumeConfig{
						Queue: broker.QueueSPIOut, Concurrency: int(concurrency), RetryDelay: retryDelay,
					}, conn.HandleOut)
				},
				conn.Pull,
			)
		}, nil
	})
}

// runAll roda as funções em paralelo; quando uma termina, cancela as outras
// e devolve o primeiro erro.
func runAll(ctx context.Context, fns ...func(context.Context) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	errs := make([]error, len(fns))
	var wg sync.WaitGroup
	for i, fn := range fns {
		wg.Go(func() {
			errs[i] = fn(ctx)
			cancel()
		})
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}
