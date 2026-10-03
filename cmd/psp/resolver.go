package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/brenokcunha/light-pix/internal/clients"
	"github.com/brenokcunha/light-pix/internal/config"
	"github.com/brenokcunha/light-pix/internal/psp"
)

// runResolver sobe o pending-resolver: resolve pagamentos parados consultando
// o SPI, reprocessa as DLQs e faz a reconciliação diária da conta PI.
func runResolver(ctx context.Context) error {
	return runRole(ctx, "pending-resolver", func(ctx context.Context, r *role) (func(context.Context) error, error) {
		spiURL, err := config.Required("SPI_URL")
		if err != nil {
			return nil, err
		}
		bacenURL, err := config.Required("BACEN_URL")
		if err != nil {
			return nil, err
		}
		var cfg psp.ResolverConfig
		for _, d := range []struct {
			dst *time.Duration
			key string
			def time.Duration
		}{
			{&cfg.Interval, "RESOLVER_INTERVAL", 10 * time.Second},
			{&cfg.MinAge, "RESOLVER_MIN_AGE", 40 * time.Second},
			{&cfg.DLQInterval, "RESOLVER_DLQ_INTERVAL", time.Minute},
			{&cfg.MEDInterval, "MED_INTERVAL", 10 * time.Second},
		} {
			if *d.dst, err = config.Duration(d.key, d.def); err != nil {
				return nil, err
			}
		}
		redrives, err := config.Float("RESOLVER_MAX_REDRIVES", 3)
		if err != nil {
			return nil, err
		}
		cfg.MaxRedrives = int(redrives)
		cfg.ReconcileAt = config.String("RECONCILE_AT", "03:00")

		bacenClient := clients.NewBacen(bacenURL, 15*time.Second)
		res, err := psp.NewResolver(r.svc, clients.NewSPI(spiURL, 5*time.Second), bacenClient, r.rabbit, cfg)
		if err != nil {
			return nil, err
		}
		res.SetIGA(bacenClient)
		if err := res.LoadLastReport(ctx); err != nil {
			return nil, err
		}
		res.Routes(r.mux)
		slog.InfoContext(ctx, "pending-resolver iniciando", "ispb", r.ispb, "version", version,
			"interval", cfg.Interval.String(), "min_age", cfg.MinAge.String(), "reconcile_at", cfg.ReconcileAt)
		return res.Run, nil
	})
}
