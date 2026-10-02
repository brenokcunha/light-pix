package main

import (
	"context"
	"log/slog"

	"github.com/brenokcunha/light-pix/internal/config"
	"github.com/brenokcunha/light-pix/internal/psp"
)

// runAPI sobe a payment-api: pedidos de Pix, consulta de pagamentos e contas
// e gestão de chaves.
func runAPI(ctx context.Context) error {
	return runRole(ctx, "payment-api", func(ctx context.Context, r *role) (func(context.Context) error, error) {
		if path := config.String("PSP_ACCOUNTS_SEED_FILE", ""); path != "" {
			if err := r.store.SeedAccounts(ctx, r.ispb, path); err != nil {
				return nil, err
			}
		}
		psp.NewAPI(r.svc, r.dict).Routes(r.mux)
		slog.InfoContext(ctx, "payment-api iniciando", "ispb", r.ispb, "version", version)
		return nil, nil
	})
}
