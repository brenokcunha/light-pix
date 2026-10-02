// Comando bacen-sim: simula as contas PI e as transferências STR do Banco
// Central. As liquidações chegam do spi-sim.
package main

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/brenokcunha/light-pix/internal/app"
	"github.com/brenokcunha/light-pix/internal/config"
	"github.com/brenokcunha/light-pix/internal/pix"
	"github.com/brenokcunha/light-pix/internal/sim/bacen"
)

// version é preenchida no build via -ldflags.
var version = "dev"

func main() {
	app.Main(app.Sim{Name: "bacen-sim", Version: version, DB: "bacen", Setup: setup})
}

func setup(ctx context.Context, client *mongo.Client, db string, mux *http.ServeMux) (func(context.Context) error, error) {
	initial, err := parseAccounts(config.List("BACEN_PI_ACCOUNTS", ""))
	if err != nil {
		return nil, err
	}
	s, err := bacen.New(ctx, client, db)
	if err != nil {
		return nil, err
	}
	if err := s.Seed(ctx, initial); err != nil {
		return nil, err
	}
	s.Routes(mux)
	return nil, nil
}

// parseAccounts lê itens "ispb:saldo" (ex.: 11111111:1000000.00).
func parseAccounts(items []string) (map[string]pix.Amount, error) {
	out := make(map[string]pix.Amount, len(items))
	for _, it := range items {
		ispb, amount, ok := strings.Cut(it, ":")
		if !ok || !pix.ValidISPB(ispb) {
			return nil, fmt.Errorf("BACEN_PI_ACCOUNTS: item inválido %q", it)
		}
		a, err := pix.ParseAmount(amount)
		if err != nil {
			return nil, fmt.Errorf("BACEN_PI_ACCOUNTS: %s: %w", ispb, err)
		}
		out[ispb] = a
	}
	return out, nil
}
