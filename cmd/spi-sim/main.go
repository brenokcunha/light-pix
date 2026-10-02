// Comando spi-sim: simula o SPI (troca de pacs.008/pacs.002 entre
// participantes via ICOM e liquidação nas contas PI do bacen-sim).
package main

import (
	"context"
	"net/http"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/brenokcunha/light-pix/internal/app"
	"github.com/brenokcunha/light-pix/internal/clients"
	"github.com/brenokcunha/light-pix/internal/config"
	"github.com/brenokcunha/light-pix/internal/faults"
	"github.com/brenokcunha/light-pix/internal/sim/spi"
)

// version é preenchida no build via -ldflags.
var version = "dev"

func main() {
	app.Main(app.Sim{
		Name: "spi-sim", Version: version, DB: "spi", Setup: setup,
		// Cada espera sem mensagens duraria segundos e viraria um trace "lento".
		SkipTrace: func(r *http.Request) bool { return strings.HasSuffix(r.URL.Path, "/stream") },
	})
}

func setup(ctx context.Context, client *mongo.Client, db string, mux *http.ServeMux, fs *faults.State) (func(context.Context) error, error) {
	bacenURL, err := config.Required("BACEN_URL")
	if err != nil {
		return nil, err
	}
	cfg := spi.Config{
		Participants: config.List("SPI_PARTICIPANTS", "11111111,22222222"),
		Settler:      clients.NewBacen(bacenURL, 5*time.Second),
		Faults:       fs,
	}
	for _, d := range []struct {
		dst *time.Duration
		key string
		def time.Duration
	}{
		{&cfg.CreditorTimeout, "SPI_CREDITOR_TIMEOUT", 10 * time.Second},
		{&cfg.PollWait, "SPI_POLL_WAIT", 20 * time.Second},
		{&cfg.Lease, "SPI_LEASE", 30 * time.Second},
		{&cfg.SweepInterval, "SPI_SWEEP_INTERVAL", time.Second},
		{&cfg.SettleRetry, "SPI_SETTLE_RETRY", 5 * time.Second},
	} {
		if *d.dst, err = config.Duration(d.key, d.def); err != nil {
			return nil, err
		}
	}
	s, err := spi.New(ctx, client, db, cfg)
	if err != nil {
		return nil, err
	}
	s.Routes(mux)
	return s.Run, nil
}
