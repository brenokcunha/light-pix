// Comando lab-ui: interface de testes do laboratório (abas do banco pagador e
// do banco recebedor), com proxy para as APIs dos dois PSPs.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/brenokcunha/light-pix/internal/app"
	"github.com/brenokcunha/light-pix/internal/config"
	"github.com/brenokcunha/light-pix/internal/httpx"
	"github.com/brenokcunha/light-pix/internal/labui"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var err error
	switch {
	case len(os.Args) > 1 && os.Args[1] == "healthcheck":
		err = httpx.Probe(ctx, "http://localhost"+config.String("HTTP_ADDR", ":8080")+"/healthz")
	case len(os.Args) > 1 && os.Args[1] != "serve":
		fmt.Fprintln(os.Stderr, "uso: lab-ui [serve|healthcheck]")
		os.Exit(2)
	default:
		err = serve(ctx)
	}
	if err != nil {
		slog.Error("encerrado com erro", "err", err)
		os.Exit(1)
	}
}

func serve(ctx context.Context) error {
	cfg := labui.Config{
		PSPA: config.String("PSP_A_URL", "http://psp-a-api:8080"),
		PSPB: config.String("PSP_B_URL", "http://psp-b-api:8080"),
	}
	site, err := labui.Handler(cfg)
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	httpx.HandleHealth(mux, nil)
	mux.Handle("/", site)
	slog.InfoContext(ctx, "lab-ui iniciando", "psp_a", cfg.PSPA, "psp_b", cfg.PSPB)
	return app.Run(ctx, "lab-ui", mux, nil, nil)
}
