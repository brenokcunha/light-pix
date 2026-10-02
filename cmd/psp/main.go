// Comando psp: um binário com um subcomando por papel do PSP, para que todos
// os papéis compartilhem a mesma imagem e escalem de forma independente.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/brenokcunha/light-pix/internal/config"
	"github.com/brenokcunha/light-pix/internal/httpx"
)

// version é preenchida no build via -ldflags.
var version = "dev"

var commands = map[string]func(context.Context) error{
	"api":         runAPI,
	"relay":       runRelay,
	"connector":   runConnector,
	"worker":      runWorker,
	"resolver":    runResolver,
	"healthcheck": runHealthcheck,
}

func main() {
	if len(os.Args) < 2 || commands[os.Args[1]] == nil {
		fmt.Fprintln(os.Stderr, "uso: psp <api|relay|connector|worker|resolver|healthcheck>")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := commands[os.Args[1]](ctx); err != nil {
		slog.Error("encerrado com erro", "command", os.Args[1], "err", err)
		os.Exit(1)
	}
}

func runHealthcheck(ctx context.Context) error {
	return httpx.Probe(ctx, "http://localhost"+config.String("HTTP_ADDR", ":8080")+"/healthz")
}
