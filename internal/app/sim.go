// Package app reúne o esqueleto comum dos binários de simulador: subcomandos,
// sinais, telemetria, conexão ao MongoDB, health checks e servidor HTTP.
package app

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/brenokcunha/light-pix/internal/config"
	"github.com/brenokcunha/light-pix/internal/httpx"
	"github.com/brenokcunha/light-pix/internal/store"
	"github.com/brenokcunha/light-pix/internal/telemetry"
)

// Sim descreve um simulador. Setup registra as rotas e pode devolver uma
// rotina de fundo, que roda até o contexto ser cancelado.
type Sim struct {
	Name    string // service.name no OTel
	Version string
	DB      string // banco padrão, sobrescrito por MONGO_DB
	Setup   func(ctx context.Context, client *mongo.Client, db string, mux *http.ServeMux) (background func(context.Context) error, err error)
}

// Main interpreta os argumentos e encerra o processo com o código adequado.
// Sem argumento ou com "serve", sobe o simulador; "healthcheck" serve de
// probe para a imagem distroless.
func Main(s Sim) {
	cmd := "serve"
	if len(os.Args) > 1 {
		cmd = os.Args[1]
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var err error
	switch cmd {
	case "serve":
		err = serve(ctx, s)
	case "healthcheck":
		err = httpx.Probe(ctx, "http://localhost"+config.String("HTTP_ADDR", ":8080")+"/healthz")
	default:
		fmt.Fprintf(os.Stderr, "uso: %s [serve|healthcheck]\n", s.Name)
		os.Exit(2)
	}
	if err != nil {
		slog.Error("encerrado com erro", "command", cmd, "err", err)
		os.Exit(1)
	}
}

func serve(ctx context.Context, s Sim) error {
	shutdown, err := telemetry.Setup(ctx, telemetry.Config{ServiceName: s.Name, Version: s.Version})
	if err != nil {
		return err
	}
	defer func() { _ = shutdown(context.Background()) }()

	mongoURI, err := config.Required("MONGO_URI")
	if err != nil {
		return err
	}
	startCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	client, err := store.Connect(startCtx, mongoURI)
	if err != nil {
		return err
	}
	defer func() { _ = client.Disconnect(context.Background()) }()

	mux := http.NewServeMux()
	httpx.HandleHealth(mux, map[string]httpx.Check{
		"mongodb": func(ctx context.Context) error { return store.Ping(ctx, client) },
	})
	background, err := s.Setup(startCtx, client, config.String("MONGO_DB", s.DB), mux)
	if err != nil {
		return err
	}

	ctx, cancelAll := context.WithCancel(ctx)
	defer cancelAll()
	bgDone := make(chan error, 1)
	if background != nil {
		go func() {
			bgDone <- background(ctx)
			cancelAll() // rotina de fundo morreu: derruba o serviço inteiro
		}()
	} else {
		bgDone <- nil
	}

	slog.InfoContext(ctx, s.Name+" iniciando", "version", s.Version)
	serveErr := httpx.Serve(ctx, config.String("HTTP_ADDR", ":8080"), httpx.Instrument(s.Name, mux))
	cancelAll()
	if err := <-bgDone; err != nil {
		return err
	}
	return serveErr
}
