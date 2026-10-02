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
	"github.com/brenokcunha/light-pix/internal/faults"
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
	// SkipTrace, se definido, exclui requisições dos traces (ex.: long-polling).
	SkipTrace func(*http.Request) bool
	// Setup registra as rotas em mux. faults é o estado das falhas injetadas
	// (já exposto em /admin/faults); o simulador o consulta para as falhas que
	// só ele sabe aplicar.
	Setup func(ctx context.Context, client *mongo.Client, db string, mux *http.ServeMux, faults *faults.State) (background func(context.Context) error, err error)
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
	fs, err := faults.NewState()
	if err != nil {
		return err
	}
	if err := fs.FromEnv(config.String("FAULTS", "")); err != nil {
		return err
	}
	fs.Routes(mux)
	background, err := s.Setup(startCtx, client, config.String("MONGO_DB", s.DB), mux, fs)
	if err != nil {
		return err
	}

	slog.InfoContext(ctx, s.Name+" iniciando", "version", s.Version)
	return Run(ctx, s.Name, fs.Wrap(mux), s.SkipTrace, background)
}

// Run serve h em HTTP_ADDR (padrão :8080) e roda background, se houver,
// até o contexto terminar. Se qualquer um dos dois parar, derruba o outro:
// um serviço sem a rotina de fundo não deve continuar parecendo saudável.
func Run(ctx context.Context, name string, h http.Handler, skipTrace func(*http.Request) bool, background func(context.Context) error) error {
	ctx, cancelAll := context.WithCancel(ctx)
	defer cancelAll()
	bgDone := make(chan error, 1)
	if background != nil {
		go func() {
			bgDone <- background(ctx)
			cancelAll()
		}()
	} else {
		bgDone <- nil
	}

	serveErr := httpx.Serve(ctx, config.String("HTTP_ADDR", ":8080"), httpx.InstrumentSkipping(name, h, skipTrace))
	cancelAll()
	if err := <-bgDone; err != nil {
		return err
	}
	return serveErr
}
