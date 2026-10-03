package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/brenokcunha/light-pix/internal/app"
	"github.com/brenokcunha/light-pix/internal/broker"
	"github.com/brenokcunha/light-pix/internal/clients"
	"github.com/brenokcunha/light-pix/internal/config"
	"github.com/brenokcunha/light-pix/internal/httpx"
	"github.com/brenokcunha/light-pix/internal/psp"
	"github.com/brenokcunha/light-pix/internal/store"
	"github.com/brenokcunha/light-pix/internal/telemetry"
)

// role é o que cada subcomando recebe pronto: configuração, conexões e o
// serviço do PSP.
type role struct {
	ispb   string
	client *mongo.Client
	db     *mongo.Database
	rabbit *amqp.Connection
	store  *psp.Store
	svc    *psp.Service
	dict   *clients.Dict
	mux    *http.ServeMux
	// retryDelays são as esperas do retry das filas (RETRY_DELAYS).
	retryDelays []time.Duration
}

// runRole monta o ambiente comum a todos os papéis: telemetria, MongoDB,
// RabbitMQ (com a topologia declarada), serviço do PSP e servidor de health.
// setup registra rotas em r.mux e devolve a rotina principal do papel.
func runRole(ctx context.Context, service string, setup func(ctx context.Context, r *role) (func(context.Context) error, error)) error {
	shutdown, err := telemetry.Setup(ctx, telemetry.Config{ServiceName: service, Version: version})
	if err != nil {
		return err
	}
	defer func() { _ = shutdown(context.Background()) }()

	ispb, err := config.Required("PSP_ISPB")
	if err != nil {
		return err
	}
	mongoURI, err := config.Required("MONGO_URI")
	if err != nil {
		return err
	}
	rabbitURL, err := config.Required("RABBITMQ_URL")
	if err != nil {
		return err
	}
	dictCfg, err := dictConfig()
	if err != nil {
		return err
	}

	delays, err := retryDelays()
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
	rabbit, err := broker.Dial(startCtx, rabbitURL)
	if err != nil {
		return err
	}
	defer rabbit.Close()
	if err := broker.Declare(rabbit, delays...); err != nil {
		return err
	}

	db := client.Database(config.String("MONGO_DB", "psp"))
	st, err := psp.NewStore(startCtx, client, db)
	if err != nil {
		return err
	}
	dict := clients.NewDict(config.String("DICT_URL", "http://dict-sim:8080"), ispb, dictCfg)
	svc, err := psp.NewService(ispb, st, dict)
	if err != nil {
		return err
	}
	svc.SetMED(dict)

	r := &role{ispb: ispb, client: client, db: db, rabbit: rabbit, store: st, svc: svc, dict: dict, mux: http.NewServeMux(), retryDelays: delays}
	httpx.HandleHealth(r.mux, map[string]httpx.Check{
		"mongodb":  func(ctx context.Context) error { return store.Ping(ctx, client) },
		"rabbitmq": func(context.Context) error { return rabbitAlive(rabbit) },
	})
	run, err := setup(startCtx, r)
	if err != nil {
		return err
	}
	return app.Run(ctx, service, r.mux, nil, run)
}

// retryDelays lê RETRY_DELAYS (ex.: "5s,30s,2m"): a espera antes de cada
// nova tentativa de uma mensagem que falhou.
func retryDelays() ([]time.Duration, error) {
	items := config.List("RETRY_DELAYS", "")
	if len(items) == 0 {
		return broker.DefaultRetryDelays, nil
	}
	delays := make([]time.Duration, len(items))
	for i, it := range items {
		d, err := time.ParseDuration(it)
		if err != nil || d <= 0 {
			return nil, fmt.Errorf("config: RETRY_DELAYS inválida: %q", it)
		}
		delays[i] = d
	}
	return delays, nil
}

func dictConfig() (clients.DictConfig, error) {
	var cfg clients.DictConfig
	var err error
	if cfg.Timeout, err = config.Duration("DICT_TIMEOUT", 800*time.Millisecond); err != nil {
		return cfg, err
	}
	if cfg.CacheTTL, err = config.Duration("DICT_CACHE_TTL", 10*time.Second); err != nil {
		return cfg, err
	}
	attempts, err := config.Float("DICT_ATTEMPTS", 3)
	cfg.Attempts = int(attempts)
	return cfg, err
}

func rabbitAlive(conn *amqp.Connection) error {
	if conn.IsClosed() {
		return errors.New("conexão fechada")
	}
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("abrir canal: %w", err)
	}
	return ch.Close()
}
