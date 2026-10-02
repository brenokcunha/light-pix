package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/brenokcunha/light-pix/internal/broker"
	"github.com/brenokcunha/light-pix/internal/config"
	"github.com/brenokcunha/light-pix/internal/httpx"
	"github.com/brenokcunha/light-pix/internal/store"
	"github.com/brenokcunha/light-pix/internal/telemetry"
)

func runAPI(ctx context.Context) error {
	shutdown, err := telemetry.Setup(ctx, telemetry.Config{ServiceName: "payment-api", Version: version})
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

	startCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	mongoClient, err := store.Connect(startCtx, mongoURI)
	if err != nil {
		return err
	}
	defer func() { _ = mongoClient.Disconnect(context.Background()) }()

	rabbit, err := broker.Dial(startCtx, rabbitURL)
	if err != nil {
		return err
	}
	defer rabbit.Close()

	mux := http.NewServeMux()
	httpx.HandleHealth(mux, map[string]httpx.Check{
		"mongodb":  func(ctx context.Context) error { return store.Ping(ctx, mongoClient) },
		"rabbitmq": func(context.Context) error { return rabbitAlive(rabbit) },
	})

	slog.InfoContext(ctx, "payment-api iniciando", "ispb", ispb, "version", version)
	return httpx.Serve(ctx, config.String("HTTP_ADDR", ":8080"), httpx.Instrument("payment-api", mux))
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
