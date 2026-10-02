// Package store concentra o acesso ao MongoDB.
package store

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

// Connect abre o cliente e confirma a conexão com o primário do replica set
// (transações e change streams exigem replica set).
func Connect(ctx context.Context, uri string) (*mongo.Client, error) {
	client, err := mongo.Connect(options.Client().
		ApplyURI(uri).
		SetServerSelectionTimeout(5 * time.Second))
	if err != nil {
		return nil, fmt.Errorf("store: connect: %w", err)
	}
	if err := Ping(ctx, client); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, err
	}
	return client, nil
}

// Ping verifica se o primário responde.
func Ping(ctx context.Context, client *mongo.Client) error {
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		return fmt.Errorf("store: ping: %w", err)
	}
	return nil
}
