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
		SetServerSelectionTimeout(5 * time.Second).
		SetMonitor(newCommandMonitor()))
	if err != nil {
		return nil, fmt.Errorf("store: connect: %w", err)
	}
	if err := Ping(ctx, client); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, err
	}
	return client, nil
}

// Transact executa fn numa transação multi-documento. O driver repete fn em
// erros transitórios (ex.: WriteConflict entre duas liquidações na mesma
// conta), então fn deve ser idempotente e usar o ctx recebido.
func Transact(ctx context.Context, client *mongo.Client, fn func(ctx context.Context) error) error {
	sess, err := client.StartSession()
	if err != nil {
		return fmt.Errorf("store: start session: %w", err)
	}
	defer sess.EndSession(context.WithoutCancel(ctx))
	_, err = sess.WithTransaction(ctx, func(ctx context.Context) (any, error) { return nil, fn(ctx) })
	return err
}

// Ping verifica se o primário responde.
func Ping(ctx context.Context, client *mongo.Client) error {
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		return fmt.Errorf("store: ping: %w", err)
	}
	return nil
}

// Now é o horário atual com a precisão do BSON (milissegundos). Usar o mesmo
// valor na resposta e no documento evita que um replay devolva outro horário.
func Now() time.Time { return time.Now().UTC().Truncate(time.Millisecond) }
