// Package testutil tem apoio aos testes de integração.
package testutil

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/brenokcunha/light-pix/internal/store"
)

// EnvMongoURI aponta para o MongoDB (replica set) dos testes de integração,
// ex.: mongodb://localhost:27017/?directConnection=true com a stack no ar.
const EnvMongoURI = "LIGHTPIX_TEST_MONGO_URI"

// Mongo conecta ao MongoDB de teste e devolve um nome de banco exclusivo,
// apagado ao fim do teste. Sem a variável de ambiente, o teste é pulado.
func Mongo(t testing.TB) (*mongo.Client, string) {
	t.Helper()
	uri := os.Getenv(EnvMongoURI)
	if uri == "" {
		t.Skip(EnvMongoURI + " não definida; teste de integração pulado")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := store.Connect(ctx, uri)
	if err != nil {
		t.Fatalf("conectar ao MongoDB: %v", err)
	}
	var b [6]byte
	_, _ = rand.Read(b[:])
	db := "test_" + hex.EncodeToString(b[:])
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = client.Database(db).Drop(ctx)
		_ = client.Disconnect(ctx)
	})
	return client, db
}
