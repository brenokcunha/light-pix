// Package outbox implementa o padrão transactional outbox: o evento é gravado
// na mesma transação do MongoDB que muda o estado de negócio, e o relay o
// publica no RabbitMQ depois, com entrega at-least-once.
package outbox

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/brenokcunha/light-pix/internal/store"
)

// Event é uma mensagem a publicar.
type Event struct {
	ID         bson.ObjectID `bson:"_id"`
	RoutingKey string        `bson:"routingKey"`
	MessageID  string        `bson:"messageId"`
	Body       []byte        `bson:"body"` // JSON
	// TraceContext é o contexto de trace de quem gravou o evento; o relay o
	// continua ao publicar.
	TraceContext map[string]string `bson:"traceContext,omitempty"`
	CreatedAt    time.Time         `bson:"createdAt"`
	PublishedAt  *time.Time        `bson:"publishedAt,omitempty"`
}

// publishedTTL é quanto tempo os eventos publicados ficam para depuração; o TTL
// não toca nos que ainda não foram publicados (sem publishedAt).
const publishedTTL = 24 * time.Hour

// Collection devolve a coleção outbox de db.
func Collection(db *mongo.Database) *mongo.Collection { return db.Collection("outbox") }

// EnsureIndexes cria o índice usado pelo relay para achar eventos pendentes e
// pelo TTL dos publicados.
func EnsureIndexes(ctx context.Context, coll *mongo.Collection) error {
	_, err := coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "publishedAt", Value: 1}},
		Options: options.Index().SetExpireAfterSeconds(int32(publishedTTL.Seconds())),
	})
	if err != nil {
		return fmt.Errorf("outbox: índices: %w", err)
	}
	return nil
}

// Add grava o evento. Chamado dentro de store.Transact (com o ctx da
// transação), entra na mesma transação do estado de negócio.
func Add(ctx context.Context, coll *mongo.Collection, routingKey, messageID string, body []byte) error {
	var tc map[string]string
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	if len(carrier) > 0 {
		tc = carrier
	}
	_, err := coll.InsertOne(ctx, Event{
		ID:           bson.NewObjectID(),
		RoutingKey:   routingKey,
		MessageID:    messageID,
		Body:         body,
		TraceContext: tc,
		CreatedAt:    store.Now(),
	})
	if err != nil {
		return fmt.Errorf("outbox: gravar evento: %w", err)
	}
	return nil
}
