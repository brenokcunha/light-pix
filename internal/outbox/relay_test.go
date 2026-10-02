package outbox_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/brenokcunha/light-pix/internal/broker"
	"github.com/brenokcunha/light-pix/internal/outbox"
	"github.com/brenokcunha/light-pix/internal/store"
	"github.com/brenokcunha/light-pix/internal/testutil"
)

// Com a varredura desligada, só o change stream e o resume token levam os
// eventos ao RabbitMQ: o relay entrega em tempo real e, depois de parar e
// voltar, retoma do ponto onde parou.
func TestRelayDeliversViaChangeStreamAndResumes(t *testing.T) {
	client, dbName := testutil.Mongo(t)
	db := client.Database(dbName)
	conn, err := broker.Dial(context.Background(), testutil.RabbitMQ(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	if err := broker.Declare(conn); err != nil {
		t.Fatal(err)
	}
	pub, err := broker.NewPublisher(conn)
	if err != nil {
		t.Fatal(err)
	}
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}

	startRelay := func() (stop func()) {
		relay, err := outbox.NewRelay(context.Background(), db, pub, outbox.RelayConfig{
			Name: "relay", SweepInterval: time.Hour, CheckpointEvery: 50 * time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		var wg sync.WaitGroup
		wg.Go(func() { _ = relay.Run(ctx) })
		time.Sleep(500 * time.Millisecond) // deixa o stream abrir
		return func() { cancel(); wg.Wait() }
	}
	add := func(id string) {
		err := store.Transact(context.Background(), client, func(ctx context.Context) error {
			return outbox.Add(ctx, outbox.Collection(db), broker.QueueSPIOut, id, []byte(`{"id":"`+id+`"}`))
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	waitMessage := func(id string) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			msg, ok, err := ch.Get(broker.QueueSPIOut, true)
			if err != nil {
				t.Fatal(err)
			}
			if ok {
				if msg.MessageId != id {
					t.Fatalf("mensagem %q, quer %q", msg.MessageId, id)
				}
				if msg.DeliveryMode != amqp.Persistent || msg.Headers[broker.HeaderEnqueuedAt] == nil {
					t.Fatalf("mensagem sem persistência ou x-enqueued-at: %+v", msg)
				}
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatalf("mensagem %q não chegou ao RabbitMQ", id)
	}
	published := func(id string) bool {
		var ev outbox.Event
		err := outbox.Collection(db).FindOne(context.Background(), bson.D{{Key: "messageId", Value: id}}).Decode(&ev)
		return err == nil && ev.PublishedAt != nil
	}

	stop := startRelay()
	added := time.Now()
	add("m1")
	waitMessage("m1")
	// Com a varredura desligada, é o change stream que entrega: tem de ser
	// rápido, não esperar outro evento nem um timeout.
	if lag := time.Since(added); lag > time.Second {
		t.Fatalf("o change stream levou %v para entregar um evento isolado", lag)
	}
	time.Sleep(200 * time.Millisecond) // deixa o checkpoint ser gravado
	stop()

	var cp bson.M
	if err := db.Collection("relay_checkpoints").FindOne(context.Background(), bson.D{{Key: "_id", Value: "relay"}}).Decode(&cp); err != nil {
		t.Fatalf("checkpoint não gravado: %v", err)
	}

	add("m2") // gravado com o relay parado
	if published("m2") {
		t.Fatal("m2 não deveria estar publicado")
	}
	stop = startRelay()
	defer stop()
	waitMessage("m2") // só o resume token explica: a varredura está desligada

	if _, ok, _ := ch.Get(broker.QueueSPIOut, true); ok {
		t.Fatal("m1 foi publicada de novo depois do retorno do relay")
	}
	if !published("m1") || !published("m2") {
		t.Fatal("eventos não marcados como publicados")
	}
}

// Uma rajada de eventos sai toda, sem perda, em tempo curto: o relay publica
// em lotes, com as confirmações em voo ao mesmo tempo.
func TestRelayPublishesBurstInBatches(t *testing.T) {
	client, dbName := testutil.Mongo(t)
	db := client.Database(dbName)
	conn, err := broker.Dial(context.Background(), testutil.RabbitMQ(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	if err := broker.Declare(conn); err != nil {
		t.Fatal(err)
	}
	pub, err := broker.NewPublisher(conn)
	if err != nil {
		t.Fatal(err)
	}
	relay, err := outbox.NewRelay(context.Background(), db, pub, outbox.RelayConfig{Name: "relay", SweepInterval: 200 * time.Millisecond, SweepMinAge: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Go(func() { _ = relay.Run(ctx) })
	t.Cleanup(func() { cancel(); wg.Wait() })
	time.Sleep(500 * time.Millisecond)

	const n = 1000
	var adders sync.WaitGroup
	for i := range n {
		adders.Go(func() {
			err := store.Transact(context.Background(), client, func(ctx context.Context) error {
				id := fmt.Sprintf("m%04d", i)
				return outbox.Add(ctx, outbox.Collection(db), broker.QueueSPIOut, id, []byte(`{}`))
			})
			if err != nil {
				t.Error(err)
			}
		})
	}
	adders.Wait()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	deadline := time.Now().Add(15 * time.Second)
	for len(seen) < n && time.Now().Before(deadline) {
		msg, ok, err := ch.Get(broker.QueueSPIOut, true)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		seen[msg.MessageId] = true
	}
	if len(seen) != n {
		t.Fatalf("chegaram %d de %d mensagens distintas", len(seen), n)
	}
	eventually := time.Now().Add(5 * time.Second)
	for time.Now().Before(eventually) {
		if c, _ := outbox.Collection(db).CountDocuments(context.Background(), bson.D{{Key: "publishedAt", Value: nil}}); c == 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("eventos publicados não foram marcados")
}
