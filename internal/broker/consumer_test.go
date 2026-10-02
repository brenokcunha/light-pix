package broker_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/brenokcunha/light-pix/internal/broker"
	"github.com/brenokcunha/light-pix/internal/testutil"
)

var delays = []time.Duration{100 * time.Millisecond, 200 * time.Millisecond}

type rig struct {
	t    *testing.T
	conn *amqp.Connection
	ch   *amqp.Channel
	pub  *broker.Publisher
}

func newRig(t *testing.T) *rig {
	t.Helper()
	conn, err := broker.Dial(context.Background(), testutil.RabbitMQ(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	if err := broker.Declare(conn, delays...); err != nil {
		t.Fatal(err)
	}
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	pub, err := broker.NewPublisher(conn)
	if err != nil {
		t.Fatal(err)
	}
	return &rig{t: t, conn: conn, ch: ch, pub: pub}
}

func (r *rig) consume(h broker.Handler) {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Go(func() {
		err := broker.Consume(ctx, r.conn, broker.ConsumeConfig{Queue: broker.QueueResults, Concurrency: 1, RetryDelays: delays}, h)
		if err != nil && ctx.Err() == nil {
			r.t.Errorf("Consume: %v", err)
		}
	})
	r.t.Cleanup(func() { cancel(); wg.Wait() })
}

func (r *rig) send(id string) {
	if err := r.pub.Publish(context.Background(), broker.ExchangePix, broker.QueueResults, broker.Message{ID: id, Body: []byte(`{}`)}); err != nil {
		r.t.Fatal(err)
	}
}

func (r *rig) waitDLQ(id string) amqp.Delivery {
	r.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		d, ok, err := r.ch.Get(broker.DLQ(broker.QueueResults), true)
		if err != nil {
			r.t.Fatal(err)
		}
		if ok {
			if d.MessageId != id {
				r.t.Fatalf("DLQ com mensagem %q, quer %q", d.MessageId, id)
			}
			return d
		}
		time.Sleep(20 * time.Millisecond)
	}
	r.t.Fatalf("mensagem %q não chegou à DLQ", id)
	return amqp.Delivery{}
}

func (r *rig) assertDLQEmpty() {
	r.t.Helper()
	if d, ok, _ := r.ch.Get(broker.DLQ(broker.QueueResults), true); ok {
		r.t.Fatalf("DLQ não deveria ter mensagens: %q", d.MessageId)
	}
}

// Falha duas vezes e funciona na terceira: as tentativas respeitam as
// esperas das filas de retry e nada vai para a DLQ.
func TestRetryTiersThenSuccess(t *testing.T) {
	r := newRig(t)
	type call struct {
		at      time.Time
		attempt int
	}
	var mu sync.Mutex
	var calls []call
	done := make(chan struct{})
	r.consume(func(_ context.Context, d amqp.Delivery) error {
		mu.Lock()
		defer mu.Unlock()
		calls = append(calls, call{time.Now(), broker.HeaderInt(d.Headers, broker.HeaderAttempt)})
		if len(calls) < 3 {
			return errors.New("indisponível")
		}
		close(done)
		return nil
	})
	r.send("m1")

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("mensagem não foi reprocessada")
	}
	mu.Lock()
	defer mu.Unlock()
	for i, c := range calls {
		if c.attempt != i {
			t.Errorf("tentativa %d chegou com x-attempt=%d", i, c.attempt)
		}
	}
	if gap := calls[1].at.Sub(calls[0].at); gap < delays[0] {
		t.Errorf("1ª espera = %v, quer ≥ %v", gap, delays[0])
	}
	if gap := calls[2].at.Sub(calls[1].at); gap < delays[1] {
		t.Errorf("2ª espera = %v, quer ≥ %v", gap, delays[1])
	}
	time.Sleep(300 * time.Millisecond)
	r.assertDLQEmpty()
}

// Falha sempre: depois da tentativa original e de uma por fila de retry, a
// mensagem vai para a DLQ com o motivo.
func TestRetryExhaustedGoesToDLQ(t *testing.T) {
	r := newRig(t)
	var mu sync.Mutex
	calls := 0
	r.consume(func(context.Context, amqp.Delivery) error {
		mu.Lock()
		defer mu.Unlock()
		calls++
		return errors.New("SPI fora do ar")
	})
	r.send("m1")

	d := r.waitDLQ("m1")
	mu.Lock()
	defer mu.Unlock()
	if calls != 1+len(delays) {
		t.Errorf("%d execuções, quer %d", calls, 1+len(delays))
	}
	if got := broker.HeaderInt(d.Headers, broker.HeaderAttempt); got != 1+len(delays) {
		t.Errorf("x-attempt = %d", got)
	}
	if d.Headers[broker.HeaderLastError] != "SPI fora do ar" || d.Headers[broker.HeaderSourceQueue] != broker.QueueResults {
		t.Errorf("headers da DLQ: %v", d.Headers)
	}
}

// Erro permanente não gasta tentativas.
func TestPermanentErrorGoesStraightToDLQ(t *testing.T) {
	r := newRig(t)
	var mu sync.Mutex
	calls := 0
	r.consume(func(context.Context, amqp.Delivery) error {
		mu.Lock()
		defer mu.Unlock()
		calls++
		return broker.Permanent(errors.New("pacs.002 ilegível"))
	})
	r.send("m1")

	d := r.waitDLQ("m1")
	time.Sleep(400 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if calls != 1 {
		t.Errorf("%d execuções, quer 1", calls)
	}
	if broker.HeaderInt(d.Headers, broker.HeaderAttempt) != 1 {
		t.Errorf("x-attempt = %v", d.Headers[broker.HeaderAttempt])
	}
}
