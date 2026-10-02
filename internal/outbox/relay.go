package outbox

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/brenokcunha/light-pix/internal/broker"
	"github.com/brenokcunha/light-pix/internal/store"
)

// Códigos do servidor para um resume token que não existe mais no oplog.
const (
	codeChangeStreamFatalError  = 280
	codeChangeStreamHistoryLost = 286
)

// RelayConfig ajusta o relay.
type RelayConfig struct {
	Name            string        // identifica o checkpoint (um por relay)
	SweepInterval   time.Duration // varredura de eventos que o change stream não entregou ou cuja publicação falhou
	SweepMinAge     time.Duration // idade mínima do evento para a varredura publicá-lo (evita disputar com o stream)
	CheckpointEvery time.Duration // frequência de gravação do resume token
}

func (c *RelayConfig) defaults() {
	if c.SweepInterval == 0 {
		c.SweepInterval = 2 * time.Second
	}
	if c.SweepMinAge == 0 {
		c.SweepMinAge = 3 * time.Second
	}
	if c.CheckpointEvery == 0 {
		c.CheckpointEvery = time.Second
	}
}

// Relay publica os eventos da outbox no RabbitMQ. Escuta o change stream da
// coleção (baixa latência) e guarda o resume token para retomar do ponto
// certo após reiniciar. Uma varredura periódica publica o que o stream não
// entregou (primeira subida, falha de publicação, token perdido). A entrega é
// at-least-once: uma mesma mensagem pode sair duas vezes, e os consumidores
// são idempotentes.
type Relay struct {
	cfg         RelayConfig
	events      *mongo.Collection
	checkpoints *mongo.Collection
	pub         *broker.Publisher
	tracer      trace.Tracer

	published metric.Int64Counter
	lag       metric.Float64Histogram
}

// NewRelay prepara o relay para a coleção outbox de db.
func NewRelay(ctx context.Context, db *mongo.Database, pub *broker.Publisher, cfg RelayConfig) (*Relay, error) {
	cfg.defaults()
	r := &Relay{
		cfg:         cfg,
		events:      Collection(db),
		checkpoints: db.Collection("relay_checkpoints"),
		pub:         pub,
		tracer:      otel.Tracer("github.com/brenokcunha/light-pix/internal/outbox"),
	}
	if err := EnsureIndexes(ctx, r.events); err != nil {
		return nil, err
	}
	meter := otel.Meter("github.com/brenokcunha/light-pix/internal/outbox")
	var err error
	if r.published, err = meter.Int64Counter("outbox.published",
		metric.WithDescription("Eventos da outbox publicados, por rota e resultado")); err != nil {
		return nil, err
	}
	if r.lag, err = meter.Float64Histogram("outbox.publish.lag", metric.WithUnit("s"),
		metric.WithDescription("Tempo entre gravar o evento e publicá-lo")); err != nil {
		return nil, err
	}
	_, err = meter.Int64ObservableGauge("outbox.pending",
		metric.WithDescription("Eventos gravados e ainda não publicados"),
		metric.WithInt64Callback(func(ctx context.Context, o metric.Int64Observer) error {
			n, err := r.events.CountDocuments(ctx, bson.D{{Key: "publishedAt", Value: nil}})
			if err != nil {
				return err
			}
			o.Observe(n)
			return nil
		}))
	return r, err
}

// Run roda até o contexto terminar. Erros do change stream reabrem o stream
// depois de uma espera; um token que o servidor não conhece mais é descartado
// (a varredura cobre o buraco).
func (r *Relay) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	wg.Go(func() { r.sweepLoop(ctx) })
	defer wg.Wait()

	backoff := time.Second
	for ctx.Err() == nil {
		err := r.stream(ctx)
		if ctx.Err() != nil {
			return nil
		}
		var cmdErr mongo.CommandError
		if errors.As(err, &cmdErr) && (cmdErr.Code == codeChangeStreamHistoryLost || cmdErr.Code == codeChangeStreamFatalError) {
			slog.WarnContext(ctx, "resume token inválido; descartando checkpoint", "err", err)
			_, _ = r.checkpoints.DeleteOne(ctx, bson.D{{Key: "_id", Value: r.cfg.Name}})
		}
		slog.ErrorContext(ctx, "change stream da outbox encerrado; reabrindo", "err", err, "retry_in", backoff)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 10*time.Second)
	}
	return nil
}

func (r *Relay) stream(ctx context.Context) error {
	opts := options.ChangeStream()
	var token bson.Raw
	var cp struct {
		Token bson.Raw `bson:"token"`
	}
	switch err := r.checkpoints.FindOne(ctx, bson.D{{Key: "_id", Value: r.cfg.Name}}).Decode(&cp); {
	case err == nil:
		token = cp.Token
		opts.SetResumeAfter(token)
	case !errors.Is(err, mongo.ErrNoDocuments):
		return fmt.Errorf("ler checkpoint: %w", err)
	}

	// No primeiro start não há token: a varredura inicial publica o acumulado.
	if token == nil {
		if err := r.sweep(ctx, 0); err != nil {
			slog.ErrorContext(ctx, "varredura inicial da outbox falhou", "err", err)
		}
	}

	cs, err := r.events.Watch(ctx, mongo.Pipeline{
		{{Key: "$match", Value: bson.D{{Key: "operationType", Value: "insert"}}}},
	}, opts)
	if err != nil {
		return fmt.Errorf("abrir change stream: %w", err)
	}
	defer cs.Close(context.WithoutCancel(ctx))

	lastSave := time.Now()
	save := func() {
		if _, err := r.checkpoints.UpdateOne(ctx, bson.D{{Key: "_id", Value: r.cfg.Name}},
			bson.D{{Key: "$set", Value: bson.D{{Key: "token", Value: cs.ResumeToken()}, {Key: "updatedAt", Value: store.Now()}}}},
			options.UpdateOne().SetUpsert(true)); err != nil && ctx.Err() == nil {
			slog.WarnContext(ctx, "falha ao gravar checkpoint do relay", "err", err)
		}
		lastSave = time.Now()
	}
	defer func() {
		// Grava o último token ao encerrar, mesmo com o contexto cancelado.
		if cs.ResumeToken() != nil {
			ctx = context.WithoutCancel(ctx)
			save()
		}
	}()

	for cs.Next(ctx) {
		var ev struct {
			FullDocument Event `bson:"fullDocument"`
		}
		if err := cs.Decode(&ev); err != nil {
			return fmt.Errorf("decodificar evento: %w", err)
		}
		// Falha de publicação não derruba o stream: o evento fica sem
		// publishedAt e a varredura tenta de novo.
		if err := r.publish(ctx, ev.FullDocument); err != nil && ctx.Err() == nil {
			slog.WarnContext(ctx, "falha ao publicar evento da outbox; a varredura repete", "id", ev.FullDocument.ID.Hex(), "err", err)
		}
		if time.Since(lastSave) >= r.cfg.CheckpointEvery {
			save()
		}
	}
	return cs.Err()
}

func (r *Relay) sweepLoop(ctx context.Context) {
	t := time.NewTicker(r.cfg.SweepInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := r.sweep(ctx, r.cfg.SweepMinAge); err != nil && ctx.Err() == nil {
				slog.ErrorContext(ctx, "varredura da outbox falhou", "err", err)
			}
		}
	}
}

// sweep publica eventos pendentes com pelo menos minAge de idade.
func (r *Relay) sweep(ctx context.Context, minAge time.Duration) error {
	filter := bson.D{
		{Key: "publishedAt", Value: nil},
		{Key: "createdAt", Value: bson.D{{Key: "$lte", Value: store.Now().Add(-minAge)}}},
	}
	for ctx.Err() == nil {
		cur, err := r.events.Find(ctx, filter, options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}).SetLimit(100))
		if err != nil {
			return err
		}
		var batch []Event
		if err := cur.All(ctx, &batch); err != nil {
			return err
		}
		failed := false
		for _, ev := range batch {
			if err := r.publish(ctx, ev); err != nil {
				slog.WarnContext(ctx, "falha ao publicar evento da outbox", "id", ev.ID.Hex(), "err", err)
				failed = true
			}
		}
		if len(batch) < 100 || failed {
			return nil
		}
	}
	return nil
}

// publish envia o evento e o marca como publicado. O span continua o trace de
// quem gravou o evento; o traceparent da mensagem AMQP é o deste span.
func (r *Relay) publish(ctx context.Context, ev Event) error {
	pctx := otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(ev.TraceContext))
	pctx, span := r.tracer.Start(pctx, "outbox.publish "+ev.RoutingKey, trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(attribute.String("messaging.destination.name", ev.RoutingKey),
			attribute.String("messaging.message.id", ev.MessageID)))
	defer span.End()

	result := "ok"
	defer func() {
		r.published.Add(ctx, 1, metric.WithAttributes(
			attribute.String("routing_key", ev.RoutingKey), attribute.String("result", result)))
	}()

	// Um evento publicado pelo outro caminho (stream × varredura) não sai de novo.
	var cur struct {
		PublishedAt *time.Time `bson:"publishedAt"`
	}
	if err := r.events.FindOne(pctx, bson.D{{Key: "_id", Value: ev.ID}}).Decode(&cur); err == nil && cur.PublishedAt != nil {
		result = "skipped"
		return nil
	}

	if err := r.pub.Publish(pctx, broker.ExchangePix, ev.RoutingKey, broker.Message{
		ID: ev.MessageID, Body: ev.Body, EnqueueAt: ev.CreatedAt,
	}); err != nil {
		result = "error"
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	now := store.Now()
	if _, err := r.events.UpdateOne(pctx, bson.D{{Key: "_id", Value: ev.ID}, {Key: "publishedAt", Value: nil}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "publishedAt", Value: now}}}}); err != nil {
		// Publicou mas não marcou: a varredura vai publicar de novo (duplicata aceita).
		result = "error"
		return fmt.Errorf("marcar evento como publicado: %w", err)
	}
	r.lag.Record(pctx, now.Sub(ev.CreatedAt).Seconds())
	return nil
}
