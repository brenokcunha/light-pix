package outbox

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
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

const (
	streamBatch = 100 // eventos do change stream publicados de uma vez
	sweepBatch  = 100 // eventos por lote da varredura
)

// publishBatch publica os eventos com as confirmações em voo ao mesmo tempo e
// depois marca os confirmados com uma única escrita: uma ida e volta ao
// broker e uma ao MongoDB por lote, em vez de uma por evento. Cada span
// continua o trace de quem gravou o evento; o traceparent da mensagem AMQP é
// o dele.
//
// O stream e a varredura podem entregar o mesmo evento; os lotes são
// serializados e cada um descarta o que o anterior já publicou. Se o processo
// cair entre publicar e marcar, o evento sai de novo (duplicata aceita).
func (r *Relay) publishBatch(ctx context.Context, events []Event) error {
	if len(events) == 0 {
		return nil
	}
	r.batchMu.Lock()
	defer r.batchMu.Unlock()

	events, err := r.stillPending(ctx, events)
	if err != nil || len(events) == 0 {
		return err
	}

	type inflight struct {
		ev   Event
		span trace.Span
		conf *broker.Confirmation
		err  error
	}
	flight := make([]inflight, len(events))
	for i, ev := range events {
		pctx := otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(ev.TraceContext))
		pctx, span := r.tracer.Start(pctx, "outbox.publish "+ev.RoutingKey, trace.WithSpanKind(trace.SpanKindProducer),
			trace.WithAttributes(attribute.String("messaging.destination.name", ev.RoutingKey),
				attribute.String("messaging.message.id", ev.MessageID)))
		f := inflight{ev: ev, span: span}
		f.conf, f.err = r.pub.PublishAsync(pctx, broker.ExchangePix, ev.RoutingKey, broker.Message{
			ID: ev.MessageID, Body: ev.Body, EnqueueAt: ev.CreatedAt,
		})
		flight[i] = f
	}

	var confirmed bson.A
	var failures []error
	for i := range flight {
		f := &flight[i]
		if f.err == nil {
			f.err = f.conf.Wait(ctx)
		}
		result := "ok"
		if f.err != nil {
			result = "error"
			f.span.RecordError(f.err)
			f.span.SetStatus(codes.Error, f.err.Error())
			failures = append(failures, f.err)
		} else {
			confirmed = append(confirmed, f.ev.ID)
		}
		f.span.End()
		r.published.Add(ctx, 1, metric.WithAttributes(
			attribute.String("routing_key", f.ev.RoutingKey), attribute.String("result", result)))
	}

	if len(confirmed) > 0 {
		now := store.Now()
		_, err := r.events.UpdateMany(ctx,
			bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: confirmed}}}, {Key: "publishedAt", Value: nil}},
			bson.D{{Key: "$set", Value: bson.D{{Key: "publishedAt", Value: now}}}})
		if err != nil {
			// Publicou mas não marcou: a varredura vai publicar de novo.
			return fmt.Errorf("marcar eventos como publicados: %w", err)
		}
		for _, f := range flight {
			if f.err == nil {
				r.lag.Record(ctx, now.Sub(f.ev.CreatedAt).Seconds())
			}
		}
	}
	return errors.Join(failures...)
}

// stillPending devolve só os eventos que ainda não foram marcados como
// publicados, na ordem em que vieram.
func (r *Relay) stillPending(ctx context.Context, events []Event) ([]Event, error) {
	ids := make(bson.A, len(events))
	for i, ev := range events {
		ids[i] = ev.ID
	}
	cur, err := r.events.Find(ctx,
		bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}, {Key: "publishedAt", Value: nil}},
		options.Find().SetProjection(bson.D{{Key: "_id", Value: 1}}))
	if err != nil {
		return nil, err
	}
	var pending []struct {
		ID bson.ObjectID `bson:"_id"`
	}
	if err := cur.All(ctx, &pending); err != nil {
		return nil, err
	}
	set := make(map[bson.ObjectID]bool, len(pending))
	for _, p := range pending {
		set[p.ID] = true
	}
	out := make([]Event, 0, len(pending))
	for _, ev := range events {
		if set[ev.ID] {
			out = append(out, ev)
		}
	}
	return out, nil
}
