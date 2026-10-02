package store

import (
	"context"
	"sync"

	"go.mongodb.org/mongo-driver/v2/event"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
)

// Comandos de controle que só poluiriam os traces.
var skipCommands = map[string]bool{
	"hello": true, "isMaster": true, "ping": true, "saslStart": true,
	"saslContinue": true, "endSessions": true, "buildInfo": true,
}

// commandTracer cria um span por comando do MongoDB. Substitui o otelmongo,
// que ainda não tem versão estável para o driver v2. Só gera span quando já
// existe um trace no contexto: laços de fundo (change streams, sweepers) sem
// trace pai não viram traces soltos.
type commandTracer struct {
	tracer trace.Tracer
	spans  sync.Map // RequestID → trace.Span
}

func newCommandMonitor() *event.CommandMonitor {
	t := &commandTracer{tracer: otel.Tracer("github.com/brenokcunha/light-pix/internal/store")}
	return &event.CommandMonitor{Started: t.started, Succeeded: t.succeeded, Failed: t.failed}
}

func (t *commandTracer) started(ctx context.Context, e *event.CommandStartedEvent) {
	if skipCommands[e.CommandName] || !trace.SpanContextFromContext(ctx).IsValid() {
		return
	}
	attrs := []attribute.KeyValue{
		semconv.DBSystemNameMongoDB,
		semconv.DBNamespace(e.DatabaseName),
		semconv.DBOperationName(e.CommandName),
	}
	name := e.CommandName
	// O primeiro elemento do comando traz a coleção (ex.: {find: "payments"}).
	if v, err := e.Command.IndexErr(0); err == nil {
		if coll, ok := v.Value().StringValueOK(); ok {
			attrs = append(attrs, semconv.DBCollectionName(coll))
			name += " " + coll
		}
	}
	_, span := t.tracer.Start(ctx, name, trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(attrs...))
	t.spans.Store(e.RequestID, span)
}

func (t *commandTracer) succeeded(_ context.Context, e *event.CommandSucceededEvent) {
	if s, ok := t.spans.LoadAndDelete(e.RequestID); ok {
		s.(trace.Span).End()
	}
}

func (t *commandTracer) failed(_ context.Context, e *event.CommandFailedEvent) {
	if s, ok := t.spans.LoadAndDelete(e.RequestID); ok {
		span := s.(trace.Span)
		span.RecordError(e.Failure)
		span.SetStatus(codes.Error, e.Failure.Error())
		span.End()
	}
}
