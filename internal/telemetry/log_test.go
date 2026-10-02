package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"go.opentelemetry.io/otel/trace"
)

func TestMask(t *testing.T) {
	cases := map[string]string{
		"":               "",
		"abc":            "***",
		"12345678901":    "*********01",
		"ana@pix.com.br": "************br",
	}
	for in, want := range cases {
		if got := Mask(in); got != want {
			t.Errorf("Mask(%q) = %q, quer %q", in, got, want)
		}
	}
}

func TestLoggerMasksAndCorrelates(t *testing.T) {
	var buf bytes.Buffer
	log := NewLogger("teste", &buf)

	traceID, _ := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	spanID, _ := trace.SpanIDFromHex("00f067aa0ba902b7")
	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: traceID, SpanID: spanID, TraceFlags: trace.FlagsSampled,
	}))

	log.InfoContext(ctx, "pix enviado", "cpf", "12345678901", "amount", 10)

	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("log não é JSON: %v", err)
	}
	want := map[string]any{
		"service":  "teste",
		"cpf":      "*********01",
		"trace_id": traceID.String(),
		"span_id":  spanID.String(),
	}
	for k, v := range want {
		if rec[k] != v {
			t.Errorf("%s = %v, quer %v", k, rec[k], v)
		}
	}
}
