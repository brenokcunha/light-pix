package telemetry

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"

	"go.opentelemetry.io/otel/trace"
)

// sensitiveKeys são atributos de log cujo valor é dado pessoal. O
// mascaramento acontece na origem; o Alloy reforça por regex no pipeline.
var sensitiveKeys = map[string]bool{
	"cpf": true, "cnpj": true, "document": true, "tax_id": true,
	"pix_key": true, "email": true, "phone": true, "owner_name": true,
}

// NewLogger cria um logger JSON que inclui trace_id e span_id do contexto e
// mascara atributos sensíveis. O nível vem de LOG_LEVEL (padrão: info).
func NewLogger(service string, w io.Writer) *slog.Logger {
	var level slog.Level
	if err := level.UnmarshalText([]byte(os.Getenv("LOG_LEVEL"))); err != nil {
		level = slog.LevelInfo
	}
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{
		Level:       level,
		ReplaceAttr: maskAttr,
	})
	return slog.New(traceHandler{h}).With("service", service)
}

func maskAttr(_ []string, a slog.Attr) slog.Attr {
	if sensitiveKeys[strings.ToLower(a.Key)] && a.Value.Kind() == slog.KindString {
		return slog.String(a.Key, Mask(a.Value.String()))
	}
	return a
}

// Mask preserva só os 2 últimos caracteres, o suficiente para conferência
// humana sem expor o dado.
func Mask(s string) string {
	r := []rune(s)
	if len(r) <= 4 {
		return strings.Repeat("*", len(r))
	}
	return strings.Repeat("*", len(r)-2) + string(r[len(r)-2:])
}

// traceHandler injeta a correlação com o trace em cada registro.
type traceHandler struct{ slog.Handler }

func (h traceHandler) Handle(ctx context.Context, r slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		r.AddAttrs(
			slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()),
		)
	}
	return h.Handler.Handle(ctx, r)
}

func (h traceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return traceHandler{h.Handler.WithAttrs(attrs)}
}

func (h traceHandler) WithGroup(name string) slog.Handler {
	return traceHandler{h.Handler.WithGroup(name)}
}
