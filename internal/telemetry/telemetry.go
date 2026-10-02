// Package telemetry configura traces, métricas e logs com OpenTelemetry.
//
// A configuração segue as variáveis de ambiente padrão do OTel
// (OTEL_SERVICE_NAME, OTEL_RESOURCE_ATTRIBUTES, OTEL_EXPORTER_OTLP_ENDPOINT,
// OTEL_METRIC_EXPORT_INTERVAL, OTEL_SDK_DISABLED...), então o mesmo binário
// roda no compose ou no Kubernetes sem código específico de ambiente.
package telemetry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
)

// Namespace agrupa todos os serviços do laboratório.
const Namespace = "light-pix"

// LatencyBuckets (em segundos) incluem os limites que importam para o Pix:
// 0,9s e 1,5s (iniciação), 6s e 10s (P50 e P99 da experiência do pagador)
// e 40s (prazo de liquidação no SPI). Com esses limites como bordas, o SLI
// "percentual abaixo de X" é uma divisão exata entre buckets.
var LatencyBuckets = []float64{
	0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 0.9, 1, 1.5, 2.5, 5, 6, 10, 15, 30, 40, 60,
}

// Config identifica o serviço. Campos vazios caem nas variáveis OTEL_*.
type Config struct {
	ServiceName string
	Version     string
}

// Shutdown descarrega e encerra os providers.
type Shutdown func(context.Context) error

// Setup instala os providers globais de trace e métrica, os propagadores W3C
// e o logger padrão (slog JSON correlacionado com o trace).
func Setup(ctx context.Context, cfg Config) (Shutdown, error) {
	if cfg.ServiceName == "" {
		cfg.ServiceName = os.Getenv("OTEL_SERVICE_NAME")
	}
	slog.SetDefault(NewLogger(cfg.ServiceName, os.Stdout))

	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))

	if strings.EqualFold(os.Getenv("OTEL_SDK_DISABLED"), "true") {
		return func(context.Context) error { return nil }, nil
	}

	res, err := resource.New(ctx,
		resource.WithFromEnv(),
		resource.WithTelemetrySDK(),
		resource.WithHost(),
		resource.WithAttributes(
			semconv.ServiceName(cfg.ServiceName),
			semconv.ServiceNamespace(Namespace),
			semconv.ServiceVersion(cfg.Version),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("telemetry: resource: %w", err)
	}

	traceExp, err := otlptracegrpc.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("telemetry: trace exporter: %w", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(traceExp),
		// A amostragem real é feita no Collector (tail sampling); aqui tudo é
		// gravado para que erros e lentidões nunca sejam descartados na origem.
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())),
	)
	otel.SetTracerProvider(tp)

	metricExp, err := otlpmetricgrpc.New(ctx)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("telemetry: metric exporter: %w", err), tp.Shutdown(ctx))
	}
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp)),
		sdkmetric.WithView(sdkmetric.NewView(
			sdkmetric.Instrument{Kind: sdkmetric.InstrumentKindHistogram, Unit: "s"},
			sdkmetric.Stream{Aggregation: sdkmetric.AggregationExplicitBucketHistogram{Boundaries: LatencyBuckets}},
		)),
	)
	otel.SetMeterProvider(mp)

	// Memória, GC e goroutines: é o que denuncia um leak antes do OOMKilled.
	if err := runtime.Start(runtime.WithMinimumReadMemStatsInterval(10 * time.Second)); err != nil {
		return nil, errors.Join(fmt.Errorf("telemetry: runtime metrics: %w", err), mp.Shutdown(ctx), tp.Shutdown(ctx))
	}

	return func(ctx context.Context) error {
		return errors.Join(tp.Shutdown(ctx), mp.Shutdown(ctx))
	}, nil
}
