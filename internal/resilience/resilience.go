// Package resilience tem retry com backoff exponencial e jitter, e a
// configuração padrão dos circuit breakers do laboratório.
package resilience

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/sony/gobreaker/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Retry chama fn até attempts vezes. Entre as tentativas espera um tempo
// aleatório em [0, min(max, base·2^n)) (full jitter), para que clientes que
// falharam juntos não voltem juntos. Só repete se retryable(err) for true e
// o contexto ainda estiver vivo; devolve o último erro.
func Retry[T any](ctx context.Context, attempts int, base, max time.Duration,
	retryable func(error) bool, fn func(context.Context) (T, error)) (T, error) {
	var (
		v   T
		err error
	)
	for n := range attempts {
		if v, err = fn(ctx); err == nil || !retryable(err) || n == attempts-1 {
			return v, err
		}
		wait := time.Duration(rand.Int64N(int64(min(max, base<<n)) + 1))
		select {
		case <-ctx.Done():
			return v, err
		case <-time.After(wait):
		}
	}
	return v, err
}

// NewBreaker cria um circuit breaker que abre após 5 falhas seguidas e fica
// aberto por 15s (depois deixa passar 3 chamadas de teste). isSuccessful diz
// quais erros contam como falha do serviço: respostas 4xx, por exemplo, não
// são. Estado e transições viram métricas.
func NewBreaker[T any](name string, isSuccessful func(error) bool) *gobreaker.CircuitBreaker[T] {
	meter := otel.Meter("github.com/brenokcunha/light-pix/internal/resilience")
	transitions, _ := meter.Int64Counter("resilience.breaker.transitions",
		metric.WithDescription("Mudanças de estado do circuit breaker"))

	cb := gobreaker.NewCircuitBreaker[T](gobreaker.Settings{
		Name:         name,
		MaxRequests:  3,
		Interval:     30 * time.Second,
		Timeout:      15 * time.Second,
		IsSuccessful: isSuccessful,
		ReadyToTrip:  func(c gobreaker.Counts) bool { return c.ConsecutiveFailures >= 5 },
		OnStateChange: func(name string, from, to gobreaker.State) {
			slog.Warn("circuit breaker mudou de estado", "breaker", name, "from", from.String(), "to", to.String())
			transitions.Add(context.Background(), 1, metric.WithAttributes(
				attribute.String("breaker", name), attribute.String("to", to.String())))
		},
	})
	_, _ = meter.Int64ObservableGauge("resilience.breaker.state",
		metric.WithDescription("Estado do circuit breaker: 0 fechado, 1 meio-aberto, 2 aberto"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			state := map[gobreaker.State]int64{gobreaker.StateClosed: 0, gobreaker.StateHalfOpen: 1, gobreaker.StateOpen: 2}[cb.State()]
			o.Observe(state, metric.WithAttributes(attribute.String("breaker", name)))
			return nil
		}))
	return cb
}
