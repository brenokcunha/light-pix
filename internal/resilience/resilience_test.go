package resilience_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sony/gobreaker/v2"

	"github.com/brenokcunha/light-pix/internal/resilience"
)

var errTemp = errors.New("temporário")

func TestRetry(t *testing.T) {
	always := func(error) bool { return true }
	calls := 0
	v, err := resilience.Retry(context.Background(), 3, time.Millisecond, 2*time.Millisecond, always,
		func(context.Context) (int, error) {
			calls++
			if calls < 3 {
				return 0, errTemp
			}
			return 42, nil
		})
	if err != nil || v != 42 || calls != 3 {
		t.Fatalf("v=%d err=%v calls=%d", v, err, calls)
	}

	calls = 0
	_, err = resilience.Retry(context.Background(), 3, time.Millisecond, time.Millisecond, always,
		func(context.Context) (int, error) { calls++; return 0, errTemp })
	if !errors.Is(err, errTemp) || calls != 3 {
		t.Fatalf("esgotadas: err=%v calls=%d", err, calls)
	}

	calls = 0
	_, err = resilience.Retry(context.Background(), 3, time.Millisecond, time.Millisecond, func(error) bool { return false },
		func(context.Context) (int, error) { calls++; return 0, errTemp })
	if err == nil || calls != 1 {
		t.Fatalf("erro não repetível: err=%v calls=%d", err, calls)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls = 0
	_, _ = resilience.Retry(ctx, 5, time.Hour, time.Hour, always,
		func(context.Context) (int, error) { calls++; return 0, errTemp })
	if calls != 1 {
		t.Fatalf("contexto cancelado deve interromper o retry: calls=%d", calls)
	}
}

func TestBreakerOpensAfterConsecutiveFailures(t *testing.T) {
	cb := resilience.NewBreaker[int]("teste", func(err error) bool { return err == nil })
	fail := func() (int, error) { return 0, errTemp }
	for range 5 {
		_, _ = cb.Execute(fail)
	}
	if _, err := cb.Execute(func() (int, error) { t.Fatal("não deveria executar"); return 0, nil }); !errors.Is(err, gobreaker.ErrOpenState) {
		t.Fatalf("err = %v, quer ErrOpenState", err)
	}
}
