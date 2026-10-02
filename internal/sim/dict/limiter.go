package dict

import (
	"sync"
	"time"
)

// limiter é um token bucket por chave (participante + usuário pagador),
// em memória: reiniciar o simulador zera os baldes.
type limiter struct {
	mu        sync.Mutex
	capacity  float64
	perSecond float64
	buckets   map[string]*bucket
}

type bucket struct {
	tokens float64
	last   time.Time
}

func newLimiter(capacity, perSecond float64) *limiter {
	return &limiter{capacity: capacity, perSecond: perSecond, buckets: map[string]*bucket{}}
}

// allow diz se há pelo menos uma ficha e, se não houver, quanto esperar.
func (l *limiter) allow(key string, now time.Time) (bool, time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	b := l.refill(key, now)
	if b.tokens >= 1 {
		return true, 0
	}
	if l.perSecond <= 0 {
		return false, time.Hour
	}
	return false, time.Duration((1 - b.tokens) / l.perSecond * float64(time.Second))
}

// charge desconta o custo da consulta. O saldo pode ficar negativo: quem
// varre chaves inexistentes paga a diferença em tempo de espera.
func (l *limiter) charge(key string, cost float64, now time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.refill(key, now).tokens -= cost
}

func (l *limiter) refill(key string, now time.Time) *bucket {
	b, ok := l.buckets[key]
	if !ok {
		if len(l.buckets) >= 100_000 {
			l.evictFull(now)
		}
		b = &bucket{tokens: l.capacity, last: now}
		l.buckets[key] = b
		return b
	}
	b.tokens = min(l.capacity, b.tokens+now.Sub(b.last).Seconds()*l.perSecond)
	b.last = now
	return b
}

// evictFull descarta baldes que já estariam cheios: recriá-los dá no mesmo.
func (l *limiter) evictFull(now time.Time) {
	for k, b := range l.buckets {
		if b.tokens+now.Sub(b.last).Seconds()*l.perSecond >= l.capacity {
			delete(l.buckets, k)
		}
	}
}
