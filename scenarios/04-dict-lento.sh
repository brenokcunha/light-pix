#!/usr/bin/env bash
# DICT lento → o circuit breaker abre e a API falha rápido.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

TITLE="Cenário 4 — DICT lento (circuit breaker)"
DESCRIPTION="O dict-sim passa a responder em 2s; o gateway tem timeout de 800ms por tentativa. Depois de 5
  falhas seguidas o circuit breaker abre e os Pix por chave passam a falhar na hora com 503 DICT_UNAVAILABLE,
  em vez de segurar a API esperando. Isso queima o SLO de disponibilidade da API. Ao fim, o breaker fecha sozinho.
  Observe: Grafana → Pix — serviços → Circuit breaker do DICT, DICT: consultas, Erros 5xx.
  Runbooks: docs/runbooks/dict-breaker.md, docs/runbooks/slo-availability.md"
EXPECT="DictCircuitBreakerOpen,PixApiAvailability:page"
RATE=10
DURATION=3m

inject() {
  step "dict-sim: latência fixa de 2000ms"
  ./scripts/faults.sh dict '{"latency":{"mode":"fixed","ms":2000}}' >/dev/null
}

restore() {
  ./scripts/faults.sh dict clear >/dev/null 2>&1
}

# Com o DICT de volta o breaker fecha sozinho (15s aberto + chamadas de teste):
# um Pix novo precisa passar.
after_drain() {
  step "o breaker fecha sozinho: um Pix por chave volta a passar"
  local i status
  for i in $(seq 1 12); do
    status=$(curl -s -o /dev/null -w '%{http_code}' -X POST http://localhost:8081/v1/pix/payments \
      -H "Idempotency-Key: cenario4-$(date +%s)-$i" -H 'Content-Type: application/json' \
      -d '{"debtorAccount":{"branch":"0001","number":"1000001"},"key":"bob@example.com","amount":0.01}')
    if [ "$status" = "201" ]; then echo "  ok: Pix aceito (201) depois de $((i - 1)) tentativas"; sleep 3; return 0; fi
    sleep 5
  done
  echo "falha: a API seguiu recusando (último status $status)"
  return 1
}

run_scenario
