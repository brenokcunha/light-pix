#!/usr/bin/env bash
# SPI intermitente (30% de 5xx) → retries, DLQ, reprocessamento.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

TITLE="Cenário 2 — SPI intermitente (30% de erros 5xx)"
DESCRIPTION="O spi-sim responde 503 a 30% das requisições. O conector reenvia pelas filas de retry (5s, 30s,
  2min); quem esgota as tentativas cai na DLQ e o resolver devolve à fila. Os Pix demoram, parte passa dos
  10s e o SLO de experiência do pagador queima orçamento. Nada se perde.
  Observe: Grafana → Pix — serviços → Consumo por resultado (retry), Filas de espera, DLQ; Pix — visão IGA.
  Runbooks: docs/runbooks/dead-letter-queue.md, docs/runbooks/slo-payer-experience.md"
EXPECT="MessageRetryRateHigh,MessagesDeadLettered,PixPayerExperience:page"
RATE=10
DURATION=4m

inject() {
  step "spi-sim: errorRate 0,3"
  ./scripts/faults.sh spi '{"errorRate":0.3}' >/dev/null
}

restore() {
  ./scripts/faults.sh spi clear >/dev/null 2>&1
}

run_scenario
