#!/usr/bin/env bash
# pacs.002 perdidas → Pix parados até o resolver consultar o SPI.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

TITLE="Cenário 3 — pacs.002 perdidas"
DESCRIPTION="O SPI perde 30% das pacs.002: a transação liquida, mas o PSP nunca é avisado. Os Pix ficam SENT
  (pagador) ou ACCEPTED (recebedor) até o pending-resolver perguntar o status ao SPI, 40s depois. O dinheiro
  segue reservado nesse tempo e o SLO de experiência do pagador estoura. No fim, tudo se resolve sozinho.
  Observe: Grafana → Pix — serviços → Pendências consultadas no SPI, Pix parados; Pix — visão IGA.
  Runbooks: docs/runbooks/stuck-payments.md, docs/runbooks/slo-payer-experience.md"
EXPECT="PaymentsStuck,PixPayerExperience:page"
RATE=10
DURATION=4m

inject() {
  step "spi-sim: dropPacs002 0,3"
  ./scripts/faults.sh spi '{"dropPacs002":0.3}' >/dev/null
}

restore() {
  ./scripts/faults.sh spi clear >/dev/null 2>&1
}

run_scenario
