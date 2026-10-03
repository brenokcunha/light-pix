#!/usr/bin/env bash
# Conta PI sem saldo → rejeições AM04 e alerta de liquidez.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

TITLE="Cenário 5 — conta PI do PSP A sem saldo (AM04)"
DESCRIPTION="Uma retirada STR deixa a conta PI do PSP A com R\$ 20,00. Com Pix só do PSP A para o B, a reserva
  acaba em segundos e o SPI rejeita as liquidações com AM04: não é saldo do cliente (esse a API recusaria
  na hora), é o PSP sem liquidez junto ao regulador. Disparam o alerta de liquidez e o SLO de sucesso.
  Ao fim, o aporte STR restaura a reserva.
  Observe: Grafana → Pix — serviços → Conta PI (saldo em R\$); Pix — visão IGA → Pix finalizados por motivo.
  Runbooks: docs/runbooks/pi-liquidity.md, docs/runbooks/slo-success-rate.md"
EXPECT="PILiquidityLow,PixRejectedForLiquidity,PixSuccessRate:page"
RATE=10
DURATION=3m
DIRECTION=a_to_b

BACEN=http://localhost:8093
ISPB=11111111
STAMP="$(date +%s)"
WITHDRAWN=""

pi_balance() {
  curl -s "$BACEN/api/v1/pi-accounts/$ISPB" | python -c 'import sys,json; print(json.load(sys.stdin)["balance"])'
}

inject() {
  local balance
  balance="$(pi_balance)"
  WITHDRAWN="$(python -c "print(round($balance - 20.00, 2))")"
  step "conta PI do PSP A: R\$ $balance → retirada de R\$ $WITHDRAWN (sobram R\$ 20,00)"
  curl -fsS -X POST "$BACEN/api/v1/pi-accounts/$ISPB/str-transfers" -H 'Content-Type: application/json' \
    -d "{\"transferId\":\"cenario5-retirada-$STAMP\",\"direction\":\"OUT\",\"amount\":$WITHDRAWN}" >/dev/null
}

restore() {
  # Idempotente pelo transferId: repetir o aporte não soma duas vezes.
  [ -n "$WITHDRAWN" ] && curl -fsS -X POST "$BACEN/api/v1/pi-accounts/$ISPB/str-transfers" -H 'Content-Type: application/json' \
    -d "{\"transferId\":\"cenario5-aporte-$STAMP\",\"direction\":\"IN\",\"amount\":$WITHDRAWN}" >/dev/null 2>&1
}

run_scenario
