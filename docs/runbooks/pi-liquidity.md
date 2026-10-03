# Liquidez da conta PI

**Alertas:** PILiquidityLow, PixRejectedForLiquidity  
**Cenário de reprodução:** 5 (`scenarios/`)

## Sintoma e impacto

A conta PI do PSP está sem reserva para liquidar: os Pix saem rejeitados com `AM04` pelo SPI. Não é saldo do cliente (esse é recusado na API com `INSUFFICIENT_FUNDS`): é o PSP sem liquidez junto ao regulador.

## Diagnóstico

1. Saldo: `curl localhost:8093/api/v1/pi-accounts/11111111` (e `22222222`); *Pix — serviços* → *Conta PI (saldo em R$)*.
2. Quem esvaziou: o extrato `GET /api/v1/pi-accounts/{ispb}/statement` mostra as retiradas STR (`STR_OUT`) e as liquidações.
3. Rejeições `AM04`: *Pix finalizados por estado e motivo*.

## Mitigação

- Aportar reserva (STR → PI): `curl -X POST localhost:8093/api/v1/pi-accounts/11111111/str-transfers -H 'Content-Type: application/json' -d '{"transferId":"aporte-1","direction":"IN","amount":500000.00}'`. O `transferId` é idempotente.
- Os Pix rejeitados não voltam sozinhos: o cliente precisa refazê-los (a reserva do cliente já foi devolvida).

## Depois

- Definir um piso de liquidez e automatizar o aporte; o alerta dispara abaixo de R$ 10.000.
