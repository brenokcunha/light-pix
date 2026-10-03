# SLO — taxa de sucesso

**Alertas:** PixSuccessRate (SLO: 99% dos Pix sem rejeição técnica)  
**Cenário de reprodução:** 2, 3, 5 (`scenarios/`)

## Sintoma e impacto

Pix estão sendo rejeitados por falha técnica: `AB03` (o recebedor não respondeu no prazo), `ED05` (falha de liquidação) ou `AM04` (falta de saldo na conta PI do PSP). Rejeições de negócio do recebedor (`AC03`, `AC06`) não contam.

## Diagnóstico

1. Painel **Pix — visão IGA** → *Pix finalizados por estado e motivo*: qual motivo cresceu?
2. `AB03`: o recebedor não respondeu em 10s. O worker/conector do PSP recebedor está de pé? `docker compose ps`; fila `pix.incoming` do PSP recebedor (`QueueBacklog`). Em laboratório pode ser `creditorTimeoutRate` no SPI.
3. `AM04`: ver `pi-liquidity.md`.
4. `ED05`: o SPI não conseguiu liquidar no bacen-sim ou a mensagem foi estacionada; ver `dead-letter-queue.md`.

## Mitigação

- Tratar o motivo dominante conforme acima. Pix rejeitados já devolveram a reserva ao pagador; nada a corrigir no saldo.

## Depois

- Comparar com o SLO de liquidação (visão do regulador) para saber se o problema está no PSP ou no SPI.
