# SLO — prazo de liquidação (regulador)

**Alertas:** PixSettlementDeadline (SLO: 99,9% das liquidações em até 40s)  
**Cenário de reprodução:** 5 (`scenarios/`)

## Sintoma e impacto

A liquidação, medida pelo SPI da chegada da `pacs.008` até a liquidação, passa de 40s. Visão do regulador (IGA): o atraso é do SPI/bacen-sim, não do PSP.

## Diagnóstico

1. *Pix — visão IGA* → *Liquidação no SPI e resposta do recebedor*. Se a resposta do recebedor está alta, o atraso é do PSP recebedor; se a liquidação está alta com resposta normal, é o SPI ou o bacen-sim.
2. bacen-sim lento ou com erros: `curl localhost:8093/admin/faults`; *Bacen: liquidações por resultado*.
3. O SPI repete liquidações paradas a cada `SPI_SETTLE_RETRY` (5s) enquanto o bacen-sim falha com erro temporário.

## Mitigação

- Limpar falhas injetadas no bacen-sim (`make faults s=bacen j=clear`) ou restabelecer o serviço.
- Liquidações em andamento terminam sozinhas quando o bacen-sim volta.

## Depois

- Conferir a reconciliação da conta PI depois do incidente (`reconciliation-divergence.md`).
