# Pix parados em estado não terminal

**Alertas:** PaymentsStuck, PaymentsStuckMany  
**Cenário de reprodução:** 3 (`scenarios/`)

## Sintoma e impacto

Há Pix `PENDING`/`SENT` (enviados) ou `ACCEPTED` (recebidos) há mais que o limiar do resolver (40s). O dinheiro do pagador segue reservado.

## Diagnóstico

1. O resolver está rodando? `docker compose logs psp-a-resolver | tail`; métrica `resolver_checked_total` por desfecho.
2. Desfecho `in_flight`: o SPI ainda trabalha (aguardando o recebedor ou liquidando). Desfecho `not_found`: o SPI não conhece o Pix, a `pacs.008` ainda está na fila de retry ou na DLQ (ver `dead-letter-queue.md`) — o resolver não rejeita nesse caso, de propósito. Desfecho `error`: o SPI não respondeu ao status.
3. `pacs.002` perdidas: o SPI liquidou e ninguém foi avisado; o resolver fecha sozinho no próximo ciclo (`confirmed`/`credited`). Confirmar com `make faults s=spi` se `dropPacs002` está ligado.

## Mitigação

- Em geral, esperar: o resolver roda a cada 10s.
- Resolver parado: `docker compose up -d psp-a-resolver`.
- `PaymentsStuckMany`: ver o motivo dominante em `resolver_checked_total`; se for `error`, o SPI está fora.

## Depois

- Garantir que `pix_payments_stuck` voltou a zero e rodar a reconciliação do dia.
