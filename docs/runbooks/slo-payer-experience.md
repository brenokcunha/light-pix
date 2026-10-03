# SLO — experiência do pagador

**Alertas:** PixPayerExperience (SLO: 99% dos Pix em até 10s)  
**Cenário de reprodução:** 2, 3 (`scenarios/`)

## Sintoma e impacto

Mais de 1% dos Pix leva mais de 10s para chegar ao estado final (confirmado ou rejeitado). O pagador espera sem resposta.

## Diagnóstico

1. Painel **Pix — visão IGA** → *Experiência do pagador*: o P99 passou de 10s? Compare PSP × regulador: se o regulador também vê, o atraso é real, não de medição.
2. Onde o tempo se perde: abra um Pix lento em *Pix lentos (> 5s)* (Tempo). Procure o maior intervalo entre spans: `outbox.publish` (relay), `process spi.out` (conector), `POST /api/v1/in` (SPI), `process pix.results` (worker).
3. Filas: `QueueBacklog`, `OutboxBacklog` ou `MessageRetryRateHigh` disparando indicam o estágio.
4. SPI instável: *Pix — serviços* → *SPI: mensagens recebidas* e `curl localhost:8091/admin/faults` (alguma falha injetada ativa?).
5. Pix parados: `PaymentsStuck`. Uma `pacs.002` perdida só fecha pelo resolver, depois de 40s — isso sozinho estoura os 10s.

## Mitigação

- Retries: o SPI está falhando; tratar a causa (ver `dead-letter-queue.md`). Os Pix voltam sozinhos nas tentativas de 5s, 30s e 2min.
- `pacs.002` perdidas: confirmar que o resolver roda (`docker compose ps psp-a-resolver`) e que `RESOLVER_MIN_AGE` não está alto demais.
- Atraso no relay ou no conector: ver `outbox-backlog.md` e `queue-backlog.md`.

## Depois

- Anotar a causa e se o limiar de 40s do resolver (`RESOLVER_MIN_AGE`) é adequado para a perda de `pacs.002`.
- Conferir `slo:period_error_budget_remaining:ratio` do SLO: quanto do orçamento de 30 dias o incidente consumiu.
